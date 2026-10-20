from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass
import time

import psycopg
import pytest

from okg.substrate.runtime.dbos_enqueue import (
    DBOSWorkflowEnqueueResult,
    TerminalDeduplicationCollision,
    active_workflow_id_for_name,
    enqueue_deployment_workflow,
    latest_registered_app_version,
)
from okg.substrate.source_sync import (
    SourceSyncCompaction,
    SourceSyncEvent,
    SourceSyncJob,
)
from okg.substrate.source_sync_enqueue import (
    enqueue_compacted_source_sync_via_dbos,
    enqueue_source_sync_via_dbos,
)


@pytest.fixture(autouse=True)
def _permit_fake_dbos_enqueues(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def permit(**_kwargs):
        yield

    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.promotion_enqueue_permit",
        permit,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.promotion_enqueue_permit",
        permit,
    )


@pytest.fixture(autouse=True)
def _default_no_running_publisher_for_source_sync_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def noop_source_sync_admission_intent_lock(**_kwargs):
        yield

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


@dataclass
class _Handle:
    workflow_id: str


def test_enqueue_deployment_workflow_passes_dedupe_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, options, *args):
            captured["options"] = options
            captured["args"] = args
            return _Handle("wf-new")

    import dbos

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: "app-v1",
    )

    result = enqueue_deployment_workflow(
        deployment="test",
        dsn="postgres://unused",
        workflow_name="okg_source_poll_git-files",
        args=("payload",),
        deduplication_id="source-poll:test:git-files",
    )

    assert result == DBOSWorkflowEnqueueResult(workflow_id="wf-new")
    assert captured["options"] == {
        "queue_name": "okg-test",
        "workflow_name": "okg_source_poll_git-files",
        "deduplication_id": "source-poll:test:git-files",
        "app_version": "app-v1",
    }
    assert captured["args"] == ("payload",)


def test_enqueue_deployment_workflow_omits_missing_app_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, options, *_args):
            captured["options"] = options
            return _Handle("wf-new")

    import dbos

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: None,
    )

    result = enqueue_deployment_workflow(
        deployment="test",
        dsn="postgres://unused",
        workflow_name="okg_source_poll_git-files",
    )

    assert result == DBOSWorkflowEnqueueResult(workflow_id="wf-new")
    assert captured["options"] == {
        "queue_name": "okg-test",
        "workflow_name": "okg_source_poll_git-files",
    }


def test_enqueue_deployment_workflow_passes_priority_option(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, options, *_args):
            captured["options"] = options
            return _Handle("wf-priority")

    import dbos

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: None,
    )

    result = enqueue_deployment_workflow(
        deployment="test",
        dsn="postgres://unused",
        workflow_name="okg_source_sync",
        queue_name="okg-test-source-sync",
        priority=100,
    )

    assert result == DBOSWorkflowEnqueueResult(workflow_id="wf-priority")
    assert captured["options"] == {
        "queue_name": "okg-test-source-sync",
        "workflow_name": "okg_source_sync",
        "priority": 100,
    }


@pytest.mark.postgres
def test_latest_registered_app_version_reads_newest(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.application_versions (
              version_id uuid PRIMARY KEY,
              version_name text NOT NULL,
              version_timestamp bigint,
              created_at bigint
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.application_versions
              (version_id, version_name, version_timestamp, created_at)
            VALUES
              ('00000000-0000-0000-0000-0000000000a1', 'app-old', 100, 100),
              ('00000000-0000-0000-0000-0000000000a2', 'app-new', 200, 200)
            ON CONFLICT DO NOTHING
            """
        )

    try:
        assert latest_registered_app_version(clean_runtime) == "app-new"
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


@pytest.mark.postgres
def test_active_workflow_id_for_name_reads_oldest_active(
    clean_runtime: str,
    ) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text,
              name text,
              queue_name text,
              created_at bigint
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status
              (workflow_uuid, status, name, queue_name, created_at)
            VALUES
              ('wf-success', 'SUCCESS', 'okg_source_poll_docs',
               'okg-demo', 10),
              ('wf-newer', 'ENQUEUED', 'okg_source_poll_docs',
               'okg-demo', 30),
              ('wf-older', 'PENDING', 'okg_source_poll_docs',
               'okg-demo', 20),
              ('wf-other', 'PENDING', 'okg_source_poll_other',
               'okg-demo', 5)
            """
        )

    assert active_workflow_id_for_name(
        dsn=clean_runtime,
        queue_name="okg-demo",
        workflow_name="okg_source_poll_docs",
    ) == "wf-older"


def test_enqueue_deployment_workflow_returns_existing_on_dedupe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._error import DBOSQueueDeduplicatedError
    import dbos

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, _options, *_args):
            raise DBOSQueueDeduplicatedError(
                "wf-attempted",
                "okg-test",
                "source-poll:test:git-files",
            )

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue."
        "active_workflow_id_for_deduplication",
        lambda **_kwargs: "wf-existing",
    )

    result = enqueue_deployment_workflow(
        deployment="test",
        dsn="postgres://unused",
        workflow_name="okg_source_poll_git-files",
        deduplication_id="source-poll:test:git-files",
    )

    assert result == DBOSWorkflowEnqueueResult(
        workflow_id="wf-existing",
        deduplicated=True,
    )


def test_enqueue_deployment_workflow_rejects_terminal_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._error import DBOSQueueDeduplicatedError
    import dbos

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, _options, *_args):
            raise DBOSQueueDeduplicatedError(
                "wf-attempted",
                "okg-test",
                "publisher:test:cycle:123",
            )

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue."
        "active_workflow_id_for_deduplication",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.workflow_id_for_deduplication",
        lambda **_kwargs: "wf-terminal",
    )

    with pytest.raises(TerminalDeduplicationCollision) as exc_info:
        enqueue_deployment_workflow(
            deployment="test",
            dsn="postgres://unused",
            workflow_name="okg_publisher_cycle",
            deduplication_id="publisher:test:cycle:123",
        )

    assert exc_info.value.queue_name == "okg-test"
    assert exc_info.value.deduplication_id == "publisher:test:cycle:123"
    assert exc_info.value.terminal_workflow_id == "wf-terminal"


def test_source_sync_dbos_enqueue_passes_deduplication_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-source-sync")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id="sync:test:poll:scope_complete:docs:closure:123",
    )

    assert workflow_id == "wf-source-sync"
    assert captured["workflow_name"] == "okg_source_sync"
    assert captured["deduplication_id"] == (
        "sync:test:poll:scope_complete:docs:closure:123"
    )
    assert captured["priority"] == 100


def test_source_sync_dbos_enqueue_waits_for_running_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    active_checks: list[dict[str, object]] = []
    sleeps: list[float] = []

    def fake_active_workflow_id_for_name(**kwargs):
        active_checks.append(kwargs)
        if len(active_checks) == 1:
            return "wf-publisher-running"
        return None

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-source-sync")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.active_workflow_id_for_name",
        fake_active_workflow_id_for_name,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )
    monkeypatch.setenv(
        "OKG_SOURCE_SYNC_PUBLISHER_WAIT_INTERVAL_SECONDS",
        "0.25",
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id="sync:test:poll:scope_complete:docs:closure:123",
    )

    assert workflow_id == "wf-source-sync"
    assert sleeps == [0.25]
    assert active_checks == [
        {
            "dsn": "postgres://unused",
            "queue_name": "okg-test",
            "workflow_name": "okg_publisher_cycle",
            "statuses": ("PENDING",),
        },
        {
            "dsn": "postgres://unused",
            "queue_name": "okg-test",
            "workflow_name": "okg_publisher_cycle",
            "statuses": ("PENDING",),
        },
        {
            "dsn": "postgres://unused",
            "queue_name": "okg-test",
            "workflow_name": "okg_publisher_cycle",
            "statuses": ("PENDING",),
        },
    ]
    assert captured["queue_name"] == "okg-test-source-sync"
    assert captured["workflow_name"] == "okg_source_sync"


def test_source_sync_dbos_enqueue_waits_for_held_publisher_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    lock_checks: list[dict[str, object]] = []
    active_checks: list[dict[str, object]] = []
    sleeps: list[float] = []

    def fake_publisher_publish_lock_holder_id(**kwargs):
        lock_checks.append(kwargs)
        if len(lock_checks) == 1:
            return "publisher-advisory-lock-held"
        return None

    def fake_active_workflow_id_for_name(**kwargs):
        active_checks.append(kwargs)
        return None

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-source-sync")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.publisher_publish_lock_holder_id",
        fake_publisher_publish_lock_holder_id,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.active_workflow_id_for_name",
        fake_active_workflow_id_for_name,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )
    monkeypatch.setenv(
        "OKG_SOURCE_SYNC_PUBLISHER_WAIT_INTERVAL_SECONDS",
        "0.25",
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id="sync:test:poll:scope_complete:docs:closure:123",
    )

    assert workflow_id == "wf-source-sync"
    assert sleeps == [0.25]
    assert lock_checks == [
        {
            "dsn": "postgres://unused",
            "deployment": "test",
        },
        {
            "dsn": "postgres://unused",
            "deployment": "test",
        },
        {
            "dsn": "postgres://unused",
            "deployment": "test",
        },
    ]
    assert len(active_checks) == 2
    assert captured["queue_name"] == "okg-test-source-sync"
    assert captured["workflow_name"] == "okg_source_sync"


def test_source_sync_dbos_enqueue_holds_admission_intent_until_enqueued(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    @contextmanager
    def fake_source_sync_admission_intent_lock(**kwargs):
        assert kwargs == {
            "deployment": "test",
            "dsn": "postgres://unused",
        }
        events.append("intent-enter")
        try:
            yield
        finally:
            events.append("intent-exit")

    def fake_active_workflow_id_for_name(**_kwargs):
        events.append("publisher-probe")
        return None

    def fake_enqueue_deployment_workflow(**_kwargs):
        events.append("enqueue")
        return DBOSWorkflowEnqueueResult("wf-source-sync")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "source_sync_admission_intent_lock",
        fake_source_sync_admission_intent_lock,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.active_workflow_id_for_name",
        fake_active_workflow_id_for_name,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id="sync:test:poll:scope_complete:docs:closure:123",
    )

    assert workflow_id == "wf-source-sync"
    assert events == [
        "publisher-probe",
        "intent-enter",
        "publisher-probe",
        "enqueue",
        "intent-exit",
    ]


def test_source_sync_dbos_enqueue_waits_outside_admission_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    publisher_checks = 0

    @contextmanager
    def fake_source_sync_admission_intent_lock(**_kwargs):
        events.append("intent-enter")
        try:
            yield
        finally:
            events.append("intent-exit")

    def fake_active_workflow_id_for_name(**_kwargs):
        nonlocal publisher_checks
        publisher_checks += 1
        events.append(f"publisher-probe-{publisher_checks}")
        return "wf-publisher-running" if publisher_checks == 1 else None

    def fake_enqueue_deployment_workflow(**_kwargs):
        events.append("enqueue")
        return DBOSWorkflowEnqueueResult("wf-source-sync")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "source_sync_admission_intent_lock",
        fake_source_sync_admission_intent_lock,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.active_workflow_id_for_name",
        fake_active_workflow_id_for_name,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.time.sleep",
        lambda _seconds: events.append("sleep"),
    )
    monkeypatch.setenv(
        "OKG_SOURCE_SYNC_PUBLISHER_WAIT_INTERVAL_SECONDS",
        "0.25",
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id="sync:test:poll:scope_complete:docs:closure:123",
    )

    assert workflow_id == "wf-source-sync"
    assert events == [
        "publisher-probe-1",
        "sleep",
        "publisher-probe-2",
        "intent-enter",
        "publisher-probe-3",
        "enqueue",
        "intent-exit",
    ]


def test_source_sync_dbos_enqueue_refuses_stuck_running_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("source-sync enqueue should wait/refuse first")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.active_workflow_id_for_name",
        lambda **_kwargs: "wf-publisher-running",
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fail_if_enqueue_called,
    )
    monkeypatch.setenv(
        "OKG_SOURCE_SYNC_PUBLISHER_WAIT_TIMEOUT_SECONDS",
        "0",
    )

    with pytest.raises(RuntimeError, match="blocked by running publisher"):
        enqueue_source_sync_via_dbos(
            deployment="test",
            dsn="postgres://unused",
            job=SourceSyncJob(
                deployment="test",
                sources=("docs",),
                run_mode="scope_complete",
            ),
            deduplication_id=(
                "sync:test:poll:scope_complete:docs:closure:123"
            ),
        )


def test_source_sync_dbos_enqueue_profile_uses_profile_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-source-sync-profile")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
            profile_force_deep=True,
            profile_collectors=("rootless-speedscope",),
        ),
        deduplication_id="sync:test:poll:scope_complete:docs:closure:123",
    )

    assert workflow_id == "wf-source-sync-profile"
    assert captured["workflow_name"] == "okg_source_sync"
    assert captured["priority"] == 1


def test_source_sync_dbos_enqueue_operator_scope_uses_operator_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-source-sync-operator")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
            events=(
                SourceSyncEvent(
                    deployment="test",
                    source_name="docs",
                    trigger_kind="manual",
                    reason="operator_scope",
                ),
            ),
        ),
        deduplication_id="sync:test:operator:scope_complete:docs:none:123",
    )

    assert workflow_id == "wf-source-sync-operator"
    assert captured["workflow_name"] == "okg_source_sync"
    assert captured["priority"] == 10


def test_source_sync_dbos_enqueue_readiness_recovery_uses_recovery_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-readiness-recovery")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="okg-workspace",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="okg-workspace",
            sources=("coordination-native",),
            run_mode="reconcile",
        ),
        deduplication_id=(
            "sync:okg-workspace:poll:reconcile:"
            "coordination-native:none:123"
        ),
    )

    assert workflow_id == "wf-readiness-recovery"
    assert captured["workflow_name"] == "okg_source_sync"
    assert captured["priority"] == 20


def test_source_sync_dbos_enqueue_non_readiness_complete_stays_routine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-routine")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="okg-workspace",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="okg-workspace",
            sources=("compute-env-local",),
            run_mode="scope_complete",
        ),
        deduplication_id=(
            "sync:okg-workspace:poll:scope_complete:"
            "compute-env-local:none:123"
        ),
    )

    assert workflow_id == "wf-routine"
    assert captured["workflow_name"] == "okg_source_sync"
    assert captured["priority"] == 100


@pytest.mark.parametrize("terminal_status", ["ERROR", "CANCELLED"])
def test_source_sync_dbos_enqueue_retries_terminal_duplicate_with_successor(
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: str,
) -> None:
    from dbos._error import DBOSQueueDeduplicatedError
    import dbos

    original_dedupe = "sync:test:poll:scope_complete:docs:none:123"
    calls: list[dict[str, object]] = []

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, options, *_args):
            calls.append(dict(options))
            if options["deduplication_id"] == original_dedupe:
                raise DBOSQueueDeduplicatedError(
                    "wf-attempted",
                    options["queue_name"],
                    original_dedupe,
                )
            return _Handle(f"wf-runnable-after-{terminal_status.lower()}")

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue."
        "active_workflow_id_for_deduplication",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.workflow_id_for_deduplication",
        lambda **_kwargs: f"wf-terminal-{terminal_status.lower()}",
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id=original_dedupe,
    )

    assert workflow_id == f"wf-runnable-after-{terminal_status.lower()}"
    assert len(calls) == 2
    assert calls[0]["deduplication_id"] == original_dedupe
    successor_dedupe = calls[1]["deduplication_id"]
    assert isinstance(successor_dedupe, str)
    assert successor_dedupe != original_dedupe
    assert successor_dedupe.startswith(
        f"{original_dedupe}:terminal-successor:"
    )
    assert calls[1]["workflow_name"] == "okg_source_sync"
    assert calls[1]["queue_name"] == calls[0]["queue_name"]


@pytest.mark.postgres
def test_compacted_source_sync_dbos_merges_enqueued_inputs(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        deserialize_args,
        serialize_args,
    )

    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("expected queued DBOS row to be compacted")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )
    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    first_event = SourceSyncEvent(
        deployment="test",
        source_name="docs",
        trigger_kind="fs_event",
        scope_key="first.md",
    )
    canonical_telemetry = {
        "schema": "okg.source_sync.trace.v1",
        "trace_id": "1" * 32,
        "root_span_id": "2" * 16,
        "wait_span_id": "3" * 16,
        "started_at": "2026-08-11T12:00:00+00:00",
        "ended_at": "2026-08-11T12:00:01+00:00",
    }
    second_job = SourceSyncJob(
        deployment="test",
        sources=("notes",),
        run_mode="scope_incomplete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="notes",
                trigger_kind="fs_event",
                scope_key="second.md",
            ),
        ),
        dependent_fanout="direct",
    )
    inputs, serialization = serialize_args(
        (
            ["docs"],
            "scope_incomplete",
            True,
            "direct",
            [first_event.to_dict()],
            True,
            True,
            False,
            [],
            None,
            False,
            canonical_telemetry,
        ),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text,
              queue_name text,
              deduplication_id text,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              priority integer,
              inputs text,
              serialization text
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, deduplication_id,
              created_at, updated_at, priority, inputs, serialization
            )
            VALUES (
              'wf-existing', 'ENQUEUED', 'okg_source_sync',
              'okg-test-source-sync',
              'sync:test:watch:scope_incomplete:docs:direct:1',
              %s, %s, 100, %s, %s
            )
            """,
            (now_ms, now_ms, inputs, serialization),
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=second_job,
            deduplication_id=(
                "sync:test:watch:scope_incomplete:docs:direct:1"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-existing"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT inputs, serialization
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = 'wf-existing'
                """
            )
            row = cur.fetchone()
            cur.execute("SELECT count(*) FROM dbos.workflow_status")
            assert cur.fetchone()[0] == 1
        workflow_inputs = deserialize_args(
            row[0],
            row[1],
            DBOSDefaultSerializer,
        )
        assert workflow_inputs["args"][0] == ["docs", "notes"]
        assert workflow_inputs["args"][10] is False
        assert workflow_inputs["args"][11] == canonical_telemetry
        events = workflow_inputs["args"][4]
        assert [event["scope_key"] for event in events] == [
            "first.md",
            "second.md",
        ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


@pytest.mark.postgres
def test_compacted_source_sync_dbos_merges_across_time_buckets(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        deserialize_args,
        serialize_args,
    )

    def fail_if_enqueue_called(**_kwargs):
        raise AssertionError("expected queued DBOS row to be compacted")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fail_if_enqueue_called,
    )
    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    first_event = SourceSyncEvent(
        deployment="test",
        source_name="docs",
        trigger_kind="fs_event",
        scope_key="first.md",
    )
    second_job = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_incomplete",
        events=(
            SourceSyncEvent(
                deployment="test",
                source_name="docs",
                trigger_kind="fs_event",
                scope_key="second.md",
            ),
        ),
        dependent_fanout="direct",
    )
    inputs, serialization = serialize_args(
        (
            ["docs"],
            "scope_incomplete",
            True,
            "direct",
            [first_event.to_dict()],
            True,
        ),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text,
              queue_name text,
              deduplication_id text,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              priority integer,
              inputs text,
              serialization text
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, deduplication_id,
              created_at, updated_at, priority, inputs, serialization
            )
            VALUES (
              'wf-existing', 'ENQUEUED', 'okg_source_sync',
              'okg-test-source-sync',
              'sync:test:watch:scope_incomplete:docs:direct:1',
              %s, %s, 100, %s, %s
            )
            """,
            (now_ms, now_ms, inputs, serialization),
        )

    try:
        workflow_id = enqueue_compacted_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=second_job,
            deduplication_id=(
                "sync:test:watch:scope_incomplete:docs:direct:2"
            ),
            compaction=compaction,
        )

        assert workflow_id == "wf-existing"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT inputs, serialization
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = 'wf-existing'
                """
            )
            row = cur.fetchone()
            cur.execute("SELECT count(*) FROM dbos.workflow_status")
            assert cur.fetchone()[0] == 1
        workflow_inputs = deserialize_args(
            row[0],
            row[1],
            DBOSDefaultSerializer,
        )
        events = workflow_inputs["args"][4]
        assert [event["scope_key"] for event in events] == [
            "first.md",
            "second.md",
        ]
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


@pytest.mark.postgres
def test_source_sync_compaction_skips_locked_enqueued_rows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        serialize_args,
    )

    enqueued: list[str | None] = []

    def fake_enqueue(**kwargs):
        enqueued.append(kwargs.get("deduplication_id"))
        return DBOSWorkflowEnqueueResult("wf-new")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fake_enqueue,
    )
    compaction = SourceSyncCompaction(
        window_seconds=60,
        max_events=10,
        max_scope_keys=10,
    )
    event = SourceSyncEvent(
        deployment="test",
        source_name="docs",
        trigger_kind="fs_event",
        scope_key="first.md",
    )
    inputs, serialization = serialize_args(
        (
            ["docs"],
            "scope_incomplete",
            True,
            "direct",
            [event.to_dict()],
            True,
        ),
        {},
        WorkflowSerializationFormat.NATIVE,
        DBOSDefaultSerializer,
    )
    now_ms = int(time.time() * 1000)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text,
              queue_name text,
              deduplication_id text,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              priority integer,
              inputs text,
              serialization text
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, deduplication_id,
              created_at, updated_at, priority, inputs, serialization
            )
            VALUES (
              'wf-existing', 'ENQUEUED', 'okg_source_sync',
              'okg-test-source-sync',
              'sync:test:watch:scope_incomplete:docs:direct:1',
              %s, %s, 100, %s, %s
            )
            """,
            (now_ms, now_ms, inputs, serialization),
        )

    locker = psycopg.connect(clean_runtime)
    try:
        with locker.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                """
                SELECT workflow_uuid
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = 'wf-existing'
                 FOR UPDATE
                """
            )

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                enqueue_compacted_source_sync_via_dbos,
                deployment="test",
                dsn=clean_runtime,
                job=SourceSyncJob(
                    deployment="test",
                    sources=("docs",),
                    run_mode="scope_incomplete",
                    events=(
                        SourceSyncEvent(
                            deployment="test",
                            source_name="docs",
                            trigger_kind="fs_event",
                            scope_key="second.md",
                        ),
                    ),
                    dependent_fanout="direct",
                ),
                deduplication_id=(
                    "sync:test:watch:scope_incomplete:docs:direct:1"
                ),
                compaction=compaction,
            )
            try:
                workflow_id = future.result(timeout=1.0)
            except TimeoutError:
                locker.rollback()
                raise AssertionError(
                    "source-sync compaction waited on a locked DBOS row"
                )

        assert workflow_id == "wf-new"
        assert enqueued == ["sync:test:watch:scope_incomplete:docs:direct:1"]
    finally:
        locker.rollback()
        locker.close()
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_compacted_source_sync_dbos_uses_successor_when_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_compact(**_kwargs):
        return None

    def fake_enqueue_source_sync_via_dbos_result(**kwargs):
        dedupe = kwargs["deduplication_id"]
        calls.append(dedupe)
        if dedupe.endswith(":successor"):
            return DBOSWorkflowEnqueueResult("wf-successor")
        return DBOSWorkflowEnqueueResult("wf-running", deduplicated=True)

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_compact_dbos_queued_source_sync",
        fake_compact,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_compact_dbos_queued_source_sync_by_prefix",
        fake_compact,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_compact_dbos_queued_source_sync_by_compatibility",
        fake_compact,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "_enqueue_source_sync_via_dbos_result",
        fake_enqueue_source_sync_via_dbos_result,
    )

    workflow_id = enqueue_compacted_source_sync_via_dbos(
        deployment="test",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="test",
            sources=("docs",),
            run_mode="scope_incomplete",
        ),
        deduplication_id="sync:test:watch:scope_incomplete:docs:direct:1",
        compaction=SourceSyncCompaction(window_seconds=60),
    )

    assert workflow_id == "wf-successor"
    assert calls == [
        "sync:test:watch:scope_incomplete:docs:direct:1",
        "sync:test:watch:scope_incomplete:docs:direct:successor",
    ]
