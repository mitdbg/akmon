"""``client_enqueue_publisher_cycle`` must enqueue a dispatchable row.

Regression cover for issue #1531: the helper enqueued the publisher
workflow through a raw ``DBOSClient`` with no ``app_version`` stamp and
no check that a worker runtime had ever registered against the
deployment's database.

The load-bearing half is the missing check. With no worker ever booted,
the call queued durable work, polled it to its own timeout, and reported
``PENDING`` -- which reads as "still running" rather than "nothing will
run this".

The stamp is the second half. DBOS dispatches an unstamped row from any
worker (its dequeue predicate admits ``application_version IS NULL``),
so an unstamped row is not stranded outright; pinning it to the
registered version is what makes an operator enqueue follow the same
dispatch and restamping rules as scheduled work. The stamping proof
below therefore reads the row while no worker is running, because a
dequeue writes that column itself and would make a post-dispatch
assertion pass either way.

Same defect shape as #1433 / PR #1509 for ``request_republish``; this
file mirrors ``test_request_republish_contract.py`` on the operator
(``okg run --once``, promotion publish) side of the same workflow.
"""
from __future__ import annotations

import base64
import pickle
import time

import psycopg
import pytest


# NOT a file-level mark. A file-level mark would hide the cheap routing
# and error-propagation regressions from the per-PR no-database lanes.
# The database-backed tests carry ``postgres`` per test, exactly like
# test_request_republish_contract.py, so the per-PR substrate-db lane
# (core-db-contracts) owns and runs them. ``dbos_integration`` is
# deliberately NOT used: scripts/ci/lane_catalog.py lists it in
# FORBIDDEN_MARKERS, so a node carrying it cannot execute in any
# blocking lane.


UNREACHABLE_DSN = "postgresql://postgres@127.0.0.1:1/okg_not_a_database"


def _wait_for_terminal_status(
    dsn: str,
    workflow_id: str,
    *,
    timeout_seconds: float = 30.0,
) -> tuple[str | None, str | None]:
    """Poll ``dbos.workflow_status`` until the workflow is terminal."""
    deadline = time.monotonic() + timeout_seconds
    terminal = {
        "SUCCESS",
        "ERROR",
        "CANCELLED",
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    }
    status: str | None = None
    output: str | None = None
    with psycopg.connect(dsn, autocommit=True) as conn:
        while time.monotonic() < deadline:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, output
                      FROM dbos.workflow_status
                     WHERE workflow_uuid = %s
                    """,
                    (workflow_id,),
                )
                row = cur.fetchone()
            if row is not None:
                status = str(row[0])
                output = None if row[1] is None else str(row[1])
                if status in terminal:
                    return status, output
            time.sleep(0.1)
    return status, output


def test_publisher_cycle_enqueue_routes_through_the_deployment_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The enqueue goes through the shared, app_version-stamping helper.

    Before the fix this fails: the function built its own DBOSClient and
    called ``client.enqueue`` directly, so the helper -- the only place
    that stamps ``app_version`` onto the row -- was never reached.
    """
    from types import SimpleNamespace

    from okg.substrate.runtime import admin
    from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult
    from okg.substrate.runtime.publisher_workflow import (
        PUBLISHER_WORKFLOW_NAME,
    )

    captured: dict[str, object] = {}

    class FakeClient:
        def enqueue(self, options, *args):  # pragma: no cover - must not run
            raise AssertionError(
                "publisher enqueue bypassed enqueue_deployment_workflow"
            )

        def list_workflows(self, *, workflow_ids):
            assert workflow_ids == ["wf-routed"]
            return [
                SimpleNamespace(
                    status="SUCCESS",
                    output={"status": "published"},
                    error=None,
                ),
            ]

    def fake_enqueue(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult(workflow_id="wf-routed")

    monkeypatch.setattr(admin, "_build_client", lambda **_kwargs: FakeClient())
    monkeypatch.setattr(
        admin, "registered_app_version", lambda _dsn: "app-version-1",
    )
    monkeypatch.setattr(admin, "enqueue_deployment_workflow", fake_enqueue)

    result = admin.client_enqueue_publisher_cycle(
        deployment="test",
        dsn="postgres://unused",
        branch_name="feature",
        force_enrichers=True,
        poll_interval_seconds=0,
        promotion_id="promo-1",
    )

    assert result.status == "SUCCESS"
    assert result.workflow_id == "wf-routed"
    assert captured == {
        "deployment": "test",
        "dsn": "postgres://unused",
        "workflow_name": PUBLISHER_WORKFLOW_NAME,
        "args": ("feature", True),
        "promotion_id": "promo-1",
        # The version this call read strictly is handed to the helper, so
        # the row is pinned to what was checked rather than to a second,
        # best-effort read that can come back empty.
        "app_version": "app-version-1",
    }


def test_publisher_cycle_enqueue_refusal_does_not_swallow_connect_errors():
    """An unreachable database is not "no worker has ever booted".

    ``dbos_enqueue.latest_registered_app_version`` returns None on any
    exception because it only decorates an enqueue with a hint. Reusing
    it as the refusal predicate would turn a transient connect stall
    into a confident, wrong "start the worker" diagnosis.
    """
    from okg.substrate.runtime import admin

    with pytest.raises(psycopg.OperationalError):
        admin.client_enqueue_publisher_cycle(
            deployment="test",
            dsn=UNREACHABLE_DSN,
            poll_interval_seconds=0,
            timeout_seconds=0,
        )


@pytest.mark.postgres
def test_publisher_cycle_enqueue_refuses_without_a_registered_runtime(
    clean_runtime,
):
    """No registered runtime means no dispatch, so the call must fail.

    Before the fix this enqueued a durable row nothing would run and
    then reported ``PENDING`` once the poll timeout elapsed.
    """
    from okg.substrate.runtime import admin
    from okg.substrate.runtime.engine import defensive_destroy

    defensive_destroy()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    expected = "no registered DBOS application version"

    # Branch 1: the dbos schema does not exist at all.
    with pytest.raises(RuntimeError, match=expected):
        admin.client_enqueue_publisher_cycle(
            deployment="test",
            dsn=clean_runtime,
            poll_interval_seconds=0,
            timeout_seconds=0,
        )

    # Branch 2: the version table exists but is empty. Without this the
    # refusal is only ever proven through the missing-relation path, and
    # an empty catalog would pass vacuously.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA dbos")
        conn.execute(
            """
            CREATE TABLE dbos.application_versions (
                version_name TEXT PRIMARY KEY,
                version_timestamp BIGINT,
                created_at TIMESTAMPTZ
            )
            """
        )

    with pytest.raises(RuntimeError, match=expected):
        admin.client_enqueue_publisher_cycle(
            deployment="test",
            dsn=clean_runtime,
            poll_interval_seconds=0,
            timeout_seconds=0,
        )

    # Nothing durable was created by either refusal.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        assert cur.fetchone()[0] is None

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


@pytest.mark.postgres
def test_publisher_cycle_enqueue_binds_to_a_launched_worker_runtime(
    clean_runtime, monkeypatch,
):
    """The enqueued row is stamped and dispatched by a real worker.

    The cycle body is stubbed so the assertion is about dispatch,
    argument binding, and the application-version stamp, not about
    publishing content.
    """
    from dbos import DBOS, DBOSConfig

    from okg.substrate.runtime import admin, partition_queue, publisher_workflow
    from okg.substrate.runtime.engine import defensive_destroy
    from okg.substrate.runtime.partition_queue import (
        register_deployment_partition_queue,
    )
    from okg.substrate.runtime.publisher_workflow import (
        PUBLISHER_WORKFLOW_NAME,
    )

    # A distinct deployment name, so this file declares its own DBOS
    # queue. ``DBOS.destroy()`` leaves the process-global queue registry
    # in place, so a second declaration of ``okg-test`` raises "Queue
    # already declared" for whichever launched-runtime test runs second.
    # Restoring partition_queue._REGISTERED is not enough on its own.
    deployment = "adminq"

    defensive_destroy()
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})
    # setitem-style cleanup: the module global must not survive this test,
    # or a later test calling publisher_cycle_workflow() gets a closure
    # bound to this deployment and never exercises real registration.
    monkeypatch.setattr(publisher_workflow, "_DECORATED", {})

    cycles: list[dict] = []

    def fake_run_one_cycle(**kwargs):
        cycles.append(dict(kwargs))
        return {
            "status": "stubbed",
            "branch_name": kwargs.get("branch_name"),
            "force_enrichers": kwargs.get("force_enrichers"),
        }

    monkeypatch.setattr(
        publisher_workflow, "_run_one_cycle", fake_run_one_cycle,
    )
    monkeypatch.setattr(
        publisher_workflow,
        "_apply_admission_feedback_safely",
        lambda **kwargs: {"status": "stubbed"},
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    try:
        DBOS(
            config=DBOSConfig(
                name="okg-admin-enqueue-test",
                system_database_url=clean_runtime,
                database_url=clean_runtime,
                run_admin_server=False,
            )
        )
        register_deployment_partition_queue(deployment=deployment)
        publisher_workflow.register_publisher_workflow(
            deployment=deployment,
        )
        DBOS.launch()
        result = admin.client_enqueue_publisher_cycle(
            deployment=deployment,
            dsn=clean_runtime,
            branch_name="probe-branch",
            force_enrichers=True,
            timeout_seconds=60.0,
            poll_interval_seconds=0.2,
        )
        status, output = _wait_for_terminal_status(
            clean_runtime, result.workflow_id,
        )
    finally:
        DBOS.destroy()

    assert status == "SUCCESS", (
        f"publisher workflow never reached a terminal state: {status!r}"
    )
    assert output is not None
    # DBOS persists workflow output as a base64-encoded pickle.
    summary = pickle.loads(base64.b64decode(output))  # noqa: S301
    assert summary["status"] == "stubbed"
    assert summary["branch_name"] == "probe-branch"
    assert summary["force_enrichers"] is True

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, queue_name
              FROM dbos.workflow_status
             WHERE workflow_uuid = %s
            """,
            (result.workflow_id,),
        )
        name, queue_name = cur.fetchone()

    assert name == PUBLISHER_WORKFLOW_NAME
    assert queue_name == "okg-adminq"
    # application_version is deliberately NOT asserted here: the dequeue
    # writes that column itself, so this row would carry one with or
    # without the fix. The stamp is proven before dispatch, in
    # test_publisher_cycle_enqueue_pins_the_row_to_the_registered_version.
    assert cycles, "the publisher cycle never ran"
    assert cycles[0] == {
        "deployment": "adminq",
        "branch_name": "probe-branch",
        "force_enrichers": True,
    }


@pytest.mark.postgres
def test_publisher_cycle_enqueue_pins_the_row_to_the_registered_version(
    clean_runtime, monkeypatch,
):
    """The queued row carries the registered application version.

    Read before any worker is running: a DBOS dequeue writes
    ``application_version`` itself, so a post-dispatch read would show a
    version whether or not the enqueue stamped one. Here a runtime is
    booted only long enough to register a version, then destroyed, so
    the column can only hold what the enqueue put there. Before the fix
    it is NULL.
    """
    from dbos import DBOS, DBOSConfig

    from okg.substrate.runtime import admin, partition_queue, publisher_workflow
    from okg.substrate.runtime.engine import defensive_destroy
    from okg.substrate.runtime.partition_queue import (
        register_deployment_partition_queue,
    )

    deployment = "adminpin"

    defensive_destroy()
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})
    monkeypatch.setattr(publisher_workflow, "_DECORATED", {})

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    try:
        DBOS(
            config=DBOSConfig(
                name="okg-admin-pin-test",
                system_database_url=clean_runtime,
                database_url=clean_runtime,
                run_admin_server=False,
            )
        )
        register_deployment_partition_queue(deployment=deployment)
        publisher_workflow.register_publisher_workflow(deployment=deployment)
        DBOS.launch()
    finally:
        # No worker from here on, so the row stays ENQUEUED and nothing
        # else can write application_version.
        DBOS.destroy()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_name
              FROM dbos.application_versions
             ORDER BY version_timestamp DESC NULLS LAST,
                      created_at DESC NULLS LAST
             LIMIT 1
            """
        )
        registered = cur.fetchone()[0]
    assert registered, "the booted runtime registered no application version"

    result = admin.client_enqueue_publisher_cycle(
        deployment=deployment,
        dsn=clean_runtime,
        timeout_seconds=0,
        poll_interval_seconds=0,
    )
    assert result.status == "PENDING"

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, application_version
              FROM dbos.workflow_status
             WHERE workflow_uuid = %s
            """,
            (result.workflow_id,),
        )
        status, application_version = cur.fetchone()

    assert status == "ENQUEUED"
    assert application_version == registered, (
        "the enqueued row is not pinned to the registered application "
        f"version: {application_version!r} != {registered!r}"
    )


@pytest.mark.postgres
def test_publisher_cycle_enqueue_waits_for_a_first_registration(
    clean_runtime,
):
    """A bounded wait lets a worker that is still booting register.

    The promotion publish step runs right after it restarts the service,
    which does not itself wait for the worker to register. Refusing
    instantly there would abort a first-ever promotion onto a fresh
    database, so that caller passes a window. The window is bounded: a
    database that will never have a worker still fails.
    """
    import threading
    import time as _time

    from okg.substrate.runtime import admin
    from okg.substrate.runtime.engine import defensive_destroy

    defensive_destroy()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        conn.execute("CREATE SCHEMA dbos")
        conn.execute(
            """
            CREATE TABLE dbos.application_versions (
                version_name TEXT PRIMARY KEY,
                version_timestamp BIGINT,
                created_at TIMESTAMPTZ
            )
            """
        )

    # The window expires with the table still empty: refuse, do not hang.
    started = _time.monotonic()
    with pytest.raises(RuntimeError, match="no registered DBOS"):
        admin.client_enqueue_publisher_cycle(
            deployment="test",
            dsn=clean_runtime,
            timeout_seconds=0,
            poll_interval_seconds=0,
            registration_wait_seconds=1.0,
        )
    waited = _time.monotonic() - started
    assert waited >= 1.0, "the wait window was not honored"
    assert waited < 20.0, "the bounded wait did not end"

    # A registration that lands inside the window is picked up rather
    # than refused. The enqueue itself is stubbed: this asserts the
    # guard's decision, not DBOS behavior.
    def register_late():
        _time.sleep(0.5)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO dbos.application_versions "
                "(version_name, version_timestamp) VALUES ('late', 1)"
            )

    seen: dict[str, object] = {}

    class FakeClient:
        def list_workflows(self, *, workflow_ids):
            from types import SimpleNamespace

            return [
                SimpleNamespace(status="SUCCESS", output={}, error=None),
            ]

    def fake_enqueue(**kwargs):
        from okg.substrate.runtime.dbos_enqueue import (
            DBOSWorkflowEnqueueResult,
        )

        seen.update(kwargs)
        return DBOSWorkflowEnqueueResult(workflow_id="wf-late")

    thread = threading.Thread(target=register_late)
    thread.start()
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(admin, "_build_client", lambda **_kw: FakeClient())
            patch.setattr(admin, "enqueue_deployment_workflow", fake_enqueue)
            result = admin.client_enqueue_publisher_cycle(
                deployment="test",
                dsn=clean_runtime,
                timeout_seconds=0,
                poll_interval_seconds=0,
                registration_wait_seconds=10.0,
            )
    finally:
        thread.join()

    assert result.workflow_id == "wf-late"
    assert seen["app_version"] == "late"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


# ---------------------------------------------------------------------------
# Issue #2502: a registered but stopped worker must not cost a 600 s wait.
# No database: the queue, the row status and the cancel are faked, so these
# run in the no-database lanes beside the routing test above.
# ---------------------------------------------------------------------------


class _ScriptedClient:
    """Fake DBOSClient whose row status follows ``statuses``.

    The last status repeats. ``running`` is what a PENDING read of the
    queue returns.
    """

    def __init__(self, statuses, *, running=(), queue_read_error=None):
        self._statuses = list(statuses)
        self.running = list(running)
        self.queue_read_error = queue_read_error
        self.queue_reads: list[dict] = []

    def list_workflows(self, *, workflow_ids=None, **kwargs):
        from types import SimpleNamespace

        if workflow_ids is None:
            self.queue_reads.append(kwargs)
            if self.queue_read_error is not None:
                raise self.queue_read_error
            return [SimpleNamespace(workflow_id=w) for w in self.running]
        status = (
            self._statuses.pop(0)
            if len(self._statuses) > 1
            else self._statuses[0]
        )
        return [
            SimpleNamespace(
                status=status,
                output={"status": "published"} if status == "SUCCESS" else None,
                error=None,
            ),
        ]


def _patch_publisher_enqueue(monkeypatch, client, *, cancel=None):
    from okg.substrate.runtime import admin
    from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult

    cancels: list[dict] = []

    def fake_cancel(**kwargs):
        cancels.append(kwargs)
        if cancel is not None:
            return cancel(**kwargs)
        return None

    monkeypatch.setattr(admin, "_build_client", lambda **_kwargs: client)
    monkeypatch.setattr(
        admin, "registered_app_version", lambda _dsn: "app-version-1",
    )
    monkeypatch.setattr(
        admin,
        "enqueue_deployment_workflow",
        lambda **_kwargs: DBOSWorkflowEnqueueResult(workflow_id="wf-2502"),
    )
    monkeypatch.setattr(admin, "cancel_deployment_workflow", fake_cancel)
    return cancels


def test_publisher_cycle_never_dequeued_is_cancelled_and_refused(monkeypatch):
    """A row no worker takes is cancelled and refused inside the window.

    Before the fix this call polled the ENQUEUED row to the 600 s
    timeout and returned PENDING ("not awaited"). The overall timeout
    here is an hour, so only the dequeue window can end the call.
    """
    import time as _time

    from okg.substrate.runtime import admin

    client = _ScriptedClient(["ENQUEUED"])
    cancels = _patch_publisher_enqueue(monkeypatch, client)

    started = _time.monotonic()
    with pytest.raises(admin.PublisherCycleNotDequeued) as raised:
        admin.client_enqueue_publisher_cycle(
            deployment="demo",
            dsn="postgres://unused",
            timeout_seconds=3600.0,
            poll_interval_seconds=0.01,
            dequeue_timeout_seconds=0.05,
        )
    waited = _time.monotonic() - started

    assert waited < 5.0, "the dequeue window did not bound the wait"
    err = raised.value
    assert err.code == "publisher_cycle_not_dequeued"
    assert cancels == [
        {
            "dsn": "postgres://unused",
            "deployment": "demo",
            "workflow_id": "wf-2502",
            # Only a row no worker has started may be cancelled.
            "expected_status": "ENQUEUED",
        },
    ]
    assert err.data["queue_name"] == "okg-demo"
    assert err.data["workflow_id"] == "wf-2502"
    assert err.data["cancelled"] is True
    assert "okg-demo" in err.message
    assert "okg runtime worker" in err.fix_hint
    assert "--inline" in err.fix_hint
    # The queue was read for a running predecessor before cancelling.
    assert client.queue_reads
    assert client.queue_reads[0]["queue_name"] == "okg-demo"
    assert client.queue_reads[0]["status"] == "PENDING"
    # The registry row (data_shape, python_exception) is checked against
    # this emitter in tests/substrate/test_error_codes_parity.py, which
    # runs outside the database lane and may read docs/.


def test_publisher_cycle_dequeued_by_a_worker_keeps_the_overall_wait(
    monkeypatch,
):
    """Once a worker takes the row, the dequeue window no longer applies."""
    from okg.substrate.runtime import admin

    client = _ScriptedClient(["PENDING", "PENDING", "PENDING", "SUCCESS"])
    cancels = _patch_publisher_enqueue(monkeypatch, client)

    result = admin.client_enqueue_publisher_cycle(
        deployment="demo",
        dsn="postgres://unused",
        poll_interval_seconds=0.01,
        # Already expired: a PENDING row past the window must still be
        # waited on, not cancelled.
        dequeue_timeout_seconds=0.0,
    )

    assert result.status == "SUCCESS"
    assert result.summary == {"status": "published"}
    assert cancels == []
    assert client.queue_reads == []


def test_publisher_cycle_behind_a_running_cycle_is_not_refused(monkeypatch):
    """A row waiting its turn on a busy queue is not a missing worker."""
    from okg.substrate.runtime import admin

    client = _ScriptedClient(["ENQUEUED"], running=["wf-scheduled"])
    cancels = _patch_publisher_enqueue(monkeypatch, client)

    result = admin.client_enqueue_publisher_cycle(
        deployment="demo",
        dsn="postgres://unused",
        timeout_seconds=0.2,
        poll_interval_seconds=0.01,
        dequeue_timeout_seconds=0.0,
    )

    assert result.status == "PENDING"
    assert cancels == []
    assert client.queue_reads, "the queue was never checked for a predecessor"


def test_publisher_cycle_unreadable_queue_is_never_cancelled(monkeypatch):
    """A failed queue read must not be taken as an idle queue."""
    from okg.substrate.runtime import admin

    client = _ScriptedClient(
        ["ENQUEUED"], queue_read_error=RuntimeError("queue read failed"),
    )
    cancels = _patch_publisher_enqueue(monkeypatch, client)

    result = admin.client_enqueue_publisher_cycle(
        deployment="demo",
        dsn="postgres://unused",
        timeout_seconds=0.2,
        poll_interval_seconds=0.01,
        dequeue_timeout_seconds=0.0,
    )

    assert result.status == "PENDING"
    assert cancels == []


def test_publisher_cycle_without_a_window_keeps_the_old_wait(monkeypatch):
    """The default sets no dequeue window, so promotion and MCP callers,
    which do not pass one, never get the refusal."""
    from okg.substrate.runtime import admin

    client = _ScriptedClient(["ENQUEUED"])
    cancels = _patch_publisher_enqueue(monkeypatch, client)

    result = admin.client_enqueue_publisher_cycle(
        deployment="demo",
        dsn="postgres://unused",
        timeout_seconds=0.1,
        poll_interval_seconds=0.01,
    )

    assert result.status == "PENDING"
    assert cancels == []
    assert client.queue_reads == []


def test_publisher_cycle_cancel_failure_is_not_reported_as_cancelled(
    monkeypatch,
):
    """A failed cancel propagates instead of becoming a refusal that
    says the row was cancelled."""
    from okg.substrate.runtime import admin

    def broken_cancel(**_kwargs):
        raise psycopg.OperationalError("connection refused")

    client = _ScriptedClient(["ENQUEUED"])
    _patch_publisher_enqueue(monkeypatch, client, cancel=broken_cancel)

    with pytest.raises(psycopg.OperationalError):
        admin.client_enqueue_publisher_cycle(
            deployment="demo",
            dsn="postgres://unused",
            poll_interval_seconds=0.01,
            dequeue_timeout_seconds=0.0,
        )


@pytest.mark.postgres
def test_publisher_cycle_not_dequeued_against_a_real_workflow_table(
    clean_runtime, monkeypatch,
):
    """Real dbos.workflow_status rows, no worker running (#2502).

    A runtime boots only long enough to register a version, as in the
    pinning test above. Then: an ENQUEUED row no worker takes is
    cancelled and refused; a row a worker has started (PENDING) is
    refused by the ENQUEUED-only cancel and left running.
    """
    from dbos import DBOS, DBOSConfig

    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime import admin, partition_queue, publisher_workflow
    from okg.substrate.runtime.engine import defensive_destroy
    from okg.substrate.runtime.partition_queue import (
        register_deployment_partition_queue,
    )
    from okg.substrate.runtime.workflow_cancel import (
        cancel_deployment_workflow,
    )

    deployment = "admindq"

    defensive_destroy()
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})
    monkeypatch.setattr(publisher_workflow, "_DECORATED", {})

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    try:
        DBOS(
            config=DBOSConfig(
                name="okg-admin-dequeue-test",
                system_database_url=clean_runtime,
                database_url=clean_runtime,
                run_admin_server=False,
            )
        )
        register_deployment_partition_queue(deployment=deployment)
        publisher_workflow.register_publisher_workflow(deployment=deployment)
        DBOS.launch()
    finally:
        DBOS.destroy()

    def _status(workflow_id):
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM dbos.workflow_status "
                " WHERE workflow_uuid = %s",
                (workflow_id,),
            )
            return cur.fetchone()[0]

    started = time.monotonic()
    with pytest.raises(admin.PublisherCycleNotDequeued) as raised:
        admin.client_enqueue_publisher_cycle(
            deployment=deployment,
            dsn=clean_runtime,
            timeout_seconds=600.0,
            poll_interval_seconds=0.1,
            dequeue_timeout_seconds=0.5,
        )
    assert time.monotonic() - started < 30.0
    refused_id = raised.value.data["workflow_id"]
    assert _status(refused_id) == "CANCELLED"

    # A row a worker has started is not cancelled by the guarded cancel.
    pending = admin.client_enqueue_publisher_cycle(
        deployment=deployment,
        dsn=clean_runtime,
        timeout_seconds=0,
        poll_interval_seconds=0,
    )
    assert _status(pending.workflow_id) == "ENQUEUED"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE dbos.workflow_status SET status = 'PENDING' "
            " WHERE workflow_uuid = %s",
            (pending.workflow_id,),
        )
    with pytest.raises(SubstrateError) as refused:
        cancel_deployment_workflow(
            dsn=clean_runtime,
            deployment=deployment,
            workflow_id=pending.workflow_id,
            expected_status="ENQUEUED",
        )
    assert refused.value.code == "workflow_status_changed"
    assert _status(pending.workflow_id) == "PENDING"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_publisher_cycle_rejects_a_negative_dequeue_window():
    from okg.substrate.runtime import admin

    with pytest.raises(ValueError, match="dequeue_timeout_seconds"):
        admin.client_enqueue_publisher_cycle(
            deployment="demo",
            dsn=UNREACHABLE_DSN,
            dequeue_timeout_seconds=-1.0,
        )
