"""``request_republish`` must enqueue a workflow a worker actually runs.

Regression cover for the boundary defect where the public SDK enqueued
``workflow_name="republish_request"`` with ``(deployment, reason,
after_generation)``. No worker registers that name and no registered
workflow takes those arguments, so the SDK returned a workflow id for a
durable row that was never dispatched — success on the caller's side,
silence on the runtime's side.

The binding test drives the real path: a launched DBOS runtime with the
publisher workflow registered under its production name, the deployment
partition queue registered, and ``request_republish()`` called through a
bound SDK context. The cycle body is stubbed so the assertion is about
dispatch and argument binding, not about publishing content.
"""
from __future__ import annotations

import base64
import inspect
import pickle
import time

import psycopg
import pytest


# NOT a file-level mark. A file-level mark would hide the cheap
# signature-binding regression from the per-PR no-database lanes. The two
# database-backed tests carry ``postgres`` per test, exactly like the sibling
# launched-runtime test in test_source_sync_workflow.py, so the per-PR
# substrate-db lane owns and runs them. ``dbos_integration`` is deliberately
# NOT used: scripts/ci/lane_catalog.py lists it in FORBIDDEN_MARKERS, so a
# node carrying it cannot execute in any blocking lane.


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


@pytest.mark.postgres
def test_request_republish_runs_the_registered_publisher_workflow(
    clean_runtime, monkeypatch,
):
    """The enqueued row reaches a terminal cycle result on a real worker.

    Before the fix this test fails at the terminal-status assertion:
    the row sits unclaimed (PENDING) forever because no worker registers
    ``republish_request``.
    """
    from dbos import DBOS, DBOSConfig

    from okg.deployment import request_republish
    from okg.deployment._internal.context import bind_test_context
    from okg.substrate.runtime import partition_queue, publisher_workflow
    from okg.substrate.runtime.engine import defensive_destroy
    from okg.substrate.runtime.partition_queue import (
        register_deployment_partition_queue,
    )
    from okg.substrate.runtime.publisher_workflow import (
        PUBLISHER_WORKFLOW_NAME,
    )

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
                name="okg-request-republish-test",
                system_database_url=clean_runtime,
                database_url=clean_runtime,
                run_admin_server=False,
            )
        )
        register_deployment_partition_queue(deployment="test")
        publisher_workflow.register_publisher_workflow(deployment="test")
        DBOS.launch()
        with psycopg.connect(clean_runtime) as conn:
            with bind_test_context(deployment="test", conn=conn):
                workflow_id = request_republish(
                    reason="regression proof",
                    branch_name="probe-branch",
                    force_enrichers=True,
                )
        status, output = _wait_for_terminal_status(
            clean_runtime, workflow_id,
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
            (workflow_id,),
        )
        name, queue_name = cur.fetchone()

    assert name == PUBLISHER_WORKFLOW_NAME
    assert queue_name == "okg-test"
    assert cycles == [
        {
            "deployment": "test",
            "branch_name": "probe-branch",
            "force_enrichers": True,
        }
    ]


@pytest.mark.postgres
def test_request_republish_refuses_without_a_registered_worker_runtime(
    clean_runtime,
):
    """No registered runtime means no dispatch, so the SDK must fail.

    Enqueuing here would return an id for a row nothing ever runs.
    """
    from okg.deployment import request_republish
    from okg.deployment._internal.context import bind_test_context
    from okg.substrate.runtime.engine import defensive_destroy

    defensive_destroy()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    expected = "no registered DBOS application version"

    # Branch 1: the dbos schema does not exist at all.
    with psycopg.connect(clean_runtime) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with pytest.raises(RuntimeError, match=expected):
                request_republish(reason="no worker has ever booted")

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

    with psycopg.connect(clean_runtime) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with pytest.raises(RuntimeError, match=expected):
                request_republish(reason="version table is empty")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def test_request_republish_rejects_a_blank_reason():
    """``reason`` is the audit annotation; a blank one is refused.

    Validated before any context or database access, so this needs no
    Postgres and runs in the per-PR lanes.
    """
    from okg.deployment import request_republish

    with pytest.raises(ValueError, match="non-blank reason"):
        request_republish(reason="   ")


def test_request_republish_arguments_bind_to_the_publisher_workflow(
    monkeypatch,
):
    """Every forwarded argument is a real publisher-workflow parameter.

    ``after_generation`` was removed rather than left silently ignored,
    so passing it is a ``TypeError`` at the call site.
    """
    from okg.deployment import request_republish
    from okg.deployment.testing import mock_dbos_context

    sdk = inspect.signature(request_republish).parameters
    assert set(sdk) == {"reason", "branch_name", "force_enrichers"}
    assert "after_generation" not in sdk

    from okg.substrate.runtime import publisher_workflow

    # The mock decorator returns the UNdecorated function; leaving it in
    # the module global would let a later test's publisher_cycle_workflow()
    # call run inline instead of as a DBOS workflow.
    monkeypatch.setattr(publisher_workflow, "_DECORATED", {})

    with mock_dbos_context():
        from okg.substrate.runtime.publisher_workflow import (
            publisher_cycle_workflow,
            register_publisher_workflow,
        )

        register_publisher_workflow(deployment="test")
        workflow_params = list(
            inspect.signature(publisher_cycle_workflow()).parameters
        )

    # request_republish forwards (branch_name, force_enrichers)
    # positionally; they must be the workflow's leading parameters in
    # that order.
    assert workflow_params[:2] == ["branch_name", "force_enrichers"]


def test_registered_app_version_read_does_not_swallow_connection_errors():
    """An unreachable database is not "no worker has ever booted".

    ``dbos_enqueue.latest_registered_app_version`` returns None on any
    exception because it only decorates an enqueue. Reusing it as the
    refusal predicate would turn a transient connect stall into a
    confident, wrong "start the worker" diagnosis.
    """
    import psycopg

    from okg.deployment._internal.external import _registered_app_version

    unreachable = (
        "postgresql://postgres@127.0.0.1:1/okg_not_a_database"
    )
    with pytest.raises(psycopg.OperationalError):
        _registered_app_version(unreachable)
