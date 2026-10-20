"""Tests for `cleanup-orphan-runtime-schedules`.

`cleanup_disabled_feature_schedules` deletes stale
`dbos.workflow_schedules` rows + cancels orphan ENQUEUED/PENDING
workflows so a worker boot starts with a clean queue when a feature
gate flipped from True (previous boot) to False (this boot).

Most tests use an in-process psycopg fake so they do not touch the
shared `okg_test` DBOS schema. The replay-boundary regression also has
an explicit real-Postgres case, which callers must point at a disposable
isolated database.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

import psycopg
import pytest
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


@pytest.fixture(autouse=True)
def _drop_reduced_workflow_status(request):
    """Leave no cut-down ``dbos.workflow_status`` behind in a shared database.

    Several tests here CREATE a six-column ``dbos.workflow_status`` to drive the
    cutover gate. The real DBOS table has more, including ``updated_at``.
    Left in place, that stub is a trap for anything later in the same database
    that reads the real schema: ``check_deployment_doctor_health`` selects
    ``updated_at`` and dies with ``UndefinedColumn``. `clean_runtime` does not
    truncate or drop it, so the debris outlives the test that made it.

    That was only ever latent while these two files never shared a lane. They
    do now -- ``tests/substrate/runtime/test_deployment_doctor_wiring.py``
    joined ``core-db-contracts`` under issue #1310 -- so the debris is swept
    here, at the file that creates it.

    Drops ONLY the reduced stub: a real ``dbos.workflow_status`` (one that has
    ``updated_at``) is left untouched.
    """
    # Resolved BEFORE the yield: requesting it at teardown time races
    # `clean_runtime`'s own teardown and pytest refuses with "the fixture
    # value is not available".
    dsn = (
        request.getfixturevalue("clean_runtime")
        if "clean_runtime" in request.fixturenames
        else None
    )
    yield
    if dsn is None:
        return
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            return
        cur.execute(
            """
            SELECT count(*)
              FROM information_schema.columns
             WHERE table_schema = 'dbos'
               AND table_name = 'workflow_status'
               AND column_name = 'updated_at'
            """
        )
        if int(cur.fetchone()[0]) > 0:
            return
        cur.execute("DROP TABLE dbos.workflow_status")



def _fake_policy_match(policy_pairs, queue_name, name) -> bool:
    """Mirror ``_replaceable_cutover_predicate`` for the in-memory store.

    The one prefix policy (the source-poll family, issue #3519) is the only
    policy name ending in ``_``; every other policy name is exact.
    """
    for policy_queue, policy_name in policy_pairs:
        if policy_queue != queue_name or name is None:
            continue
        if policy_name.endswith("_"):
            if name.startswith(policy_name):
                return True
        elif name == policy_name:
            return True
    return False


class _FakeCursor:
    """Records DELETE + UPDATE counts that the helper triggers."""

    def __init__(self, store: "_FakeStore"):
        self.store = store
        self.rowcount = 0
        self._returning_rows: list[tuple] = []
        self._fetchone: tuple | None = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        sql_upper = sql.upper().strip()
        self._returning_rows = []
        self._fetchone = None
        if sql_upper.startswith("DELETE FROM DBOS.WORKFLOW_SCHEDULES"):
            schedule_id = params[0]
            n = self.store.schedules.pop(schedule_id, None)
            self.rowcount = 1 if n is not None else 0
        elif sql_upper.startswith("LOCK TABLE DBOS.WORKFLOW_STATUS"):
            return
        elif sql_upper.startswith(
            "SELECT SET_CONFIG('IDLE_IN_TRANSACTION_SESSION_TIMEOUT'"
        ):
            # The cutover boundary lifts the connection factory's
            # idle-in-transaction bound for its own transaction (issue #1310):
            # it deliberately stays open through DBOS.launch(), which on a
            # first launch runs migrations. A session GUC has nothing to
            # simulate here.
            return
        elif sql_upper.startswith(
            "SELECT PG_ADVISORY_XACT_LOCK(HASHTEXTEXTENDED"
        ):
            return
        elif sql_upper.startswith("SELECT TO_REGCLASS"):
            self._fetchone = ("dbos.workflow_status",)
            return
        elif (
            "RUNTIME_CUTOVER_REPLACEABLE" in sql_upper
            and "SELECT WORKFLOW_UUID, NAME, QUEUE_NAME" in sql_upper
        ):
            live_version = params[0]
            policy_pairs = tuple(zip(params[1::2], params[2::2]))

            def eligible(row):
                name, status, queue_name = row[:3]
                started_at = row[6] if len(row) > 6 else None
                exact_policy = _fake_policy_match(policy_pairs, queue_name, name)
                pending_safe = (
                    status != "PENDING"
                    or name == "okg_source_sync"
                    or started_at is None
                )
                return exact_policy and pending_safe

            self._returning_rows = [
                (
                    wf_id, row[0], row[2], row[1], row[3],
                    row[6] if len(row) > 6 else None,
                )
                for wf_id, row in self.store.statuses.items()
                if len(row) >= 5
                and row[1] in ("ENQUEUED", "DELAYED", "PENDING")
                and row[3] != live_version
                and not eligible(row)
            ][:20]
            return
        elif (
            "RUNTIME_CUTOVER_REPLACEABLE" in sql_upper
            and "UPDATE DBOS.WORKFLOW_STATUS" in sql_upper
        ):
            live_version = params[0]
            policy_pairs = tuple(zip(params[1::2], params[2::2]))
            for wf_id, row in list(self.store.statuses.items()):
                if len(row) < 5:
                    continue
                name, status, queue_name, app_ver = row[:4]
                started_at = row[6] if len(row) > 6 else None
                pending_safe = (
                    status != "PENDING"
                    or name == "okg_source_sync"
                    or started_at is None
                )
                if (
                    _fake_policy_match(policy_pairs, queue_name, name)
                    and pending_safe
                    and status in ("ENQUEUED", "DELAYED", "PENDING")
                    and app_ver != live_version
                ):
                    self.store.statuses[wf_id] = (
                        name, "CANCELLED", *row[2:],
                    )
                    self._returning_rows.append(
                        (
                            wf_id, name, queue_name, "CANCELLED", app_ver,
                            started_at,
                        )
                    )
            self.rowcount = len(self._returning_rows)
            return
        elif sql_upper.startswith("UPDATE DBOS.WORKFLOW_STATUS"):
            if "APPLICATION_VERSION IS DISTINCT FROM" in sql_upper:
                queue_name, workflow_name, live_version = params
                cancelled = 0
                for wf_id, row in list(self.store.statuses.items()):
                    if len(row) < 5:
                        continue
                    name, status, queue, app_ver = row[:4]
                    if (
                        queue == queue_name
                        and name == workflow_name
                        and status in ("ENQUEUED", "DELAYED", "PENDING")
                        and app_ver != live_version
                    ):
                        self.store.statuses[wf_id] = (
                            name, "CANCELLED", *row[2:]
                        )
                        self._returning_rows.append(
                            (wf_id, "CANCELLED", app_ver)
                        )
                        cancelled += 1
                self.rowcount = cancelled
                return
            # Two shapes:
            #  (1) `cleanup_disabled_feature_schedules`: WHERE name=%s AND
            #      status IN ('ENQUEUED','PENDING')  → params = (name,)
            #  (2) `cleanup_stale_pending_workflows`: WHERE queue_name=%s
            #      AND status='PENDING' AND updated_at < %s
            #      → params = (queue_name, cutoff_ms),
            #      RETURNING workflow_uuid, name, application_version,
            #      updated_at
            if "QUEUE_NAME" in sql_upper:
                queue_name, cutoff_ms = params[:2]
                recovered_cutoff_ms = params[2] if len(params) > 2 else cutoff_ms
                cancelled = 0
                enqueued_cleanup = "STATUS = 'ENQUEUED'" in sql_upper
                never_started_only = (
                    "OKG_NO_STARTED_PENDING_CANCEL" in sql_upper
                )
                for wf_id, row in list(self.store.statuses.items()):
                    name, status, queue, app_ver, updated_at = row[:5]
                    created_at = (
                        row[5]
                        if len(row) > 5 and isinstance(row[5], int)
                        else updated_at
                    )
                    started_at = (
                        row[6]
                        if len(row) > 6 and isinstance(row[6], int)
                        else None
                    )
                    deduplication_id = row[7] if len(row) > 7 else None
                    recovery_attempts = row[8] if len(row) > 8 else 0
                    priority = (
                        row[8]
                        if (
                            enqueued_cleanup
                            and len(row) > 8
                            and isinstance(row[8], int)
                        )
                        else None
                    )
                    replaceable = (
                        name == "okg_publisher_cycle"
                        or name == "okg_source_sync"
                        or name.startswith("okg_source_poll_")
                    )
                    routine_source_sync = (
                        name != "okg_source_sync"
                        or priority is None
                        or priority >= 100
                    )
                    if enqueued_cleanup:
                        if (
                            queue == queue_name
                            and status == "ENQUEUED"
                            and updated_at < cutoff_ms
                            and replaceable
                            and routine_source_sync
                        ):
                            self.store.statuses[wf_id] = (
                                name, "CANCELLED", queue, app_ver,
                                *row[4:],
                            )
                            self._returning_rows.append(
                                (
                                    wf_id,
                                    name,
                                    app_ver,
                                    created_at,
                                    updated_at,
                                    deduplication_id,
                                )
                            )
                            cancelled += 1
                        continue
                    stale_anchor = max(
                        updated_at,
                        created_at or updated_at,
                        started_at or updated_at,
                    )
                    recovered_freshness_anchor = max(
                        updated_at,
                        started_at or updated_at,
                    )
                    recovered_replaceable_stale = (
                        replaceable
                        and recovery_attempts > 0
                        and started_at is None
                        and (created_at or updated_at) < recovered_cutoff_ms
                        and recovered_freshness_anchor < recovered_cutoff_ms
                    )
                    if (
                        queue == queue_name
                        and status == "PENDING"
                        and (
                            (
                                stale_anchor < cutoff_ms
                                and not (
                                    never_started_only
                                    and started_at is not None
                                )
                            )
                            or recovered_replaceable_stale
                        )
                    ):
                        self.store.statuses[wf_id] = (
                            name, "CANCELLED", queue, app_ver,
                            *row[4:],
                        )
                        self._returning_rows.append(
                            (
                                wf_id,
                                name,
                                app_ver,
                                updated_at,
                                created_at,
                                started_at,
                                stale_anchor,
                            )
                        )
                        cancelled += 1
                self.rowcount = cancelled
            else:
                name_param = params[0]
                cancelled = 0
                for wf_id, row in list(self.store.statuses.items()):
                    # Detect legacy 2-tuple vs new 5-tuple shape.
                    if len(row) == 2:
                        wf_name, status = row
                        if (
                            wf_name == name_param
                            and status in ("ENQUEUED", "PENDING")
                        ):
                            self.store.statuses[wf_id] = (
                                wf_name, "CANCELLED",
                            )
                            cancelled += 1
                    else:
                        wf_name, status = row[0], row[1]
                        if (
                            wf_name == name_param
                            and status in ("ENQUEUED", "PENDING")
                        ):
                            self.store.statuses[wf_id] = (
                                wf_name, "CANCELLED", *row[2:],
                            )
                            cancelled += 1
                self.rowcount = cancelled
        else:
            raise NotImplementedError(f"unmocked SQL: {sql_upper[:80]}")

    def fetchone(self):
        row, self._fetchone = self._fetchone, None
        return row

    def fetchall(self):
        return self._returning_rows


class _FakeConn:
    def __init__(self, store: "_FakeStore"):
        self.store = store

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return _FakeCursor(self.store)

    def commit(self):
        pass


class _FakeStore:
    """In-memory tables:
      - schedules[schedule_id] = cron
      - statuses[workflow_uuid] = either:
          (name, status)  ← legacy 2-tuple for orphan-feature tests
          (name, status, queue_name, application_version, updated_at)
          ← 5-tuple for stale-PENDING tests; tests that need created_at,
          started_at, or dedupe add them as tuple items 5, 6, and 7.
    The fake _FakeCursor branches on tuple length.
    """

    def __init__(self):
        self.schedules: dict[str, str] = {}
        self.statuses: dict[str, tuple] = {}


def _install_fake(monkeypatch, store: _FakeStore) -> None:
    from okg.substrate.runtime import services as svc
    from okg.substrate.runtime import promotion_fence as fence

    def _fake_connect(*args, **kwargs):
        return _FakeConn(store)

    # Both modules reach libpq through the substrate connection factory, so
    # `db_connect` is the name each one calls; stubbing `psycopg` on them
    # would patch a name neither calls any more and the placeholder DSN below
    # would be resolved for real.
    monkeypatch.setattr(svc, "db_connect", _fake_connect)
    monkeypatch.setattr(fence, "db_connect", _fake_connect)


def test_source_sync_replay_boundary_supersedes_only_incompatible_wakeups(
    monkeypatch,
):
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    store = _FakeStore()
    queue = "okg-okg-workspace-source-sync"
    store.statuses = {
        "old-enqueued": (
            "okg_source_sync", "ENQUEUED", queue, "old-version", 1
        ),
        "old-pending": (
            "okg_source_sync", "PENDING", queue, "old-version", 1
        ),
        "old-delayed": (
            "okg_source_sync", "DELAYED", queue, None, 1
        ),
        "current-pending": (
            "okg_source_sync", "PENDING", queue, "new-version", 1
        ),
        "terminal-old": (
            "okg_source_sync", "SUCCESS", queue, "old-version", 1
        ),
        "other-workflow": (
            "okg_publisher_cycle", "PENDING", queue, "new-version", 1
        ),
        "other-queue": (
            "okg_source_sync", "PENDING", "other-queue", "new-version", 1
        ),
    }
    _install_fake(monkeypatch, store)

    out = supersede_incompatible_source_sync_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        live_version="new-version",
    )

    assert out["cancelled_count"] == 3
    assert {
        workflow_id
        for workflow_id, row in store.statuses.items()
        if row[1] == "CANCELLED"
    } == {"old-enqueued", "old-pending", "old-delayed"}
    assert store.statuses["current-pending"][1] == "PENDING"
    assert store.statuses["terminal-old"][1] == "SUCCESS"
    assert store.statuses["other-workflow"][1] == "PENDING"
    assert store.statuses["other-queue"][1] == "PENDING"


def test_source_sync_replay_boundary_refuses_other_incompatible_workflows(
    monkeypatch,
):
    from okg.substrate.runtime.services import (
        RuntimeCutoverBlocked,
        supersede_incompatible_source_sync_workflows,
    )

    store = _FakeStore()
    # Issue #3519 made an unstarted old publisher cycle replaceable; one that
    # has started (start marker set) still refuses the cutover.
    store.statuses = {
        "old-publisher": (
            "okg_publisher_cycle",
            "PENDING",
            "okg-okg-workspace",
            "old-version",
            1,
            1,
            1234,
        ),
    }
    _install_fake(monkeypatch, store)

    with pytest.raises(RuntimeCutoverBlocked, match="runtime_cutover_blocked"):
        supersede_incompatible_source_sync_workflows(
            dsn="postgres://test",
            deployment="okg-workspace",
            live_version="new-version",
        )
    assert store.statuses["old-publisher"][1] == "PENDING"


def test_cutover_cancels_exact_replaceable_scheduled_state_matrix(
    monkeypatch,
):
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    deployment_queue = "okg-okg-workspace"
    source_sync_queue = "okg-okg-workspace-source-sync"
    store = _FakeStore()
    store.statuses = {
        "source-old": (
            "okg_source_sync", "ENQUEUED", source_sync_queue,
            "old-version", 1, 1, None,
        ),
        "gate-enqueued": (
            "okg_runtime_gates_reevaluate", "ENQUEUED", deployment_queue,
            "old-version", 1, 1, None,
        ),
        "gate-delayed": (
            "okg_runtime_gates_reevaluate", "DELAYED", deployment_queue,
            None, 1, 1, None,
        ),
        "gate-pending-unstarted": (
            "okg_runtime_gates_reevaluate", "PENDING", deployment_queue,
            "old-version", 1, 1, None,
        ),
        "gate-current": (
            "okg_runtime_gates_reevaluate", "ENQUEUED", deployment_queue,
            "new-version", 1, 1, None,
        ),
        "gate-wrong-queue-current": (
            "okg_runtime_gates_reevaluate", "PENDING", "other-queue",
            "new-version", 1, 1, None,
        ),
    }
    _install_fake(monkeypatch, store)

    result = supersede_incompatible_source_sync_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        live_version="new-version",
    )

    assert result["cancelled_count"] == 4
    assert {
        workflow_id
        for workflow_id, row in store.statuses.items()
        if row[1] == "CANCELLED"
    } == {
        "source-old",
        "gate-enqueued",
        "gate-delayed",
        "gate-pending-unstarted",
    }
    assert store.statuses["gate-current"][1] == "ENQUEUED"
    assert store.statuses["gate-wrong-queue-current"][1] == "PENDING"


@pytest.mark.parametrize(
    ("workflow_id", "name", "queue_name", "started_at"),
    [
        ("custom-old", "deployment_custom_workflow", "okg-okg-workspace", None),
        (
            "gate-started",
            "okg_runtime_gates_reevaluate",
            "okg-okg-workspace",
            1234,
        ),
        (
            "gate-null-queue",
            "okg_runtime_gates_reevaluate",
            None,
            None,
        ),
    ],
)
def test_cutover_refuses_custom_and_started_pending_with_typed_diagnostic(
    monkeypatch,
    workflow_id,
    name,
    queue_name,
    started_at,
):
    from okg.substrate.runtime.services import (
        RuntimeCutoverBlocked,
        supersede_incompatible_source_sync_workflows,
    )

    store = _FakeStore()
    store.statuses = {
        workflow_id: (
            name, "PENDING", queue_name, "old-version", 1, 1, started_at,
        ),
    }
    _install_fake(monkeypatch, store)

    with pytest.raises(RuntimeCutoverBlocked) as caught:
        supersede_incompatible_source_sync_workflows(
            dsn="postgres://test",
            deployment="okg-workspace",
            live_version="new-version",
        )

    diagnostic = caught.value.to_dict()
    assert diagnostic["code"] == "runtime_cutover_blocked"
    assert diagnostic["deployment"] == "okg-workspace"
    assert diagnostic["live_application_version"] == "new-version"
    assert diagnostic["blockers"] == [
        {
            "workflow_id": workflow_id,
            "name": name,
            "queue_name": queue_name,
            "status": "PENDING",
            "application_version": "old-version",
            "started_at_epoch_ms": started_at,
            "inspection_command": (
                "uv run okg runtime debug-workflow --deployment "
                f"okg-workspace {workflow_id}"
            ),
            # Issue #1254: the refusal names the scoped remedy, not only the
            # blocking row.
            "remedy_command": (
                "uv run okg runtime cancel-workflow --deployment "
                f"okg-workspace {workflow_id}"
            ),
        }
    ]
    assert store.statuses[workflow_id][1] == "PENDING"


@pytest.mark.postgres
def test_cutover_refuses_null_queue_exact_gate_in_real_postgres(
    clean_runtime: str,
):
    from okg.substrate.runtime.services import (
        RuntimeCutoverBlocked,
        supersede_incompatible_source_sync_workflows,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                    workflow_uuid text PRIMARY KEY,
                    queue_name text,
                    name text,
                    status text,
                    application_version text,
                    started_at_epoch_ms bigint,
                    deduplication_id text
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                    (workflow_uuid, queue_name, name, status,
                     application_version, started_at_epoch_ms)
                VALUES ('gate-null-queue', NULL,
                        'okg_runtime_gates_reevaluate', 'ENQUEUED',
                        'old-version', NULL)
                """
            )

    with pytest.raises(RuntimeCutoverBlocked) as caught:
        supersede_incompatible_source_sync_workflows(
            dsn=clean_runtime,
            deployment="okg-workspace",
            live_version="new-version",
        )

    assert caught.value.blockers[0]["queue_name"] is None
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
            ("gate-null-queue",),
        )
        assert cur.fetchone() == ("ENQUEUED",)


@pytest.mark.postgres
def test_cutover_cleanup_failure_rolls_back_all_replaceable_rows(
    clean_runtime: str,
):
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                    workflow_uuid text PRIMARY KEY,
                    queue_name text,
                    name text,
                    status text,
                    application_version text,
                    started_at_epoch_ms bigint,
                    deduplication_id text
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO dbos.workflow_status
                    (workflow_uuid, queue_name, name, status,
                     application_version, started_at_epoch_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        "source-old", "okg-okg-workspace-source-sync",
                        "okg_source_sync", "ENQUEUED", "old-version", None,
                    ),
                    (
                        "gate-old", "okg-okg-workspace",
                        "okg_runtime_gates_reevaluate", "ENQUEUED",
                        "old-version", None,
                    ),
                ],
            )
            cur.execute(
                """
                CREATE FUNCTION dbos.reject_gate_cancel() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.workflow_uuid = 'gate-old'
                     AND NEW.status = 'CANCELLED' THEN
                    RAISE EXCEPTION 'injected cleanup failure';
                  END IF;
                  RETURN NEW;
                END
                $$
                """
            )
            cur.execute(
                """
                CREATE TRIGGER reject_gate_cancel
                BEFORE UPDATE ON dbos.workflow_status
                FOR EACH ROW EXECUTE FUNCTION dbos.reject_gate_cancel()
                """
            )

    with pytest.raises(Exception, match="injected cleanup failure"):
        supersede_incompatible_source_sync_workflows(
            dsn=clean_runtime,
            deployment="okg-workspace",
            live_version="new-version",
        )

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status FROM dbos.workflow_status "
            "ORDER BY workflow_uuid"
        )
        assert cur.fetchall() == [
            ("gate-old", "ENQUEUED"),
            ("source-old", "ENQUEUED"),
        ]


def test_cutover_policies_name_exact_periodic_workflows():
    """Issues #3514 and #3519: the allowlist names each periodic workflow on
    its own queue, each scheduler wrapper on DBOS's internal queue, and the
    source-poll family by prefix on that queue only. Only source sync may be
    cancelled after it has started."""
    from okg.substrate.runtime.services import (
        ReplaceableCutoverWorkflowPolicy,
        replaceable_cutover_workflow_policies,
    )

    policies = replaceable_cutover_workflow_policies("okg-workspace")

    def unstarted_only(name: str, queue: str) -> ReplaceableCutoverWorkflowPolicy:
        return ReplaceableCutoverWorkflowPolicy(
            workflow_name=name,
            queue_name=queue,
            allow_started_pending=False,
        )

    control = "okg-okg-workspace"
    internal = "_dbos_internal_queue"
    # Source sync stays first: the boundary reports policies[0].
    assert policies == (
        ReplaceableCutoverWorkflowPolicy(
            workflow_name="okg_source_sync",
            queue_name="okg-okg-workspace-source-sync",
            allow_started_pending=True,
        ),
        unstarted_only("okg_runtime_gates_reevaluate", control),
        unstarted_only(
            "okg_declared_attr_index_maintenance",
            "okg-okg-workspace-index-maintenance",
        ),
        unstarted_only("okg_publisher_cycle", control),
        unstarted_only("okg_escalation_timeout_sweep", control),
        unstarted_only("okg_lens_putback_sweep", control),
        unstarted_only("okg_embed_batch", control),
        unstarted_only("okg_publisher_cycle_enqueue", internal),
        unstarted_only("okg_escalation_timeout_sweep_enqueue", internal),
        unstarted_only("okg_lens_putback_sweep_enqueue", internal),
        unstarted_only("okg_embed_batch_enqueue", internal),
        unstarted_only("okg_runtime_gates_reevaluate_enqueue", internal),
        unstarted_only("okg_declared_attr_index_maintenance_enqueue", internal),
        unstarted_only("doctor_workflow", internal),
        ReplaceableCutoverWorkflowPolicy(
            workflow_name="okg_source_poll_",
            queue_name=internal,
            allow_started_pending=False,
            name_is_prefix=True,
        ),
    )
    assert [p for p in policies if p.name_is_prefix] == [policies[-1]]


def _reduced_workflow_status_with(dsn: str, rows: list[tuple]) -> None:
    """Create the reduced ``dbos.workflow_status`` stub holding ``rows``.

    Each row is ``(workflow_uuid, queue_name, name, status,
    application_version, started_at_epoch_ms[, deduplication_id])``.
    """
    rows = [(*row, None) if len(row) == 6 else row for row in rows]
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                    workflow_uuid text PRIMARY KEY,
                    queue_name text,
                    name text,
                    status text,
                    application_version text,
                    started_at_epoch_ms bigint,
                    deduplication_id text
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO dbos.workflow_status
                    (workflow_uuid, queue_name, name, status,
                     application_version, started_at_epoch_ms,
                     deduplication_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )


def _workflow_statuses(dsn: str) -> list[tuple]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status FROM dbos.workflow_status "
            "ORDER BY workflow_uuid"
        )
        return cur.fetchall()


@pytest.mark.postgres
def test_cutover_cancels_old_enqueued_index_maintenance_in_real_postgres(
    clean_runtime: str,
):
    """Issue #3514: an old-version index-maintenance run that is only
    enqueued no longer blocks the upgrade; the boundary cancels it."""
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    _reduced_workflow_status_with(
        clean_runtime,
        [
            (
                "index-old-enqueued", "okg-okg-workspace-index-maintenance",
                "okg_declared_attr_index_maintenance", "ENQUEUED",
                "old-version", None,
            ),
            (
                "index-old-pending-unstarted",
                "okg-okg-workspace-index-maintenance",
                "okg_declared_attr_index_maintenance", "PENDING",
                "old-version", None,
            ),
            (
                "index-old-delayed", "okg-okg-workspace-index-maintenance",
                "okg_declared_attr_index_maintenance", "DELAYED",
                "old-version", None,
            ),
            (
                "index-current", "okg-okg-workspace-index-maintenance",
                "okg_declared_attr_index_maintenance", "ENQUEUED",
                "new-version", None,
            ),
        ],
    )

    out = supersede_incompatible_source_sync_workflows(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="new-version",
    )

    assert out["status"] == "cutover_applied"
    assert out["cancelled_count"] == 3
    assert {row["workflow_id"] for row in out["cancelled_workflows"]} == {
        "index-old-delayed", "index-old-enqueued", "index-old-pending-unstarted",
    }
    assert _workflow_statuses(clean_runtime) == [
        ("index-current", "ENQUEUED"),
        ("index-old-delayed", "CANCELLED"),
        ("index-old-enqueued", "CANCELLED"),
        ("index-old-pending-unstarted", "CANCELLED"),
    ]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("workflow_id", "queue_name", "status", "started_at"),
    [
        # A started run may be inside CREATE INDEX CONCURRENTLY with a live
        # executor: the boundary must not cancel it.
        (
            "index-old-started", "okg-okg-workspace-index-maintenance",
            "PENDING", 1234,
        ),
        # Exact identity: the same workflow name on another queue is not
        # the scheduled maintenance run.
        (
            "index-old-wrong-queue", "okg-okg-workspace",
            "ENQUEUED", None,
        ),
    ],
)
def test_cutover_refuses_started_or_misqueued_index_maintenance_in_real_postgres(
    clean_runtime: str,
    workflow_id: str,
    queue_name: str,
    status: str,
    started_at: int | None,
):
    from okg.substrate.runtime.services import (
        RuntimeCutoverBlocked,
        supersede_incompatible_source_sync_workflows,
    )

    _reduced_workflow_status_with(
        clean_runtime,
        [
            (
                workflow_id, queue_name,
                "okg_declared_attr_index_maintenance", status,
                "old-version", started_at,
            ),
            (
                "source-old", "okg-okg-workspace-source-sync",
                "okg_source_sync", "ENQUEUED", "old-version", None,
            ),
        ],
    )

    with pytest.raises(RuntimeCutoverBlocked) as caught:
        supersede_incompatible_source_sync_workflows(
            dsn=clean_runtime,
            deployment="okg-workspace",
            live_version="new-version",
        )

    assert [row["workflow_id"] for row in caught.value.blockers] == [workflow_id]
    assert caught.value.blockers[0]["queue_name"] == queue_name
    # Refused before any mutation: the replaceable source-sync row stays too.
    assert _workflow_statuses(clean_runtime) == sorted([
        (workflow_id, status),
        ("source-old", "ENQUEUED"),
    ])


# Issue #3519: every periodic row the substrate schedules, on its exact queue.
_PERIODIC_CUTOVER_ROWS = [
    ("okg_publisher_cycle", "okg-okg-workspace"),
    ("okg_escalation_timeout_sweep", "okg-okg-workspace"),
    ("okg_lens_putback_sweep", "okg-okg-workspace"),
    ("okg_embed_batch", "okg-okg-workspace"),
    # Source-poll rows: a cadence group, and an older per-source name.
    ("okg_source_poll_interval_300s", "_dbos_internal_queue"),
    ("okg_source_poll_docs", "_dbos_internal_queue"),
    # Scheduler wrapper rows a killed pod left on DBOS's internal queue.
    ("okg_publisher_cycle_enqueue", "_dbos_internal_queue"),
    ("okg_escalation_timeout_sweep_enqueue", "_dbos_internal_queue"),
    ("okg_lens_putback_sweep_enqueue", "_dbos_internal_queue"),
    ("okg_embed_batch_enqueue", "_dbos_internal_queue"),
    ("okg_runtime_gates_reevaluate_enqueue", "_dbos_internal_queue"),
    ("okg_declared_attr_index_maintenance_enqueue", "_dbos_internal_queue"),
    # The substrate doctor is scheduled directly under its own name.
    ("doctor_workflow", "_dbos_internal_queue"),
]


@pytest.mark.postgres
@pytest.mark.parametrize(("name", "queue_name"), _PERIODIC_CUTOVER_ROWS)
def test_cutover_cancels_old_unstarted_periodic_rows_in_real_postgres(
    clean_runtime: str,
    name: str,
    queue_name: str,
):
    """Issue #3519: an old-version periodic row that has not started no
    longer blocks the upgrade; the boundary cancels it and leaves a
    current-version row of the same workflow alone."""
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    _reduced_workflow_status_with(
        clean_runtime,
        [
            ("old-enqueued", queue_name, name, "ENQUEUED", "old-version", None),
            ("old-delayed", queue_name, name, "DELAYED", "old-version", None),
            (
                "old-pending-unstarted", queue_name, name, "PENDING",
                "old-version", None,
            ),
            ("current", queue_name, name, "ENQUEUED", "new-version", None),
        ],
    )

    out = supersede_incompatible_source_sync_workflows(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="new-version",
    )

    assert out["status"] == "cutover_applied"
    assert out["cancelled_count"] == 3
    assert {row["workflow_id"] for row in out["cancelled_workflows"]} == {
        "old-delayed", "old-enqueued", "old-pending-unstarted",
    }
    assert {row["name"] for row in out["cancelled_workflows"]} == {name}
    assert _workflow_statuses(clean_runtime) == [
        ("current", "ENQUEUED"),
        ("old-delayed", "CANCELLED"),
        ("old-enqueued", "CANCELLED"),
        ("old-pending-unstarted", "CANCELLED"),
    ]


@pytest.mark.postgres
def test_cutover_clears_deduplication_key_of_cancelled_rows_in_real_postgres(
    clean_runtime: str,
):
    """Issue #3519: the boundary clears a cancelled row's deduplication key,
    as DBOS's own cancel does, so the new worker's publisher enqueue in the
    same two-minute bucket is not refused as a terminal collision. A
    current-version row keeps its key."""
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    bucket_key = "publisher:okg-workspace:cycle:14753"
    _reduced_workflow_status_with(
        clean_runtime,
        [
            (
                "publisher-old", "okg-okg-workspace", "okg_publisher_cycle",
                "ENQUEUED", "old-version", None, bucket_key,
            ),
            (
                "publisher-current", "okg-okg-workspace",
                "okg_publisher_cycle", "ENQUEUED", "new-version", None,
                "publisher:okg-workspace:cycle:14754",
            ),
        ],
    )

    out = supersede_incompatible_source_sync_workflows(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="new-version",
    )

    assert out["cancelled_count"] == 1
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status, deduplication_id "
            "FROM dbos.workflow_status ORDER BY workflow_uuid"
        )
        assert cur.fetchall() == [
            (
                "publisher-current", "ENQUEUED",
                "publisher:okg-workspace:cycle:14754",
            ),
            ("publisher-old", "CANCELLED", None),
        ]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("name", "queue_name", "status", "started_at"),
    [
        # A started run may have a live executor: each one still blocks.
        *[
            (name, queue_name, "PENDING", 1234)
            for name, queue_name in _PERIODIC_CUTOVER_ROWS
        ],
        # Exact identity: the right name on the wrong queue still blocks.
        ("okg_publisher_cycle", "_dbos_internal_queue", "ENQUEUED", None),
        ("okg_embed_batch", "okg-okg-workspace-source-sync", "ENQUEUED", None),
        ("okg_publisher_cycle_enqueue", "okg-okg-workspace", "ENQUEUED", None),
        ("okg_source_poll_interval_300s", "okg-okg-workspace", "DELAYED", None),
        # The source-poll prefix is the whole family prefix, nothing shorter.
        ("okg_source_poll", "_dbos_internal_queue", "ENQUEUED", None),
        ("okg_source_pollster", "_dbos_internal_queue", "ENQUEUED", None),
        ("doctor_workflow", "okg-okg-workspace", "ENQUEUED", None),
        # Another workflow's wrapper on the internal queue is not allowlisted.
        ("deployment_custom_enqueue", "_dbos_internal_queue", "ENQUEUED", None),
        # A deployment-author schedule (``@deployment_scheduled``) is not
        # allowlisted either; the operator clears it with cancel-workflow.
        ("okg_ci_resource_monitor", "_dbos_internal_queue", "ENQUEUED", None),
    ],
)
def test_cutover_refuses_started_or_misqueued_periodic_rows_in_real_postgres(
    clean_runtime: str,
    name: str,
    queue_name: str,
    status: str,
    started_at: int | None,
):
    from okg.substrate.runtime.services import (
        RuntimeCutoverBlocked,
        supersede_incompatible_source_sync_workflows,
    )

    _reduced_workflow_status_with(
        clean_runtime,
        [
            ("blocker", queue_name, name, status, "old-version", started_at),
            (
                "source-old", "okg-okg-workspace-source-sync",
                "okg_source_sync", "ENQUEUED", "old-version", None,
            ),
        ],
    )

    with pytest.raises(RuntimeCutoverBlocked) as caught:
        supersede_incompatible_source_sync_workflows(
            dsn=clean_runtime,
            deployment="okg-workspace",
            live_version="new-version",
        )

    assert [row["workflow_id"] for row in caught.value.blockers] == ["blocker"]
    assert caught.value.blockers[0]["name"] == name
    assert caught.value.blockers[0]["queue_name"] == queue_name
    # Refused before any mutation: the replaceable source-sync row stays too.
    assert _workflow_statuses(clean_runtime) == [
        ("blocker", status),
        ("source-old", "ENQUEUED"),
    ]


@pytest.mark.dbos_integration
@pytest.mark.postgres
def test_real_dbos_version_cutover_replaces_gate_schedule_and_becomes_ready(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSClient, DBOSConfig

    from okg.substrate.runtime import gates_reevaluate_schedule as schedule
    from okg.substrate.runtime import gates_reevaluate_workflow as workflow
    from okg.substrate.runtime import partition_queue
    from okg.substrate.runtime.partition_queue import (
        partition_queue_name,
        register_deployment_partition_queue,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher.publisher import Publisher
    from okg.substrate.runtime.services import source_sync_replay_boundary

    deployment = "consistency-fixture"
    queue_name = partition_queue_name(deployment)
    monkeypatch.setenv("OKG_DSN", clean_runtime)

    def config(version: str) -> DBOSConfig:
        return DBOSConfig(
            name="okg-runtime-cutover-gates-test",
            system_database_url=clean_runtime,
            database_url=clean_runtime,
            run_admin_server=False,
            application_version=version,
        )

    def register(version: str) -> None:
        DBOS(config=config(version))
        register_deployment_partition_queue(deployment=deployment)
        workflow.register_gates_reevaluate_workflow(deployment=deployment)
        schedule.register_gates_reevaluate_schedule(
            deployment=deployment,
            dsn=clean_runtime,
        )

    def launch(version: str) -> dict:
        with source_sync_replay_boundary(
            dsn=clean_runtime,
            deployment=deployment,
            live_version=version,
        ) as boundary:
            DBOS.launch()
            return boundary

    DBOS.destroy(destroy_registry=True)
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})
    workflow.reset_decorated_cache_for_tests()
    schedule.reset_decorated_cache_for_tests()
    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

        register("runtime-version-a")
        first_boundary = launch("runtime-version-a")
        assert first_boundary["schema_initialized"] is False

        client = DBOSClient(
            database_url=clean_runtime,
            system_database_url=clean_runtime,
            application_database_url=clean_runtime,
        )
        DBOS.destroy(destroy_registry=True)
        partition_queue._REGISTERED.clear()
        first = client.enqueue(
            {
                "queue_name": queue_name,
                "workflow_name": workflow.GATES_REEVALUATE_WORKFLOW_NAME,
                "app_version": "runtime-version-a",
            }
        )
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, application_version, started_at_epoch_ms
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = %s
                """,
                (first.workflow_id,),
            )
            assert cur.fetchone() == ("ENQUEUED", "runtime-version-a", None)

        workflow.reset_decorated_cache_for_tests()
        schedule.reset_decorated_cache_for_tests()
        register("runtime-version-b")
        second_boundary = launch("runtime-version-b")
        assert second_boundary["cancelled_count"] == 1
        assert second_boundary["cancelled_workflows"][0]["workflow_id"] == (
            first.workflow_id
        )

        schedule.apply_gates_reevaluate_schedule(
            schedule_overrides={
                schedule.GATES_REEVALUATE_SCHEDULE_ID: "* * * * *",
            }
        )
        schedule_enqueue = schedule._DECORATED[
            schedule.GATES_REEVALUATE_SCHEDULE_ID
        ]
        replacement_id = schedule_enqueue(datetime.now(timezone.utc), None)
        assert isinstance(replacement_id, str)
        assert _wait_for_terminal_status(clean_runtime, replacement_id) == "SUCCESS"

        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                (first.workflow_id,),
            )
            assert cur.fetchone() == ("CANCELLED",)
            cur.execute(
                """
                SELECT output IS NOT NULL
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = %s
                """,
                (replacement_id,),
            )
            assert cur.fetchone() == (True,)

        published = Publisher(
            clean_runtime,
            deployment,
            enrichers=[],
            catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
        ).cycle()
        assert published["status"] == "published", published
        generation_id = str(published["generation_id"])
        store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
        try:
            generation = store.get_generation(generation_id)
            generation_status = generation.metadata["status"]
            invariant_summary = generation.metadata["invariant_summary"]
            checkout = store.checkout_generation(generation_id)
            node_count = int(
                checkout.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
            )
            edge_count = int(
                checkout.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
            )
        finally:
            store.close()
        # Scope the "nothing stranded, nothing dead-lettered" check to the two
        # workflows this test created. A whole-table count is not usable here:
        # this test applied a live cron schedule above, and a scheduled tick
        # enqueues the SAME workflow name onto the SAME partition queue, so
        # neither the table nor a (queue_name, name) filter separates the
        # cutover's own rows from a concurrent scheduled enqueue. Issue #1532.
        cutover_workflow_ids = [first.workflow_id, replacement_id]
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT workflow_uuid, name, queue_name, status
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = ANY(%s)
                """,
                (cutover_workflow_ids,),
            )
            cutover_rows = [
                {
                    "workflow_id": row[0],
                    "workflow_name": row[1],
                    "queue_name": row[2],
                    "status": row[3],
                }
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT workflow_uuid, name, queue_name, status
                  FROM dbos.workflow_status
                 WHERE application_version = %s
                   AND status IN ('ENQUEUED', 'DELAYED', 'PENDING')
                """,
                ("runtime-version-a",),
            )
            stranded_pre_cutover_rows = [
                {
                    "workflow_id": row[0],
                    "workflow_name": row[1],
                    "queue_name": row[2],
                    "status": row[3],
                }
                for row in cur.fetchall()
            ]
        # Non-vacuity: both rows must be PRESENT, and on this deployment's
        # partition queue under the gates-reevaluate workflow name. A missing,
        # truncated, or mis-scoped table therefore cannot satisfy the counts
        # below by counting nothing. This check deliberately does NOT pin the
        # statuses: the queue and dead-letter assertions below are what must
        # fail on a stranded or dead-lettered workflow, and the exact terminal
        # states are asserted after them.
        assert {row["workflow_id"] for row in cutover_rows} == {
            first.workflow_id,
            replacement_id,
        }, f"cutover workflow rows missing from dbos.workflow_status: {cutover_rows}"
        assert {
            (row["queue_name"], row["workflow_name"]) for row in cutover_rows
        } == {
            (queue_name, workflow.GATES_REEVALUATE_WORKFLOW_NAME)
        }, f"cutover rows are not on the test queue/workflow: {cutover_rows}"
        queued_rows = [
            row
            for row in cutover_rows
            if row["status"] in {"ENQUEUED", "DELAYED", "PENDING"}
        ]
        dlq_rows = [
            row
            for row in cutover_rows
            if row["status"] in {"ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"}
        ]
        queue_depth = len(queued_rows)
        dlq_count = len(dlq_rows)
        assert generation_status == "published"
        assert queue_depth == 0, (
            f"cutover left queued workflows: {queued_rows}"
        )
        assert dlq_count == 0, (
            f"cutover dead-lettered workflows: {dlq_rows}"
        )
        assert {row["workflow_id"]: row["status"] for row in cutover_rows} == {
            first.workflow_id: "CANCELLED",
            replacement_id: "SUCCESS",
        }, f"unexpected cutover workflow end states: {cutover_rows}"
        # The old whole-table check also reached work stranded by the cutover
        # under any name or queue. Keep that reach deterministically by
        # restricting it to the PRE-cutover application version: the live
        # schedule this test applied runs under runtime-version-b, so a
        # scheduled tick can never land in this set.
        assert stranded_pre_cutover_rows == [], (
            "cutover left pre-cutover-version workflows queued: "
            f"{stranded_pre_cutover_rows}"
        )
        proof = {
            "generation_id": generation_id,
            "generation_status": generation_status,
            "invariant_summary": invariant_summary,
            "node_count": node_count,
            "edge_count": edge_count,
            "nodes_added": published["nodes_added"],
            "nodes_retracted": published["nodes_retracted"],
            "edges_added": published["edges_added"],
            "edges_retracted": published["edges_retracted"],
            "queue_depth": queue_depth,
            "dlq_count": dlq_count,
            "cutover_workflow_rows": cutover_rows,
            "stranded_pre_cutover_rows": stranded_pre_cutover_rows,
            "cutover_cancelled": second_boundary["cancelled_count"],
            "replacement_workflow_id": replacement_id,
        }
        record_property("runtime_cutover_proof", json.dumps(proof, default=str))
        print("RUNTIME_CUTOVER_PROOF=" + json.dumps(proof, default=str))
    finally:
        try:
            DBOS.destroy(destroy_registry=True)
        except Exception:  # noqa: BLE001
            pass
        partition_queue._REGISTERED.clear()
        workflow.reset_decorated_cache_for_tests()
        schedule.reset_decorated_cache_for_tests()


def _wait_for_terminal_status(
    dsn: str,
    workflow_id: str,
    *,
    timeout_seconds: float = 20.0,
) -> str | None:
    deadline = time.monotonic() + timeout_seconds
    terminal = {
        "SUCCESS",
        "ERROR",
        "CANCELLED",
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    }
    status: str | None = None
    while time.monotonic() < deadline:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                (workflow_id,),
            )
            row = cur.fetchone()
        status = str(row[0]) if row else None
        if status in terminal:
            return status
        time.sleep(0.1)
    return status


@pytest.mark.postgres
def test_source_sync_replay_boundary_uses_real_postgres_rows(
    clean_runtime: str,
):
    from okg.substrate.runtime.services import (
        supersede_incompatible_source_sync_workflows,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                    workflow_uuid text PRIMARY KEY,
                    queue_name text,
                    name text,
                    status text,
                    application_version text,
                    started_at_epoch_ms bigint,
                    deduplication_id text
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO dbos.workflow_status
                    (workflow_uuid, queue_name, name, status, application_version)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    ("old-enqueued", "okg-okg-workspace-source-sync", "okg_source_sync", "ENQUEUED", "old-version"),
                    ("old-pending", "okg-okg-workspace-source-sync", "okg_source_sync", "PENDING", "old-version"),
                    ("old-delayed", "okg-okg-workspace-source-sync", "okg_source_sync", "DELAYED", None),
                    ("current-pending", "okg-okg-workspace-source-sync", "okg_source_sync", "PENDING", "new-version"),
                    ("terminal-old", "okg-okg-workspace-source-sync", "okg_source_sync", "SUCCESS", "old-version"),
                    ("other-workflow", "okg-okg-workspace-source-sync", "okg_publisher_cycle", "PENDING", "new-version"),
                    ("other-queue", "other-queue", "okg_source_sync", "PENDING", "new-version"),
                ],
            )

    out = supersede_incompatible_source_sync_workflows(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="new-version",
    )

    assert out["cancelled_count"] == 3
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_uuid, status
              FROM dbos.workflow_status
             ORDER BY workflow_uuid
            """
        )
        assert cur.fetchall() == [
            ("current-pending", "PENDING"),
            ("old-delayed", "CANCELLED"),
            ("old-enqueued", "CANCELLED"),
            ("old-pending", "CANCELLED"),
            ("other-queue", "PENDING"),
            ("other-workflow", "PENDING"),
            ("terminal-old", "SUCCESS"),
        ]


@pytest.mark.postgres
def test_source_sync_replay_boundary_holds_table_lock_until_launch_returns(
    clean_runtime: str,
):
    """A direct DBOS insert cannot slip between cancellation and launch."""
    from okg.substrate.runtime.services import source_sync_replay_boundary

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                    workflow_uuid text PRIMARY KEY,
                    queue_name text,
                    name text,
                    status text,
                    application_version text,
                    started_at_epoch_ms bigint,
                    deduplication_id text
                )
                """
            )

    insert_started = threading.Event()
    insert_finished = threading.Event()
    errors: list[BaseException] = []

    def insert_directly() -> None:
        try:
            with psycopg.connect(clean_runtime, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    insert_started.set()
                    cur.execute(
                        """
                        INSERT INTO dbos.workflow_status
                            (workflow_uuid, queue_name, name, status,
                             application_version)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            "inserted-during-launch",
                            "okg-okg-workspace-source-sync",
                            "okg_source_sync",
                            "ENQUEUED",
                            "okg-runtime-source-sync-v2",
                        ),
                    )
                conn.commit()
        except Exception as exc:  # pragma: no cover - assertion below
            errors.append(exc)
        finally:
            insert_finished.set()

    thread = threading.Thread(target=insert_directly, daemon=True)
    with source_sync_replay_boundary(
        dsn=clean_runtime,
        deployment="okg-workspace",
        live_version="okg-runtime-source-sync-v2",
    ):
        thread.start()
        assert insert_started.wait(timeout=5)
        assert not insert_finished.wait(timeout=0.25)

    assert insert_finished.wait(timeout=5)
    thread.join(timeout=1)
    assert not errors
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM dbos.workflow_status "
            "WHERE workflow_uuid = %s",
            ("inserted-during-launch",),
        )
        assert cur.fetchone() == ("ENQUEUED",)


@pytest.mark.postgres
def test_source_sync_replay_boundary_rolls_back_when_launch_fails(
    clean_runtime: str,
):
    """A failed replacement launch does not commit the cancellation sweep."""
    from okg.substrate.runtime.services import source_sync_replay_boundary

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                    workflow_uuid text PRIMARY KEY,
                    queue_name text,
                    name text,
                    status text,
                    application_version text,
                    started_at_epoch_ms bigint,
                    deduplication_id text
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                    (workflow_uuid, queue_name, name, status,
                     application_version)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    "old-before-failed-launch",
                    "okg-okg-workspace-source-sync",
                    "okg_source_sync",
                    "PENDING",
                    "okg-runtime-source-sync-v1",
                ),
            )

    def fail_launch() -> None:
        raise RuntimeError("launch failed")

    with pytest.raises(RuntimeError, match="launch failed"):
        with source_sync_replay_boundary(
            dsn=clean_runtime,
            deployment="okg-workspace",
            live_version="okg-runtime-source-sync-v2",
        ) as boundary:
            assert boundary["cancelled_count"] == 1
            fail_launch()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, application_version "
            "FROM dbos.workflow_status WHERE workflow_uuid = %s",
            ("old-before-failed-launch",),
        )
        assert cur.fetchone() == ("PENDING", "okg-runtime-source-sync-v1")


def test_cleanup_returns_empty_dict_when_nothing_disabled(monkeypatch):
    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )
    store = _FakeStore()
    _install_fake(monkeypatch, store)
    out = cleanup_disabled_feature_schedules(
        dsn="postgres://test", disabled={},
    )
    assert out == {}


def test_cleanup_deletes_stale_schedule_row(monkeypatch):
    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )
    store = _FakeStore()
    store.schedules["okg_embed_batch"] = "*/5 * * * *"
    _install_fake(monkeypatch, store)

    out = cleanup_disabled_feature_schedules(
        dsn="postgres://test",
        disabled={
            "okg_embed_batch": [
                "okg_embed_batch", "okg_embed_batch_enqueue",
            ],
        },
    )
    assert out["okg_embed_batch"]["schedule_deleted"] == 1
    assert out["okg_embed_batch"]["workflows_cancelled"] == 0
    assert "okg_embed_batch" not in store.schedules


def test_cleanup_cancels_orphan_enqueued_and_pending(monkeypatch):
    """Workflows in ENQUEUED or PENDING get cancelled. SUCCESS /
    ERROR / CANCELLED rows are NOT touched."""
    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )
    store = _FakeStore()
    store.statuses.update({
        "wf-1": ("okg_lens_putback_sweep", "ENQUEUED"),
        "wf-2": ("okg_lens_putback_sweep", "PENDING"),
        "wf-3": ("okg_lens_putback_sweep", "SUCCESS"),
        "wf-4": ("okg_lens_putback_sweep_enqueue", "ENQUEUED"),
        "wf-5": ("okg_lens_putback_sweep_enqueue", "ERROR"),
        "wf-6": ("other_workflow", "ENQUEUED"),
    })
    _install_fake(monkeypatch, store)

    out = cleanup_disabled_feature_schedules(
        dsn="postgres://test",
        disabled={
            "okg_lens_putback_sweep": [
                "okg_lens_putback_sweep",
                "okg_lens_putback_sweep_enqueue",
            ],
        },
    )
    assert out["okg_lens_putback_sweep"]["workflows_cancelled"] == 3

    # Row-by-row check.
    assert store.statuses == {
        "wf-1": ("okg_lens_putback_sweep", "CANCELLED"),
        "wf-2": ("okg_lens_putback_sweep", "CANCELLED"),
        "wf-3": ("okg_lens_putback_sweep", "SUCCESS"),
        "wf-4": ("okg_lens_putback_sweep_enqueue", "CANCELLED"),
        "wf-5": ("okg_lens_putback_sweep_enqueue", "ERROR"),
        "wf-6": ("other_workflow", "ENQUEUED"),
    }


def test_cleanup_handles_multiple_features(monkeypatch):
    """All three disabled features cleaned in one call."""
    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )
    store = _FakeStore()
    store.schedules.update({
        "okg_embed_batch": "*/5 * * * *",
        "okg_lens_putback_sweep": "*/5 * * * *",
        "okg_escalation_timeout_sweep": "*/1 * * * *",
    })
    store.statuses.update({
        "e-1": ("okg_embed_batch", "ENQUEUED"),
        "l-1": ("okg_lens_putback_sweep", "ENQUEUED"),
        "l-2": ("okg_lens_putback_sweep", "ENQUEUED"),
        "esc-1": ("okg_escalation_timeout_sweep", "PENDING"),
    })
    _install_fake(monkeypatch, store)

    out = cleanup_disabled_feature_schedules(
        dsn="postgres://test",
        disabled={
            "okg_embed_batch": [
                "okg_embed_batch", "okg_embed_batch_enqueue",
            ],
            "okg_lens_putback_sweep": [
                "okg_lens_putback_sweep",
                "okg_lens_putback_sweep_enqueue",
            ],
            "okg_escalation_timeout_sweep": [
                "okg_escalation_timeout_sweep",
                "okg_escalation_timeout_sweep_enqueue",
            ],
        },
    )
    assert out["okg_embed_batch"]["schedule_deleted"] == 1
    assert out["okg_embed_batch"]["workflows_cancelled"] == 1
    assert out["okg_lens_putback_sweep"]["schedule_deleted"] == 1
    assert out["okg_lens_putback_sweep"]["workflows_cancelled"] == 2
    assert (
        out["okg_escalation_timeout_sweep"]["schedule_deleted"] == 1
    )
    assert (
        out["okg_escalation_timeout_sweep"]["workflows_cancelled"] == 1
    )


def test_cleanup_is_idempotent(monkeypatch):
    """Calling it twice produces empty deltas the second time."""
    from okg.substrate.runtime.services import (
        cleanup_disabled_feature_schedules,
    )
    store = _FakeStore()
    store.schedules["okg_embed_batch"] = "*/5 * * * *"
    store.statuses["e1"] = ("okg_embed_batch", "ENQUEUED")
    _install_fake(monkeypatch, store)

    disabled = {
        "okg_embed_batch": [
            "okg_embed_batch", "okg_embed_batch_enqueue",
        ],
    }
    first = cleanup_disabled_feature_schedules(
        dsn="postgres://test", disabled=disabled,
    )
    second = cleanup_disabled_feature_schedules(
        dsn="postgres://test", disabled=disabled,
    )
    assert first["okg_embed_batch"]["schedule_deleted"] == 1
    assert first["okg_embed_batch"]["workflows_cancelled"] == 1
    assert second["okg_embed_batch"]["schedule_deleted"] == 0
    assert second["okg_embed_batch"]["workflows_cancelled"] == 0


# ---------------------------------------------------------------------------
# cleanup_stale_pending_workflows
# ---------------------------------------------------------------------------


def test_stale_pending_cancels_old_pending_on_matching_queue(
    monkeypatch,
):
    """A PENDING older than the grace period is cancelled; SUCCESS /
    ENQUEUED rows are not touched."""
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_ms = now_ms - 30 * 60 * 1000      # 30 min ago — stale
    fresh_ms = now_ms - 60 * 1000          # 60s ago — fresh
    store.statuses = {
        # (name, status, queue_name, application_version, updated_at)
        "zombie-1": ("okg_source_sync", "PENDING",
                     "okg-okg-workspace", "old-ver", old_ms),
        "zombie-2": ("okg_publisher_cycle", "PENDING",
                     "okg-okg-workspace", "old-ver", old_ms),
        "alive": ("okg_source_sync", "PENDING",
                  "okg-okg-workspace", "new-ver", fresh_ms),
        "other-deployment": ("okg_source_sync", "PENDING",
                             "okg-other", "old-ver", old_ms),
        "already-success": ("okg_source_sync", "SUCCESS",
                            "okg-okg-workspace", "old-ver", old_ms),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test", deployment="okg-workspace",
        stale_after_seconds=600,  # 10 min
    )
    assert out["cancelled_count"] == 2
    assert out["stale_after_seconds"] == 600
    sample_uuids = {s["workflow_uuid"] for s in out["cancelled_sample"]}
    assert sample_uuids == {"zombie-1", "zombie-2"}

    # Row-state check: zombies cancelled, alive untouched, other
    # deployment untouched, SUCCESS untouched.
    statuses = {wf_id: row[1] for wf_id, row in store.statuses.items()}
    assert statuses == {
        "zombie-1": "CANCELLED",
        "zombie-2": "CANCELLED",
        "alive": "PENDING",
        "other-deployment": "PENDING",
        "already-success": "SUCCESS",
    }


def test_stale_pending_cancels_old_source_sync_lane_pending(
    monkeypatch,
):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    old_ms = int(_time.time() * 1000) - 30 * 60 * 1000
    store.statuses = {
        "source-sync-lane-zombie": (
            "okg_source_sync",
            "PENDING",
            "okg-okg-workspace-source-sync",
            "v",
            old_ms,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
    )

    assert out["cancelled_count"] == 1
    assert store.statuses["source-sync-lane-zombie"][1] == "CANCELLED"


def test_stale_pending_returns_zero_when_nothing_stale(monkeypatch):
    """No stale PENDINGs → cancelled_count is 0, sample is empty."""
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    fresh_ms = int(_time.time() * 1000) - 60 * 1000  # 60s ago
    store.statuses = {
        "fresh-1": ("okg_source_sync", "PENDING",
                    "okg-okg-workspace", "v", fresh_ms),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test", deployment="okg-workspace",
        stale_after_seconds=600,
    )
    assert out["cancelled_count"] == 0
    assert out["cancelled_sample"] == []
    # Fresh PENDING untouched.
    assert store.statuses["fresh-1"][1] == "PENDING"


def test_stale_pending_preserves_old_recovered_pending_with_fresh_update(
    monkeypatch,
):
    """A recovered workflow that has a fresh lifecycle signal is active.

    It may have waited in the queue for a long time, but maintenance must
    not cancel it immediately after DBOS starts recovery.
    """
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_ms = now_ms - 3 * 60 * 60 * 1000
    fresh_ms = now_ms - 5 * 1000
    store.statuses = {
        "recovered-zombie": (
            "okg_source_sync",
            "PENDING",
            "okg-okg-workspace",
            "new-ver",
            fresh_ms,
            old_ms,
            old_ms,
            "sync:zombie",
            1,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
    )

    assert out["cancelled_count"] == 0
    assert store.statuses["recovered-zombie"][1] == "PENDING"
    assert out["cancelled_sample"] == []


def test_stale_pending_cancels_recovered_replaceable_wakeup_after_short_grace(
    monkeypatch,
):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_enough_ms = now_ms - 6 * 60 * 1000
    fresh_ms = now_ms - 5 * 1000
    store.statuses = {
        "publisher": (
            "okg_publisher_cycle",
            "PENDING",
            "okg-okg-workspace",
            "v",
            old_enough_ms,
            old_enough_ms,
            None,
            "publisher:demo",
            1,
        ),
        "custom": (
            "deployment_custom_workflow",
            "PENDING",
            "okg-okg-workspace",
            "v",
            fresh_ms,
            old_enough_ms,
            fresh_ms,
            "custom:demo",
            1,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
        recovered_wakeup_stale_after_seconds=300,
    )

    assert out["cancelled_count"] == 1
    assert store.statuses["publisher"][1] == "CANCELLED"
    assert store.statuses["custom"][1] == "PENDING"


def test_stale_pending_preserves_started_recovered_until_full_stale_window(
    monkeypatch,
):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_created_ms = now_ms - 30 * 60 * 1000
    started_ms = now_ms - 5 * 60 * 1000 - 5_000
    store.statuses = {
        "source-sync": (
            "okg_source_sync",
            "PENDING",
            "okg-okg-workspace-source-sync",
            "v",
            started_ms,
            old_created_ms,
            started_ms,
            "sync:demo",
            1,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
        recovered_wakeup_stale_after_seconds=300,
    )

    assert out["cancelled_count"] == 0
    assert store.statuses["source-sync"][1] == "PENDING"


def test_stale_pending_preserves_started_running_post_launch_policy(
    monkeypatch,
):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_created_ms = now_ms - 30 * 60 * 1000
    old_started_ms = now_ms - 12 * 60 * 1000
    store.statuses = {
        "source-sync": (
            "okg_source_sync",
            "PENDING",
            "okg-okg-workspace-source-sync",
            "v",
            old_started_ms,
            old_created_ms,
            old_started_ms,
            "sync:demo",
            1,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
        recovered_wakeup_stale_after_seconds=300,
        cancel_started_pending=False,
    )

    assert out["cancelled_count"] == 0
    assert out["cancel_started_pending"] is False
    assert store.statuses["source-sync"][1] == "PENDING"


def test_stale_enqueued_cancels_replaceable_wakeups(monkeypatch):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_enqueued_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_ms = now_ms - 30 * 60 * 1000
    fresh_ms = now_ms - 60 * 1000
    store.statuses = {
        "source-sync": (
            "okg_source_sync", "ENQUEUED", "okg-okg-workspace",
            "v", old_ms, old_ms, None, "sync:demo",
        ),
        "source-poll": (
            "okg_source_poll_pact", "ENQUEUED", "okg-okg-workspace",
            "v", old_ms, old_ms, None, "source-poll:demo",
        ),
        "publisher": (
            "okg_publisher_cycle", "ENQUEUED", "okg-okg-workspace",
            "v", old_ms, old_ms, None, "publisher:demo",
        ),
        "fresh-source-sync": (
            "okg_source_sync", "ENQUEUED", "okg-okg-workspace",
            "v", fresh_ms, fresh_ms, None, "sync:fresh",
        ),
        "pending-source-sync": (
            "okg_source_sync", "PENDING", "okg-okg-workspace",
            "v", old_ms, old_ms, None, "sync:pending",
        ),
        "custom-workflow": (
            "deployment_custom_workflow", "ENQUEUED", "okg-okg-workspace",
            "v", old_ms, old_ms, None, "custom:old",
        ),
        "other-deployment": (
            "okg_source_sync", "ENQUEUED", "okg-other",
            "v", old_ms, old_ms, None, "sync:other",
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_enqueued_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
    )

    assert out["cancelled_count"] == 3
    assert {
        row["workflow_uuid"] for row in out["cancelled_sample"]
    } == {"source-sync", "source-poll", "publisher"}
    statuses = {wf_id: row[1] for wf_id, row in store.statuses.items()}
    assert statuses == {
        "source-sync": "CANCELLED",
        "source-poll": "CANCELLED",
        "publisher": "CANCELLED",
        "fresh-source-sync": "ENQUEUED",
        "pending-source-sync": "PENDING",
        "custom-workflow": "ENQUEUED",
        "other-deployment": "ENQUEUED",
    }


def test_stale_enqueued_cancels_source_sync_lane_wakeups(monkeypatch):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_enqueued_workflows,
    )
    store = _FakeStore()
    old_ms = int(_time.time() * 1000) - 30 * 60 * 1000
    store.statuses = {
        "source-sync-lane": (
            "okg_source_sync", "ENQUEUED",
            "okg-okg-workspace-source-sync",
            "v", old_ms, old_ms, None, "sync:demo",
        ),
        "custom-source-sync-lane": (
            "custom_workflow", "ENQUEUED",
            "okg-okg-workspace-source-sync",
            "v", old_ms, old_ms, None, "custom:demo",
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_enqueued_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
    )

    assert out["cancelled_count"] == 1
    assert store.statuses["source-sync-lane"][1] == "CANCELLED"
    assert store.statuses["custom-source-sync-lane"][1] == "ENQUEUED"


def test_stale_enqueued_preserves_priority_source_sync_work(monkeypatch):
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_enqueued_workflows,
    )
    store = _FakeStore()
    old_ms = int(_time.time() * 1000) - 30 * 60 * 1000
    store.statuses = {
        "readiness-recovery": (
            "okg_source_sync", "ENQUEUED",
            "okg-okg-workspace-source-sync",
            "v", old_ms, old_ms, None,
            "sync:okg-workspace:poll:scope_complete:_poll_batch:none:1",
            20,
        ),
        "operator-request": (
            "okg_source_sync", "ENQUEUED",
            "okg-okg-workspace-source-sync",
            "v", old_ms, old_ms, None,
            None,
            10,
        ),
        "routine-wakeup": (
            "okg_source_sync", "ENQUEUED",
            "okg-okg-workspace-source-sync",
            "v", old_ms, old_ms, None,
            "sync:okg-workspace:watch:scope_incomplete:docs:none:1",
            100,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_enqueued_workflows(
        dsn="postgres://test",
        deployment="okg-workspace",
        stale_after_seconds=600,
    )

    assert out["cancelled_count"] == 1
    assert store.statuses["readiness-recovery"][1] == "ENQUEUED"
    assert store.statuses["operator-request"][1] == "ENQUEUED"
    assert store.statuses["routine-wakeup"][1] == "CANCELLED"


def test_source_sync_maintenance_runs_stale_enqueued_cleanup(monkeypatch):
    from okg.substrate.runtime import services

    calls: list[dict[str, object]] = []

    def cleanup_stale_pending_workflows(**kwargs):
        calls.append({"fn": "pending", **dict(kwargs)})
        return {"cancelled_count": 1}

    def cleanup_stale_enqueued_workflows(**kwargs):
        calls.append({"fn": "enqueued", **dict(kwargs)})
        return {"cancelled_count": 2}

    # Issue #1257: maintenance also releases slots held by workflows past the
    # deadline DBOS recorded for them.
    def cleanup_deadline_expired_pending_workflows(**kwargs):
        calls.append({"fn": "deadline_expired", **dict(kwargs)})
        return {"cancelled_count": 4}

    from okg.substrate.runtime import workflow_cancel

    monkeypatch.setattr(
        workflow_cancel,
        "cleanup_deadline_expired_pending_workflows",
        cleanup_deadline_expired_pending_workflows,
    )
    monkeypatch.setattr(
        services,
        "cleanup_stale_pending_workflows",
        cleanup_stale_pending_workflows,
    )
    monkeypatch.setattr(
        services,
        "cleanup_stale_enqueued_workflows",
        cleanup_stale_enqueued_workflows,
    )
    services._reset_state("okg-workspace")

    report = services._run_maintenance_once(
        deployment="okg-workspace",
        dsn="postgres://test",
        stale_after_seconds=300,
    )

    assert report == {
        "cancelled_count": 7,
        "pending": {"cancelled_count": 1},
        "deadline_expired": {"cancelled_count": 4},
        "enqueued": {"cancelled_count": 2},
    }
    assert calls == [
        {
            "fn": "pending",
            "deployment": "okg-workspace",
            "dsn": "postgres://test",
            "cancel_started_pending": False,
        },
        {
            "fn": "deadline_expired",
            "deployment": "okg-workspace",
            "dsn": "postgres://test",
        },
        {
            "fn": "enqueued",
            "deployment": "okg-workspace",
            "dsn": "postgres://test",
            "stale_after_seconds": 300,
        }
    ]
    state = services.get_services_state().maintenance
    assert state.cleanup_runs == 1
    assert state.cancelled_count == 7
    assert state.last_error is None


def test_stale_pending_caps_sample_at_5_uuids(monkeypatch):
    """If many zombies, the operator log shouldn't print all of
    them — sample caps at 5."""
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_pending_workflows,
    )
    store = _FakeStore()
    old_ms = int(_time.time() * 1000) - 30 * 60 * 1000
    for i in range(20):
        store.statuses[f"zombie-{i}"] = (
            "okg_source_sync", "PENDING",
            "okg-okg-workspace", "v", old_ms,
        )
    _install_fake(monkeypatch, store)

    out = cleanup_stale_pending_workflows(
        dsn="postgres://test", deployment="okg-workspace",
        stale_after_seconds=600,
    )
    assert out["cancelled_count"] == 20
    assert len(out["cancelled_sample"]) == 5


# ---------------------------------------------------------------------------
# cleanup_stale_internal_pending_workflows
# ---------------------------------------------------------------------------


def test_stale_internal_pending_cancels_old_internal_queue_rows(
    monkeypatch,
):
    """Old DBOS scheduler wrappers on the internal queue are safe to
    cancel; fresh rows and deployment-queue rows are not touched."""
    import time as _time
    from okg.substrate.runtime.services import (
        cleanup_stale_internal_pending_workflows,
    )
    store = _FakeStore()
    now_ms = int(_time.time() * 1000)
    old_ms = now_ms - 30 * 60 * 1000
    fresh_ms = now_ms - 60 * 1000
    store.statuses = {
        "old-internal": (
            "okg_source_poll_pact_enqueue",
            "PENDING",
            "_dbos_internal_queue",
            "old-ver",
            old_ms,
        ),
        "fresh-internal": (
            "doctor_workflow",
            "PENDING",
            "_dbos_internal_queue",
            "new-ver",
            fresh_ms,
        ),
        "old-deployment": (
            "okg_source_sync",
            "PENDING",
            "okg-okg-workspace",
            "old-ver",
            old_ms,
        ),
        "already-done": (
            "okg_publisher_cycle_enqueue",
            "SUCCESS",
            "_dbos_internal_queue",
            "old-ver",
            old_ms,
        ),
    }
    _install_fake(monkeypatch, store)

    out = cleanup_stale_internal_pending_workflows(
        dsn="postgres://test",
        stale_after_seconds=600,
    )

    assert out["cancelled_count"] == 1
    assert out["cancelled_sample"][0]["workflow_uuid"] == "old-internal"
    statuses = {wf_id: row[1] for wf_id, row in store.statuses.items()}
    assert statuses == {
        "old-internal": "CANCELLED",
        "fresh-internal": "PENDING",
        "old-deployment": "PENDING",
        "already-done": "SUCCESS",
    }
