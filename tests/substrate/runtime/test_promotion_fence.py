from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError
import importlib.util
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.errors import DeploymentEnvironmentInvalid
from okg.substrate.runtime.promotion_fence import (
    acquire_promotion_fence,
    assert_deployment_admitted,
    deployment_admission_lock,
    mark_promotion_recovery_blocked,
    promotion_enqueue_permit,
    promotion_fence_status,
    release_promotion_fence,
)


pytestmark = pytest.mark.postgres


DEPLOYMENT = "promotion-test"
PROMOTION = "promotion-1"


@pytest.fixture
def promotion_database(okg_workspace_isolated_migrated: str) -> str:
    with psycopg.connect(okg_workspace_isolated_migrated) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_schedules (
              schedule_id TEXT PRIMARY KEY,
              schedule_name TEXT NOT NULL UNIQUE,
              workflow_name TEXT NOT NULL,
              workflow_class_name TEXT,
              schedule TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'ACTIVE',
              context TEXT NOT NULL,
              last_fired_at TEXT,
              automatic_backfill BOOLEAN NOT NULL DEFAULT false,
              cron_timezone TEXT,
              queue_name TEXT
            )
            """
        )
        cur.execute(
            "DELETE FROM okg.deployment_promotion_fences WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        cur.execute("DELETE FROM dbos.workflow_schedules")
    return okg_workspace_isolated_migrated


def test_acquire_fence_claims_owner_and_removes_schedules(
    promotion_database: str,
) -> None:
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES ('schedule-1', 'schedule-1', 'workflow-1', '* * * * *', '{}')
            """
        )

    receipt = acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
        metadata={"artifact_id": "sha256:candidate"},
    )

    assert receipt["promotion_id"] == PROMOTION
    assert receipt["status"] == "fenced"
    assert receipt["schedules_removed"] == 1
    assert receipt["schedules_removed_now"] == 1
    assert receipt["resumed"] is False
    assert receipt["receipt_digest"].startswith("sha256:")
    status = promotion_fence_status(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
    )
    assert status is not None
    assert status["metadata"] == {"artifact_id": "sha256:candidate"}


def test_fence_acquisition_waits_for_inflight_enqueue_permit(
    promotion_database: str,
) -> None:
    permit_entered = Event()
    release_permit = Event()

    def hold_permit() -> None:
        with promotion_enqueue_permit(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
        ):
            permit_entered.set()
            assert release_permit.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=2) as pool:
        permit = pool.submit(hold_permit)
        assert permit_entered.wait(timeout=5)
        acquire = pool.submit(
            acquire_promotion_fence,
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            promotion_id=PROMOTION,
        )
        with pytest.raises(TimeoutError):
            acquire.result(timeout=0.2)
        release_permit.set()
        permit.result(timeout=5)
        receipt = acquire.result(timeout=5)

    assert receipt["promotion_id"] == PROMOTION


def test_fence_rejects_non_owner_but_allows_owner_and_persists_blocker(
    promotion_database: str,
) -> None:
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    with pytest.raises(DeploymentEnvironmentInvalid, match="enqueues are fenced"):
        with promotion_enqueue_permit(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
        ):
            pass
    with promotion_enqueue_permit(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    ):
        pass

    blocked = mark_promotion_recovery_blocked(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
        blocker={"step": "restore_backup", "error": "disk full"},
    )
    assert blocked["status"] == "recovery_blocked"
    assert blocked["blocker"]["step"] == "restore_backup"


def test_only_owner_can_release_fence(promotion_database: str) -> None:
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    with pytest.raises(DeploymentEnvironmentInvalid, match="not owned"):
        release_promotion_fence(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            promotion_id="promotion-2",
        )
    receipt = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    assert receipt["released"] is True
    assert receipt["schedules_restored"] == 0
    status = promotion_fence_status(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
    )
    assert status is not None
    assert status["status"] == "released"
    replay = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    assert replay == receipt


def test_release_restores_exact_schedule_snapshot(promotion_database: str) -> None:
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, status, context,
              last_fired_at, automatic_backfill, cron_timezone, queue_name
            ) VALUES (
              'schedule-restore', 'schedule-restore', 'workflow-restore',
              '*/5 * * * *', 'ACTIVE', '{"source":"test"}',
              '2026-07-26T12:34:56Z', true, 'America/Chicago', 'okg-promotion-test'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    receipt = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    assert receipt["schedules_restored"] == 1
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT schedule_name, workflow_name, schedule, status, context,
                   last_fired_at, automatic_backfill, cron_timezone, queue_name
              FROM dbos.workflow_schedules
             WHERE schedule_id = 'schedule-restore'
            """
        )
        assert cur.fetchone() == (
            "schedule-restore",
            "workflow-restore",
            "*/5 * * * *",
            "ACTIVE",
            '{"source":"test"}',
            "2026-07-26T12:34:56Z",
            True,
            "America/Chicago",
            "okg-promotion-test",
        )


def test_pre_migration_drain_abort_restores_and_removes_legacy_fence(
    promotion_database: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'legacy-before', 'legacy-before', 'workflow-before',
              '*/5 * * * *', '{"source":"before"}'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence._released_status_supported",
        lambda cur: False,
    )

    receipt = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
        schedule_disposition="restore",
    )

    assert receipt["released"] is True
    assert receipt["legacy_schema"] is True
    assert receipt["durable_release_receipt"] is False
    assert promotion_fence_status(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
    ) is None
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_name FROM dbos.workflow_schedules "
            "WHERE schedule_id = 'legacy-before'"
        )
        assert cur.fetchone()[0] == "workflow-before"


def test_release_replaces_schedule_reregistered_under_fence(
    promotion_database: str,
) -> None:
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, status, context
            ) VALUES (
              'schedule-before', 'stable-name', 'workflow-before',
              '*/5 * * * *', 'ACTIVE', '{"source":"before"}'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, status, context
            ) VALUES (
              'schedule-reregistered', 'stable-name', 'workflow-after',
              '*/10 * * * *', 'ACTIVE', '{"source":"after"}'
            )
            """
        )

    release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT schedule_id, workflow_name, schedule, context
              FROM dbos.workflow_schedules
             WHERE schedule_name = 'stable-name'
            """
        )
        assert cur.fetchone() == (
            "schedule-before",
            "workflow-before",
            "*/5 * * * *",
            '{"source":"before"}',
        )


def test_release_removes_new_schedule_created_under_fence(
    promotion_database: str,
) -> None:
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'schedule-before', 'schedule-before', 'workflow-before',
              '*/5 * * * *', '{"source":"before"}'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'schedule-during-fence', 'schedule-during-fence', 'workflow-new',
              '*/10 * * * *', '{"source":"during-fence"}'
            )
            """
        )

    release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT schedule_id, schedule_name, workflow_name
              FROM dbos.workflow_schedules
             ORDER BY schedule_id
            """
        )
        assert cur.fetchall() == [
            ("schedule-before", "schedule-before", "workflow-before")
        ]


def test_successful_release_keeps_candidate_schedules_and_reopens_enqueues(
    promotion_database: str,
) -> None:
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'schedule-before', 'stable-name', 'workflow-before',
              '*/5 * * * *', '{"source":"before"}'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'schedule-candidate', 'stable-name', 'workflow-after',
              '*/10 * * * *', '{"source":"candidate"}'
            )
            """
        )

    receipt = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
        schedule_disposition="keep",
    )

    assert receipt["status"] == "released"
    assert receipt["schedule_disposition"] == "keep"
    assert receipt["schedules_restored"] == 0
    assert receipt["schedules_kept"] == 1
    assert receipt["candidate_schedule_ids"] == ["schedule-candidate"]
    assert receipt["candidate_schedule_snapshot_digest"].startswith("sha256:")
    with promotion_enqueue_permit(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
    ):
        pass
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT schedule_id, workflow_name, schedule, context
              FROM dbos.workflow_schedules
             WHERE schedule_name = 'stable-name'
            """
        )
        assert cur.fetchone() == (
            "schedule-candidate",
            "workflow-after",
            "*/10 * * * *",
            '{"source":"candidate"}',
        )


def test_same_owner_reacquire_preserves_exactly_empty_initial_snapshot(
    promotion_database: str,
) -> None:
    first = acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    assert first["schedule_snapshot_digest"].startswith("sha256:")
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'candidate-only', 'candidate-only', 'workflow-after',
              '*/10 * * * *', '{"source":"candidate"}'
            )
            """
        )

    resumed = acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    assert resumed["resumed"] is True
    assert resumed["schedule_snapshot_digest"] == first["schedule_snapshot_digest"]
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dbos.workflow_schedules")
        assert cur.fetchone()[0] == 0


def test_predecessor_bridge_restores_full_dbos_schedule_identity(
    promotion_database: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    source = bridge_path.read_text()
    assert "okg.substrate.runtime.admin" not in source
    assert "okg.substrate.runtime.quiescence" not in source
    assert "okg.substrate.runtime.promotion_fence" not in source
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)

    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, status,
              context, last_fired_at, automatic_backfill, cron_timezone,
              queue_name
            ) VALUES (
              'bridge-schedule', 'bridge-schedule', 'bridge-workflow',
              '17 */2 * * *', 'ACTIVE', '{"bridge":true}',
              '2026-07-26T10:00:00Z', true, 'America/Chicago',
              'okg-promotion-test'
            )
            """
        )
    monkeypatch.setattr(
        bridge,
        "client_drain",
        lambda **kwargs: SimpleNamespace(
            deployment=kwargs["deployment"],
            drained=False,
            in_flight_at_timeout=1,
            elapsed_seconds=0.1,
            pending_workflows=[{"workflow_id": "scheduled-inflight"}],
        ),
    )

    payload = bridge.quiesce_runtime(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
        candidate_revision="a" * 40,
        lifecycle_plan_digest="sha256:plan",
        lifecycle_contract_digest="sha256:contract",
        max_wait_seconds=1,
        stable_zero_seconds=0,
    )

    assert payload["ok"] is False
    assert payload["pre_mutation_reopen"]["schedules_restored"] == 1
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_name, schedule, status, context, last_fired_at,
                   automatic_backfill, cron_timezone, queue_name
              FROM dbos.workflow_schedules
             WHERE schedule_id = 'bridge-schedule'
            """
        )
        assert cur.fetchone() == (
            "bridge-workflow",
            "17 */2 * * *",
            "ACTIVE",
            '{"bridge":true}',
            "2026-07-26T10:00:00Z",
            True,
            "America/Chicago",
            "okg-promotion-test",
        )


def test_predecessor_bridge_refuses_same_owner_candidate_rebinding(
    promotion_database: str,
) -> None:
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge_binding",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    metadata = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": DEPLOYMENT,
        "candidate_revision": "a" * 40,
        "lifecycle_plan_digest": "sha256:plan",
        "lifecycle_contract_digest": "sha256:contract",
        "bootstrap_quiescence": True,
    }
    bridge._acquire_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
        metadata=metadata,
    )

    with pytest.raises(RuntimeError, match="candidate_revision"):
        bridge._acquire_predecessor_fence(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            owner=PROMOTION,
            metadata={**metadata, "candidate_revision": "b" * 40},
        )

    bridge._restore_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
    )


def test_release_refuses_keep_when_candidate_registered_no_schedules(
    promotion_database: str,
) -> None:
    """A `keep` release with an empty live table would silently end all
    scheduled work: the fence already deleted the predecessor's rows, and
    the digest of [] is a well-formed sha256 that shape checks accept."""
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'schedule-before', 'stable-name', 'workflow-before',
              '*/5 * * * *', '{"source":"before"}'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    # Candidate never registers anything — the live table stays empty.
    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="schedule_disposition=keep",
    ) as excinfo:
        release_promotion_fence(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            promotion_id=PROMOTION,
            schedule_disposition="keep",
        )
    # The operator needs to know WHICH schedules would be lost.
    assert excinfo.value.data["dropped_schedule_names"] == ["stable-name"]

    # The refusal must leave the fence recoverable: still held, snapshot
    # intact, so a `restore` release can put the predecessor back.
    status = promotion_fence_status(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
    )
    assert status is not None
    assert status["status"] == "fenced"

    receipt = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
        schedule_disposition="restore",
    )
    assert receipt["schedules_restored"] == 1
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute("SELECT schedule_id FROM dbos.workflow_schedules")
        assert cur.fetchall() == [("schedule-before",)]


def test_release_refuses_keep_when_candidate_registered_only_some(
    promotion_database: str,
) -> None:
    """A candidate that registers 1 of 3 loses the other 2 just as surely as
    one that registers none, and only the schedule names reveal it."""
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        for name in ("sched-a", "sched-b", "sched-c"):
            cur.execute(
                """
                INSERT INTO dbos.workflow_schedules (
                  schedule_id, schedule_name, workflow_name, schedule, context
                ) VALUES (%s, %s, %s, '*/5 * * * *', '{}')
                """,
                (f"id-{name}", name, f"workflow-{name}"),
            )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    # Candidate registers only one of the three, with a fresh schedule_id.
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'candidate-uuid', 'sched-a', 'workflow-sched-a',
              '*/5 * * * *', '{}'
            )
            """
        )

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="schedule_disposition=keep",
    ) as excinfo:
        release_promotion_fence(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            promotion_id=PROMOTION,
            schedule_disposition="keep",
        )
    assert excinfo.value.data["dropped_schedule_names"] == [
        "sched-b",
        "sched-c",
    ]


def test_release_keep_accepts_reregistration_under_a_new_schedule_id(
    promotion_database: str,
) -> None:
    """DBOS mints a fresh schedule_id on every registration, so the gate must
    compare schedule_name — otherwise every normal deployment would refuse."""
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'old-uuid', 'sched-a', 'workflow-old', '*/5 * * * *', '{}'
            )
            """
        )
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'new-uuid', 'sched-a', 'workflow-new', '*/5 * * * *', '{}'
            )
            """
        )

    receipt = release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
        schedule_disposition="keep",
    )
    assert receipt["status"] == "released"
    assert receipt["schedules_kept"] == 1


def test_predecessor_bridge_resume_keeps_post_capture_schedules_restorable(
    promotion_database: str,
) -> None:
    """A resume deletes whatever was registered since the last capture. If
    those rows do not join the snapshot, the later restore cannot bring them
    back and deletes them silently — which would defeat the restore guard."""
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge_resume",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    metadata = {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": DEPLOYMENT,
        "candidate_revision": "a" * 40,
        "lifecycle_plan_digest": "sha256:plan",
        "lifecycle_contract_digest": "sha256:contract",
    }

    # First capture: empty table, so the snapshot starts empty.
    bridge._acquire_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
        metadata=metadata,
    )
    # Predecessor worker registers a schedule, then the operation resumes.
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'late-uuid', 'late-name', 'workflow-late',
              '*/5 * * * *', '{"source":"late"}'
            )
            """
        )
    bridge._acquire_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
        metadata=metadata,
    )

    # The resume captured it, so restore must put it back rather than drop it.
    bridge._restore_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
    )
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schedule_name, workflow_name FROM dbos.workflow_schedules"
        )
        assert cur.fetchall() == [("late-name", "workflow-late")]


def test_predecessor_bridge_restore_allows_reregistration_with_new_id(
    promotion_database: str,
) -> None:
    """The fence does not stop the predecessor worker, and DBOS mints a fresh
    schedule_id on every registration. Re-registering the SAME logical
    schedule must restore cleanly — keying the guard on schedule_id would
    refuse the ordinary case and wedge the deployment."""
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge_reregister",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)

    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, status,
              context
            ) VALUES (
              'old-uuid', 'bridge-schedule', 'workflow-before',
              '*/5 * * * *', 'ACTIVE', '{"source":"before"}'
            )
            """
        )
    bridge._acquire_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
        metadata={
            "schema": "okg.deployment_runtime_quiescence.v1",
            "deployment": DEPLOYMENT,
            "candidate_revision": "a" * 40,
            "lifecycle_plan_digest": "sha256:plan",
            "lifecycle_contract_digest": "sha256:contract",
        },
    )
    # Predecessor worker relaunches and re-registers the same logical
    # schedule under a brand-new UUID.
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, status,
              context
            ) VALUES (
              'fresh-uuid', 'bridge-schedule', 'workflow-before',
              '*/5 * * * *', 'ACTIVE', '{"source":"before"}'
            )
            """
        )

    bridge._restore_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
    )
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schedule_id, schedule_name FROM dbos.workflow_schedules"
        )
        assert cur.fetchall() == [("old-uuid", "bridge-schedule")]


def test_predecessor_bridge_restore_refuses_unrestorable_live_schedules(
    promotion_database: str,
) -> None:
    """The fence does not stop the predecessor worker, so a schedule can be
    registered after capture. Restore may only delete what it can put back."""
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge_restore",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)

    # Fence with an empty schedule table: the snapshot is [].
    bridge._acquire_predecessor_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        owner=PROMOTION,
        metadata={
            "schema": "okg.deployment_runtime_quiescence.v1",
            "deployment": DEPLOYMENT,
            "candidate_revision": "a" * 40,
            "lifecycle_plan_digest": "sha256:plan",
            "lifecycle_contract_digest": "sha256:contract",
        },
    )
    # The predecessor worker registers a schedule afterwards.
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbos.workflow_schedules (
              schedule_id, schedule_name, workflow_name, schedule, context
            ) VALUES (
              'schedule-late', 'late-name', 'workflow-late',
              '*/5 * * * *', '{"source":"late"}'
            )
            """
        )

    with pytest.raises(RuntimeError, match="absent from the captured snapshot"):
        bridge._restore_predecessor_fence(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            owner=PROMOTION,
        )

    # The live schedule must survive the refusal.
    with psycopg.connect(promotion_database) as conn, conn.cursor() as cur:
        cur.execute("SELECT schedule_id FROM dbos.workflow_schedules")
        assert cur.fetchall() == [("schedule-late",)]


def test_predecessor_bridge_drain_fails_closed_on_queue_read_error() -> None:
    """A queue-read fault must surface, not read as an empty queue —
    swallowing it reports drained=True with work still in flight."""
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge_drain_error",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)

    class BrokenDBOSClient:
        def list_queued_workflows(self, *, queue_name: str) -> list[object]:
            raise RuntimeError(
                'relation "dbos.workflow_queue" does not exist'
            )

    bridge._build_dbos_client = lambda **_kwargs: BrokenDBOSClient()

    with pytest.raises(RuntimeError, match="does not exist"):
        bridge.client_drain(
            deployment="okg-ci",
            dsn="postgres://predecessor",
            max_wait_seconds=1,
            poll_interval_seconds=0,
            stable_zero_seconds=0,
        )


def test_predecessor_bridge_owns_internal_queue_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge_path = (
        Path(__file__).resolve().parents[3]
        / "deployments"
        / "okg-ci"
        / "runtime_quiescence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "okg_ci_predecessor_quiescence_bridge_drain",
        bridge_path,
    )
    assert spec is not None and spec.loader is not None
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    observed: list[str] = []

    class PredecessorDBOSClient:
        def list_queued_workflows(self, *, queue_name: str) -> list[object]:
            observed.append(queue_name)
            return []

    monkeypatch.setattr(
        bridge,
        "_build_dbos_client",
        lambda **_kwargs: PredecessorDBOSClient(),
    )

    result = bridge.client_drain(
        deployment="okg-ci",
        dsn="postgres://predecessor",
        max_wait_seconds=1,
        poll_interval_seconds=0,
        stable_zero_seconds=0,
    )

    assert result.drained is True
    assert observed == [
        "okg-okg-ci",
        "okg-okg-ci-source-sync",
        "_dbos_internal_queue",
    ]


def test_manual_schedule_trigger_is_rejected_by_active_fence(
    promotion_database: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admin

    triggered: list[str] = []

    class FakeClient:
        def trigger_schedule(self, schedule_id: str) -> SimpleNamespace:
            triggered.append(schedule_id)
            return SimpleNamespace(workflow_id="manual-trigger")

    monkeypatch.setattr(admin, "_build_client", lambda **_kwargs: FakeClient())
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="enqueues are fenced",
    ):
        admin.client_trigger_schedule(
            deployment=DEPLOYMENT,
            dsn=promotion_database,
            runtime=SimpleNamespace(schedules={}),
            schedule_id="manual-source-poll",
        )

    assert triggered == []
    release_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )
    workflow_id = admin.client_trigger_schedule(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        runtime=SimpleNamespace(schedules={}),
        schedule_id="manual-source-poll",
    )
    assert workflow_id == "manual-trigger"
    assert triggered == ["manual-source-poll"]


# ---------------------------------------------------------------------------
# Issue #1310 — leaked idle-in-transaction sessions holding advisory locks.
#
# The permit is a `pg_advisory_xact_lock`, so it is an OPEN Postgres
# transaction for as long as its body runs. Held around an enqueue that is
# itself a database write, that is correct and bounded. Held around a long
# non-database wait it is the live defect: on okg-workspace two sessions on
# `okg.deployment_promotion_fences` sat `idle in transaction` for ~57 minutes
# with 41 connections queued behind their advisory locks, and the same shape
# reproduced during a clean `okg provision` at 266 seconds.
#
# These probes read `pg_locks` by the fence's advisory-lock KEY and
# `pg_stat_activity` by the fence QUERY TEXT, never by `application_name`.
# That matters: this change is what first gives fence sessions an
# application_name, so a probe keyed on it would have found zero holders
# before the fix and passed vacuously.
# ---------------------------------------------------------------------------


_DEPLOYMENT_LOCK_KEY_SQL = "SELECT hashtextextended(%s, 0)"


def _deployment_lock_key(dsn: str, deployment: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            _DEPLOYMENT_LOCK_KEY_SQL,
            (f"okg:deployment-promotion:{deployment}",),
        )
        return int(cur.fetchone()[0])


def _granted_deployment_advisory_locks(dsn: str, key: int) -> int:
    """Count granted advisory locks on the deployment-promotion key.

    `pg_advisory_xact_lock(bigint)` splits the key across `classid` (high 32
    bits) and `objid` (low 32 bits), so recombine rather than comparing to
    either half.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM pg_locks
             WHERE locktype = 'advisory'
               AND granted
               AND database = (
                   SELECT oid FROM pg_database WHERE datname = current_database()
               )
               AND ((classid::bigint << 32) | objid::bigint)
                   = (%s::bigint & 4294967295)
                     | ((%s::bigint >> 32 & 4294967295) << 32)
            """,
            (key, key),
        )
        return int(cur.fetchone()[0])


def _idle_in_transaction_fence_sessions(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM pg_stat_activity
             WHERE pid <> pg_backend_pid()
               AND datname = current_database()
               AND state = 'idle in transaction'
               AND query ILIKE '%deployment_promotion_fences%'
            """
        )
        return int(cur.fetchone()[0])


def test_permit_body_runs_inside_an_open_fence_transaction(
    promotion_database: str,
) -> None:
    """Pins WHY a long body must not use the permit.

    Not a regression guard on a bug — this is the permit working as designed.
    It is recorded so a later reader can see that "hold the permit" and "hold
    a transaction open" are the same statement, which is what makes wrapping
    a minutes-long body with it a pool-jamming leak.
    """
    key = _deployment_lock_key(promotion_database, DEPLOYMENT)

    with promotion_enqueue_permit(
        deployment=DEPLOYMENT, dsn=promotion_database,
    ):
        assert _granted_deployment_advisory_locks(promotion_database, key) == 1
        assert _idle_in_transaction_fence_sessions(promotion_database) == 1

    assert _granted_deployment_advisory_locks(promotion_database, key) == 0
    assert _idle_in_transaction_fence_sessions(promotion_database) == 0


def test_assert_deployment_admitted_holds_nothing_while_the_body_runs(
    promotion_database: str,
) -> None:
    """The admission check must release before the caller's body starts."""
    key = _deployment_lock_key(promotion_database, DEPLOYMENT)

    assert_deployment_admitted(
        deployment=DEPLOYMENT, dsn=promotion_database,
    )

    assert _granted_deployment_advisory_locks(promotion_database, key) == 0
    assert _idle_in_transaction_fence_sessions(promotion_database) == 0


def test_assert_deployment_admitted_still_refuses_under_an_active_fence(
    promotion_database: str,
) -> None:
    """Releasing the transaction must not weaken the admission decision."""
    acquire_promotion_fence(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )

    with pytest.raises(
        DeploymentEnvironmentInvalid, match="enqueues are fenced",
    ):
        assert_deployment_admitted(
            deployment=DEPLOYMENT, dsn=promotion_database,
        )

    # The owning promotion itself is still admitted.
    assert_deployment_admitted(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        promotion_id=PROMOTION,
    )


def test_fence_sessions_carry_the_idle_in_transaction_bound(
    promotion_database: str,
    monkeypatch,
) -> None:
    """A fence session must not be able to sit idle in transaction forever.

    The bound is what makes a future leak self-healing rather than a jam that
    needs an operator to terminate backends by hand.
    """
    from okg.substrate.db.connection import (
        DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
        IDLE_IN_TRANSACTION_TIMEOUT_ENV,
    )

    # A DISTINCTIVE value, and `source = 'client'`. Asserting only the
    # built-in default against SHOW would pass on a cluster that already had
    # `idle_in_transaction_session_timeout = 300000` set by an operator --
    # exactly the okg-workspace posture the default was chosen to match -- so
    # the test could not tell "our option arrived" from "the server was
    # already configured that way".
    distinctive_ms = DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS + 271_000
    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, str(distinctive_ms))

    with deployment_admission_lock(
        deployment=DEPLOYMENT, dsn=promotion_database,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT setting, source, reset_val FROM pg_settings "
                "WHERE name = 'idle_in_transaction_session_timeout'"
            )
            setting, source, reset_val = cur.fetchone()

    assert source == "client", (
        f"fence session took idle_in_transaction_session_timeout from "
        f"{source!r} (setting={setting!r}, reset_val={reset_val!r}); the "
        f"connection factory must send it as a client option"
    )
    assert int(setting) == distinctive_ms, (
        f"fence session reported idle_in_transaction_session_timeout="
        f"{setting!r}; expected {distinctive_ms}"
    )

    # And the shipped default, when nothing overrides it.
    monkeypatch.delenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, raising=False)
    with deployment_admission_lock(
        deployment=DEPLOYMENT, dsn=promotion_database,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT setting, source FROM pg_settings "
                "WHERE name = 'idle_in_transaction_session_timeout'"
            )
            setting, source = cur.fetchone()
    assert source == "client"
    assert int(setting) == DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS


def test_the_cutover_boundary_exempts_itself_from_the_idle_bound(
    promotion_database: str,
) -> None:
    """The one hold that is meant to sit idle in transaction opts out.

    `source_sync_replay_boundary` keeps this transaction open through
    `DBOS.launch()`. Terminating it mid-launch would roll the boundary back
    and refuse worker readiness with a message about the cutover rather than
    about a timeout, so it lifts the bound for that transaction only.
    """
    with deployment_admission_lock(
        deployment=DEPLOYMENT,
        dsn=promotion_database,
        bound_idle_in_transaction=False,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW idle_in_transaction_session_timeout")
            assert cur.fetchone()[0] == "0"

    # SET LOCAL: the exemption must not survive its transaction.
    with deployment_admission_lock(
        deployment=DEPLOYMENT, dsn=promotion_database,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW idle_in_transaction_session_timeout")
            assert cur.fetchone()[0] != "0"
