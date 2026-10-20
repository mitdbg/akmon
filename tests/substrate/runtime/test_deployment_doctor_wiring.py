"""Tests for the scheduled doctor's deployment-check fan-out.

Covers:
  * `_run_deployment_doctor_checks` is a no-op when the deployment
    has no registered checks (fast path).
  * `_check_payload_ok` reads a missing or malformed ok as unknown (#2573).
  * `DeploymentDoctorHealthVerdict.to_dict` shape is pinned.
  * `check_deployment_doctor_health` reports `never_ran` on a deployment
    that hasn't been scheduled-doctored yet.

The full end-to-end (scheduled workflow → deployment_checks event →
doctor probe parses it) is exercised by the cutover-time verification
step on okg-workspace.
"""
from __future__ import annotations

import pytest


def test_check_payload_ok_reads_missing_verdict_as_unknown():
    """Missing or malformed `ok` is Unknown with a reason, never a pass (#2573)."""
    from okg.substrate.runtime.scheduled import _check_payload_ok
    from okg.substrate.unknown import Unknown

    assert _check_payload_ok({"ok": True}) is True
    assert _check_payload_ok({"ok": False}) is False
    assert _check_payload_ok({}) == Unknown("deployment_check_reported_no_ok")
    assert _check_payload_ok({"ok": "no"}) == Unknown("deployment_check_ok_not_boolean")
    assert _check_payload_ok("not-a-dict") == Unknown("deployment_check_not_an_object")


def test_run_deployment_doctor_checks_empty_for_unknown():
    """A deployment that doesn't exist on the filesystem returns
    no checks. Confirmed by `load_deployment_doctor_checks` returning
    [] when the deployment dir is missing."""
    from okg.substrate.runtime.scheduled import (
        _run_deployment_doctor_checks,
    )

    result = _run_deployment_doctor_checks(deployment="ghost-deployment")
    assert isinstance(result, dict)
    # Either {} (no checks loaded) or {"<load-failed>": ...} are
    # both acceptable signals that no checks ran. Importantly, the
    # function did NOT raise.


def test_deployment_doctor_verdict_to_dict():
    """Verdict.to_dict returns the expected shape."""
    from okg.substrate.runtime.publisher_health import (
        DeploymentDoctorHealthVerdict,
    )

    v = DeploymentDoctorHealthVerdict(
        deployment="test",
        ok=False,
        code="deployment_doctor_check_failed",
        message="1 of 2 checks failed",
        checks_total=2,
        checks_failed=1,
        last_run_at="2026-05-11T16:00:00+00:00",
        threshold_minutes=15,
    )
    d = v.to_dict()
    assert d["deployment"] == "test"
    assert d["ok"] is False
    assert d["checks_total"] == 2
    assert d["checks_failed"] == 1
    assert d["threshold_minutes"] == 15
    assert d["last_run_at"] == "2026-05-11T16:00:00+00:00"


def test_doctor_workflow_emits_deployment_checks_field():
    """Source inspection: the scheduled doctor's workflow body
    populates ``verdict["deployment_checks"]`` before emitting the
    event."""
    import inspect

    from okg.substrate.runtime import scheduled

    src = inspect.getsource(scheduled.register_substrate_schedules)
    assert "deployment_checks" in src, (
        "scheduled doctor workflow must populate "
        "verdict['deployment_checks']"
    )
    assert "_run_deployment_doctor_checks" in src, (
        "scheduled doctor workflow must call "
        "_run_deployment_doctor_checks"
    )
    assert 'verdict["deployment"] = deployment' in src, (
        "scheduled doctor verdicts must carry the deployment identity used "
        "by health readers"
    )
    assert "promotion_id_from_recovery_context" in src
    assert "promotion_id=promotion_id" in src


pytestmark = pytest.mark.postgres


def test_check_deployment_doctor_health_never_ran_path(
    clean_runtime: str,
) -> None:
    """On a deployment with no scheduled-doctor history, the probe
    reports `never_ran`, not a crash."""
    from okg.substrate.runtime.publisher_health import (
        check_deployment_doctor_health,
    )

    verdict = check_deployment_doctor_health(
        deployment="never-existed-deployment",
        dsn=clean_runtime,
    )

    assert verdict.ok is False
    assert verdict.code == "deployment_doctor_never_ran"
    assert verdict.checks_total == 0
    assert verdict.last_run_at is None


def test_deployment_doctor_health_matches_deployment_from_event(
    monkeypatch,
) -> None:
    """DBOS workflow inputs are pickled, so deployment matching must
    use the emitted doctor_verdict event rather than SQL LIKE on
    workflow_status.inputs."""
    from datetime import datetime, timezone

    from okg.substrate.runtime import publisher_health as ph

    recent_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    class _Cursor:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            if "to_regclass" in sql:
                self._conn._next_one = ("dbos.workflow_status",)
                self._conn._next_all = []
            else:
                self._conn._next_one = None
                self._conn._next_all = [("wf-okg-workspace", recent_ms)]

        def fetchone(self):
            return self._conn._next_one

        def fetchall(self):
            return self._conn._next_all

    class _Conn:
        def __init__(self):
            self._next_one = None
            self._next_all = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return _Cursor(self)

    # check_deployment_doctor_health is defined in (and re-exported from) the
    # publisher_health_doctor_overlay sub-module, so it resolves its connection
    # seam in THAT module's namespace -- patch it there, not on the re-export
    # shim. That seam is `db_connect`, the substrate connection factory the
    # module now calls; patching `psycopg` there would name something the
    # module no longer binds.
    monkeypatch.setattr(
        "okg.substrate.runtime.publisher_health_doctor_overlay.db_connect",
        lambda *a, **kw: _Conn(),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_get_event",
        lambda **_kw: {
            "deployment": "okg-workspace",
            "deployment_checks": {
                "coding-context": {"ok": True},
            },
        },
    )

    verdict = ph.check_deployment_doctor_health(
        deployment="okg-workspace",
        dsn="postgres://unused",
    )

    assert verdict.ok is True
    assert verdict.code == "deployment_doctor_healthy"
    assert verdict.checks_total == 1
    assert verdict.checks_failed == 0


# ---------------------------------------------------------------------------
# Issue #1310 -- the scheduled doctor must not hold a Postgres transaction
# open across its own body.
#
# `promotion_enqueue_permit` is a `pg_advisory_xact_lock`, so wrapping the
# doctor body with it kept a transaction open for the whole run -- and the
# doctor's own comment says that is minutes on a large graph. Live signature
# during a clean `okg provision`: a session `idle in transaction` for 266
# seconds on the fence SELECT, in Client/ClientRead, with backends queued
# behind its advisory lock. The doctor now decides admission once and lets go
# before running.
#
# The probes below key on the fence's advisory-lock key and on the fence query
# text, never on `application_name`: fence sessions only acquired an
# application_name in this same change, so an application_name probe would
# have found zero holders before the fix and passed vacuously.
# ---------------------------------------------------------------------------


_GRANTED_DEPLOYMENT_ADVISORY_LOCKS = """
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
"""

_IDLE_IN_TRANSACTION_FENCE_SESSIONS = """
    SELECT count(*)
      FROM pg_stat_activity
     WHERE pid <> pg_backend_pid()
       AND datname = current_database()
       AND state = 'idle in transaction'
       AND query ILIKE '%deployment_promotion_fences%'
"""


def _fake_dbos(monkeypatch):
    """Decorate-only DBOS stand-in; returns the captured events dict."""
    import dbos

    events: dict[str, object] = {}

    def fake_workflow(*_args, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        dbos.DBOS,
        "set_event",
        staticmethod(lambda key, value: events.__setitem__(key, value)),
    )
    return events


def test_scheduled_doctor_body_runs_with_no_open_fence_transaction(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Fail-before/pass-after for #1310.

    Before the fix the assertions inside the fake doctor body observed one
    granted advisory lock on the deployment-promotion key and one session
    `idle in transaction` on `okg.deployment_promotion_fences`, because the
    permit wrapped the body. They must both be zero.
    """
    import psycopg

    from okg.substrate.runtime import scheduled as sched

    deployment = "doctor-idle-in-transaction-1310"
    events = _fake_dbos(monkeypatch)

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT hashtextextended(%s, 0)",
            (f"okg:deployment-promotion:{deployment}",),
        )
        key = int(cur.fetchone()[0])

    observed: dict[str, int] = {}

    def probing_doctor_subprocess(*, deployment: str) -> dict:
        # A SEPARATE connection, read while the doctor body is mid-flight --
        # the moment the live jam was observed at.
        with psycopg.connect(
            clean_runtime, autocommit=True,
        ) as probe, probe.cursor() as cur:
            cur.execute(_GRANTED_DEPLOYMENT_ADVISORY_LOCKS, (key, key))
            observed["advisory_locks"] = int(cur.fetchone()[0])
            cur.execute(_IDLE_IN_TRANSACTION_FENCE_SESSIONS)
            observed["idle_in_transaction"] = int(cur.fetchone()[0])
        return {"ok": True, "errors": []}

    monkeypatch.setattr(
        sched, "_run_doctor_subprocess", probing_doctor_subprocess,
    )
    monkeypatch.setattr(
        sched, "_run_deployment_doctor_checks", lambda **_kwargs: {},
    )

    sched.register_substrate_schedules(
        deployment=deployment, dsn=clean_runtime,
    )
    sched._DECORATED[sched.DOCTOR_SCHEDULE_ID](None, None)

    assert events.get("doctor_verdict") is not None, (
        "the doctor body must still run and publish its verdict"
    )
    assert observed["advisory_locks"] == 0, (
        "the scheduled doctor held the deployment-promotion advisory lock "
        "for the duration of its body; that lock is transaction-scoped, so "
        "holding it means an open transaction and a jammed connection pool "
        "(issue #1310)"
    )
    assert observed["idle_in_transaction"] == 0, (
        "a fence session was left idle in transaction while the doctor body "
        "ran (issue #1310)"
    )


def test_scheduled_doctor_still_refuses_under_an_active_promotion_fence(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Releasing the transaction must not weaken the admission decision."""
    import psycopg

    from okg.substrate.errors import DeploymentEnvironmentInvalid
    from okg.substrate.runtime import scheduled as sched

    deployment = "doctor-fenced-1310"
    _fake_dbos(monkeypatch)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.deployment_promotion_fences "
            "(deployment_name, promotion_id, status) "
            "VALUES (%s, %s, 'fenced') "
            "ON CONFLICT (deployment_name) DO UPDATE SET "
            "promotion_id = EXCLUDED.promotion_id, status = EXCLUDED.status",
            (deployment, "promotion-owner"),
        )

    ran: list[str] = []

    def _record(*, deployment: str) -> dict:
        ran.append(deployment)
        return {"ok": True}

    monkeypatch.setattr(sched, "_run_doctor_subprocess", _record)
    monkeypatch.setattr(
        sched, "_run_deployment_doctor_checks", lambda **_kwargs: {},
    )

    sched.register_substrate_schedules(
        deployment=deployment, dsn=clean_runtime,
    )
    with pytest.raises(
        DeploymentEnvironmentInvalid, match="enqueues are fenced",
    ):
        sched._DECORATED[sched.DOCTOR_SCHEDULE_ID](None, None)

    assert ran == []
