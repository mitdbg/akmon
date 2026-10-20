"""Tests for `harden-runtime-supervisor`.

Covers:

  1. `services.should_register_*` DSN probes do not hang on an
     unreachable DSN — `connect_timeout=10` bounds the wait (§A).
  2. The gates-reevaluate workflow:
     * Skips features whose gate is currently False.
     * Skips features that are already registered at boot.
     * Late-registers features whose gate flipped from False → True
       since boot.
"""
from __future__ import annotations

import time

import pytest


# ---------------------------------------------------------------------------
# §A connect_timeout convention
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_should_register_lens_putback_does_not_hang_on_unreachable_dsn():
    """A DSN pointing at a closed port returns within
    ~connect_timeout (10s), not hangs forever."""
    from okg.substrate.runtime.services import (
        should_register_lens_putback,
    )

    # Use a TEST-NET reserved IP (RFC 5737) that should always be
    # unroutable. A bare `127.0.0.1:1` would `ECONNREFUSED` fast on
    # macOS; we want a SYN-no-ACK condition that exercises the
    # connect_timeout. The 192.0.2.0/24 block is reserved for docs
    # and never routes.
    bad_dsn = (
        "postgres://nobody:nobody@192.0.2.1:5432/x"
        "?sslmode=disable"
    )

    started = time.monotonic()
    result = should_register_lens_putback(dsn=bad_dsn)
    elapsed = time.monotonic() - started

    # Conservative bias on failure: True.
    assert result is True
    # Bounded by connect_timeout=10 plus margin. Allow up to 15s
    # for slow CI / system DNS / kernel retry quirks.
    assert elapsed < 15.0, (
        f"call took {elapsed:.1f}s — connect_timeout did not bound "
        f"the wait"
    )


@pytest.mark.slow
def test_should_register_escalation_sweep_does_not_hang():
    from okg.substrate.runtime.services import (
        should_register_escalation_sweep,
    )

    bad_dsn = (
        "postgres://nobody:nobody@192.0.2.1:5432/x"
        "?sslmode=disable"
    )

    started = time.monotonic()
    result = should_register_escalation_sweep(dsn=bad_dsn)
    elapsed = time.monotonic() - started

    assert result is True
    assert elapsed < 15.0, (
        f"call took {elapsed:.1f}s — connect_timeout did not bound "
        f"the wait"
    )


# ---------------------------------------------------------------------------
# §B gates-reevaluate
# ---------------------------------------------------------------------------


def test_reevaluation_skips_features_with_false_gate(monkeypatch):
    """If a gate returns False, the feature stays unregistered."""
    from okg.substrate.runtime import gates_reevaluate_workflow as m

    # Stub the dependencies so we don't touch a real DBOS / DB.
    fake_manifest = type("M", (), {
        "postgres_dsn": "postgres://test",
        "runtime": type("R", (), {"schedules": {}})(),
    })()

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: fake_manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_embed",
        lambda _m: False,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_lens_putback",
        lambda *, dsn: False,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_escalation_sweep",
        lambda *, dsn: False,
    )

    # Patch _is_already_registered + _register_feature so we can
    # see what would have happened without touching DBOS.
    register_calls: list[str] = []
    monkeypatch.setattr(
        m, "_is_already_registered", lambda _f: False,
    )
    monkeypatch.setattr(
        m, "_register_feature",
        lambda f, **_kw: register_calls.append(f),
    )

    diff = m._run_reevaluation(deployment="test-dep")

    assert diff["registered_now"] == []
    assert set(diff["skipped_gate_false"]) == {
        "embed", "lens_putback", "escalation_sweep",
    }
    assert register_calls == []


def test_reevaluation_skips_features_already_registered(monkeypatch):
    """If a gate is True but the feature is already registered,
    no double-decoration."""
    from okg.substrate.runtime import gates_reevaluate_workflow as m

    fake_manifest = type("M", (), {
        "postgres_dsn": "postgres://test",
        "runtime": type("R", (), {"schedules": {}})(),
    })()
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: fake_manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_embed",
        lambda _m: True,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_lens_putback",
        lambda *, dsn: True,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_escalation_sweep",
        lambda *, dsn: True,
    )
    monkeypatch.setattr(
        m, "_is_already_registered", lambda _f: True,
    )
    register_calls: list[str] = []
    monkeypatch.setattr(
        m, "_register_feature",
        lambda f, **_kw: register_calls.append(f),
    )

    diff = m._run_reevaluation(deployment="test-dep")

    assert diff["registered_now"] == []
    assert set(diff["skipped_already_registered"]) == {
        "embed", "lens_putback", "escalation_sweep",
    }
    assert register_calls == []


def test_reevaluation_registers_features_that_flipped_on(monkeypatch):
    """The core behavior — a gate that flipped from skip → register
    since boot triggers a fresh `_register_feature` call."""
    from okg.substrate.runtime import gates_reevaluate_workflow as m

    fake_manifest = type("M", (), {
        "postgres_dsn": "postgres://test",
        "runtime": type("R", (), {
            "schedules": {"okg_lens_putback_sweep": "*/7 * * * *"},
        })(),
    })()
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: fake_manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_embed",
        lambda _m: False,  # still off
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_lens_putback",
        lambda *, dsn: True,  # flipped on
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_escalation_sweep",
        lambda *, dsn: True,  # also flipped on
    )
    monkeypatch.setattr(
        m, "_is_already_registered", lambda _f: False,
    )

    register_calls: list[tuple[str, dict[str, str]]] = []

    def fake_register(feature: str, **kwargs):
        register_calls.append(
            (feature, dict(kwargs["schedule_overrides"])),
        )

    monkeypatch.setattr(m, "_register_feature", fake_register)

    diff = m._run_reevaluation(deployment="test-dep")

    # Two features registered, embed still skipped (gate False).
    assert set(diff["registered_now"]) == {
        "lens_putback", "escalation_sweep",
    }
    assert diff["skipped_gate_false"] == ["embed"]
    # Manifest's schedule override is plumbed through to the
    # late-registration path so the new schedule honors it.
    lens_call = next(c for c in register_calls if c[0] == "lens_putback")
    assert lens_call[1] == {"okg_lens_putback_sweep": "*/7 * * * *"}


# ---------------------------------------------------------------------------
# §C doctor-verdict overlay
# ---------------------------------------------------------------------------


def test_overlay_returns_none_when_no_workflow_status_table(monkeypatch):
    """Fresh DB with no dbos schema → overlay returns None and the
    caller falls back to `not_observed`."""
    from okg.substrate.runtime import publisher_health as ph
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    class _FakeCursor:
        def __init__(self):
            self.queries: list[str] = []
            self.row: tuple | None = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self.queries.append(sql)
            if "to_regclass" in sql:
                self.row = (None,)
            else:
                self.row = None

        def fetchone(self):
            return self.row

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return _FakeCursor()

    monkeypatch.setattr(pdo, "db_connect", lambda *a, **kw: _FakeConn())

    out = ph._try_doctor_verdict_overlay(
        deployment="x",
        dsn="postgres://does-not-matter",
        threshold_minutes=10.0,
    )
    assert out is None


def test_overlay_returns_fresh_verdict_when_doctor_recently_ran(
    monkeypatch,
):
    """A recent successful doctor verdict produces a fresh
    overlay with `source=scheduled_doctor_event`."""
    from datetime import datetime, timezone
    from okg.substrate.runtime import publisher_health as ph
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    recent_ms = now_ms - 2 * 60 * 1000  # 2 minutes ago

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
                # The overlay now uses fetchall() with LIMIT 5.
                self._conn._next_all = [("fake-wf-uuid", recent_ms)]
                self._conn._next_one = None

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

    monkeypatch.setattr(pdo, "db_connect", lambda *a, **kw: _Conn())
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_get_event",
        lambda **_kw: {
            "ok": True,
            "deployment": "x",
            "runtime_services_health": {
                "ok": True,
                "code": "runtime_services_healthy",
                "message": "alive",
                "watcher": {"alive": True, "events_seen": 42},
                "webhook": {"alive": False, "enabled": False},
            },
        },
    )

    out = ph._try_doctor_verdict_overlay(
        deployment="x",
        dsn="postgres://does-not-matter",
        threshold_minutes=10.0,
    )
    assert out is not None
    assert out.ok is True
    assert out.code == "runtime_services_healthy"
    assert out.source == "scheduled_doctor_event"
    assert out.observed_minutes_ago is not None
    assert out.observed_minutes_ago < 3.0
    assert out.watcher == {"alive": True, "events_seen": 42}


def test_overlay_flips_unhealthy_when_stale(monkeypatch):
    """A doctor verdict older than the threshold trips the
    `runtime_services_overlay_stale` code."""
    from datetime import datetime, timezone
    from okg.substrate.runtime import publisher_health as ph
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    very_old_ms = now_ms - 30 * 60 * 1000  # 30 minutes ago

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
                self._conn._next_all = [("fake-wf-uuid", very_old_ms)]
                self._conn._next_one = None

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

    monkeypatch.setattr(pdo, "db_connect", lambda *a, **kw: _Conn())
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_get_event",
        lambda **_kw: {
            "ok": True,
            "deployment": "x",
            "runtime_services_health": {
                "ok": True,
                "code": "runtime_services_healthy",
                "watcher": {"alive": True},
                "webhook": {"alive": True},
            },
        },
    )

    out = ph._try_doctor_verdict_overlay(
        deployment="x",
        dsn="postgres://does-not-matter",
        threshold_minutes=10.0,
    )
    assert out is not None
    assert out.ok is False
    assert out.code == "runtime_services_overlay_stale"
    assert out.observed_minutes_ago > 10.0
    assert "may not be running" in out.message


def test_check_runtime_services_health_uses_overlay_when_dsn_given(
    monkeypatch,
):
    """End-to-end of §C: in-process registry empty + dsn provided
    → probe returns the overlay verdict, not `not_observed`."""
    from okg.substrate.runtime import publisher_health as ph
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo
    from okg.substrate.runtime.services import get_services_state

    # Force registry to look empty.
    get_services_state().deployment = None

    fake_overlay = ph.RuntimeServicesHealthVerdict(
        deployment="x",
        ok=True,
        code="runtime_services_healthy",
        message="from overlay",
        watcher={"alive": True},
        webhook={"alive": False},
        source="scheduled_doctor_event",
        last_observed_at="2026-05-11T10:00:00+00:00",
        observed_minutes_ago=3.4,
    )
    # Patch the overlay helper in its real home; the
    # `check_runtime_services_health` body lives in
    # publisher_health_doctor_overlay and looks up the helper
    # by local name there.
    monkeypatch.setattr(
        pdo, "_try_doctor_verdict_overlay",
        lambda **_kw: fake_overlay,
    )

    out = ph.check_runtime_services_health(
        deployment="x",
        dsn="postgres://test",
    )
    assert out.source == "scheduled_doctor_event"
    assert out.observed_minutes_ago == 3.4
    assert out.code == "runtime_services_healthy"


def test_check_runtime_services_health_falls_back_when_no_overlay(
    monkeypatch,
):
    """No DSN AND no live registry → keeps the existing
    `not_observed` payload."""
    from okg.substrate.runtime import publisher_health as ph
    from okg.substrate.runtime.services import get_services_state

    get_services_state().deployment = None

    out = ph.check_runtime_services_health(deployment="x")
    assert out.code == "runtime_services_not_observed"
    assert out.source == "in_process_registry"
    assert out.ok is True


def test_reevaluation_swallows_per_feature_registration_errors(monkeypatch):
    """A failure registering one feature must not break the
    re-evaluation of the others — leave it for the next tick."""
    from okg.substrate.runtime import gates_reevaluate_workflow as m

    fake_manifest = type("M", (), {
        "postgres_dsn": "postgres://test",
        "runtime": type("R", (), {"schedules": {}})(),
    })()
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: fake_manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_embed",
        lambda _m: True,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_lens_putback",
        lambda *, dsn: True,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.should_register_escalation_sweep",
        lambda *, dsn: True,
    )
    monkeypatch.setattr(
        m, "_is_already_registered", lambda _f: False,
    )

    succeeded: list[str] = []

    def flaky_register(feature: str, **_kwargs):
        if feature == "lens_putback":
            raise RuntimeError("DBOS rejected re-decoration")
        succeeded.append(feature)

    monkeypatch.setattr(m, "_register_feature", flaky_register)

    diff = m._run_reevaluation(deployment="test-dep")

    # embed + escalation_sweep registered; lens_putback's failure
    # didn't take the others down.
    assert set(diff["registered_now"]) == {"embed", "escalation_sweep"}
    assert succeeded == ["embed", "escalation_sweep"]
