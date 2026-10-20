from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest


def test_client_list_schedules_reads_live_dbos_rows(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    class FakeClient:
        def list_schedules(self):
            return [
                {
                    "schedule_name": "okg_source_poll_pact",
                    "workflow_name": "okg_source_poll_pact_enqueue",
                    "schedule": "*/5 * * * *",
                    "status": "ACTIVE",
                },
                {
                    "schedule_name": "okg_substrate_doctor",
                    "workflow_name": "doctor_workflow",
                    "schedule": "*/5 * * * *",
                    "status": "PAUSED",
                },
            ]

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())

    entries = admin.client_list_schedules(
        deployment="okg-workspace",
        dsn="postgres://example",
        runtime=SimpleNamespace(schedules={}),
    )

    assert [(e.schedule_id, e.workflow_name, e.paused) for e in entries] == [
        (
            "okg_source_poll_pact",
            "okg_source_poll_pact_enqueue",
            False,
        ),
        ("okg_substrate_doctor", "doctor_workflow", True),
    ]


def test_client_list_schedules_falls_back_to_manifest(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    class BrokenClient:
        def list_schedules(self):
            raise RuntimeError("dbos unavailable")

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: BrokenClient())

    entries = admin.client_list_schedules(
        deployment="okg-workspace",
        dsn="postgres://example",
        runtime=SimpleNamespace(
            schedules={"okg_publisher_cycle": "*/2 * * * *"},
        ),
    )

    assert [(e.schedule_id, e.cron) for e in entries] == [
        ("okg_substrate_doctor", "*/5 * * * *"),
        ("okg_publisher_cycle", "*/2 * * * *"),
    ]


def test_client_trigger_schedule_uses_dbos_client(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    calls: list[str] = []

    class FakeClient:
        def trigger_schedule(self, schedule_id: str):
            calls.append(f"trigger:{schedule_id}")
            return SimpleNamespace(workflow_id="wf-triggered")

    @contextmanager
    def permit(**kwargs):
        calls.append(f"permit:{kwargs['deployment']}")
        yield
        calls.append("permit:released")

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())
    monkeypatch.setattr(admin, "promotion_enqueue_permit", permit)

    workflow_id = admin.client_trigger_schedule(
        deployment="okg-workspace",
        dsn="postgres://example",
        runtime=SimpleNamespace(schedules={}),
        schedule_id="okg_source_poll_pact",
    )

    assert workflow_id == "wf-triggered"
    assert calls == [
        "permit:okg-workspace",
        "trigger:okg_source_poll_pact",
        "permit:released",
    ]


def test_client_pause_schedule_uses_dbos_client(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    calls: list[str] = []
    schedule = {
        "schedule_name": "okg_publisher_cycle",
        "status": "ACTIVE",
    }

    class FakeClient:
        def get_schedule(self, schedule_id: str):
            calls.append(f"get:{schedule_id}")
            return dict(schedule)

        def pause_schedule(self, schedule_id: str) -> None:
            calls.append(f"pause:{schedule_id}")
            schedule["status"] = "PAUSED"

        def list_workflows(self, **kwargs):
            calls.append(f"list:{kwargs['schedule_name']}")
            return []

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())

    result = admin.client_pause_schedule(
        deployment="okg-workspace",
        dsn="postgres://example",
        schedule_id="okg_publisher_cycle",
        min_settle_seconds=0,
    )

    assert result.persisted_paused is True
    assert result.scheduler_fenced is True
    assert calls == [
        "get:okg_publisher_cycle",
        "pause:okg_publisher_cycle",
        "get:okg_publisher_cycle",
        "list:okg_publisher_cycle",
        "get:okg_publisher_cycle",
    ]


def test_client_pause_schedule_rejects_missing_schedule(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    class FakeClient:
        def get_schedule(self, schedule_id: str):
            return None

        def pause_schedule(self, schedule_id: str) -> None:
            raise AssertionError("missing schedule must not be mutated")

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())

    with pytest.raises(RuntimeError, match="missing DBOS schedule"):
        admin.client_pause_schedule(
            deployment="okg-workspace",
            dsn="postgres://example",
            schedule_id="missing",
            min_settle_seconds=0,
        )


def test_client_pause_schedule_waits_through_scheduler_poll_window(
    monkeypatch,
) -> None:
    from okg.substrate.runtime import admin

    schedule = {
        "schedule_name": "okg_publisher_cycle",
        "status": "ACTIVE",
    }
    clock = {"now": 0.0}
    list_calls = 0

    class FakeClient:
        def get_schedule(self, schedule_id: str):
            return dict(schedule)

        def pause_schedule(self, schedule_id: str) -> None:
            schedule["status"] = "PAUSED"

        def list_workflows(self, **kwargs):
            nonlocal list_calls
            list_calls += 1
            if list_calls == 1:
                return [SimpleNamespace(status="PENDING")]
            return []

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())
    monkeypatch.setattr(admin.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        admin.time,
        "sleep",
        lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )

    result = admin.client_pause_schedule(
        deployment="okg-workspace",
        dsn="postgres://example",
        schedule_id="okg_publisher_cycle",
        max_wait_seconds=10,
        min_settle_seconds=3,
        poll_interval_seconds=1,
    )

    assert result.scheduler_fenced is True
    assert result.elapsed_seconds == 3
    assert list_calls == 4


def test_client_resume_schedule_verifies_active_state(monkeypatch) -> None:
    from okg.substrate.runtime import admin

    schedule = {
        "schedule_name": "okg_publisher_cycle",
        "status": "PAUSED",
    }

    class FakeClient:
        def get_schedule(self, schedule_id: str):
            return dict(schedule)

        def resume_schedule(self, schedule_id: str) -> None:
            schedule["status"] = "ACTIVE"

    monkeypatch.setattr(admin, "_build_client", lambda *, dsn: FakeClient())

    admin.client_resume_schedule(
        deployment="okg-workspace",
        dsn="postgres://example",
        schedule_id="okg_publisher_cycle",
    )

    assert schedule["status"] == "ACTIVE"


class _EventClient:
    """A DBOS client double that records its reads and whether it was closed."""

    def __init__(self, dsn: str, fail: bool = False) -> None:
        self.dsn = dsn
        self.fail = fail
        self.reads: list[tuple[str, str]] = []
        self.destroyed = False

    def get_event(self, *, workflow_id: str, key: str, timeout_seconds: float):
        self.reads.append((workflow_id, key))
        if self.fail:
            raise RuntimeError("system database went away")
        return {"workflow_id": workflow_id, "key": key}

    def destroy(self) -> None:
        self.destroyed = True


@pytest.fixture()
def event_clients(monkeypatch):
    """Count DBOS client builds; start every test with no shared client."""
    from okg.substrate.runtime import admin

    built: list[_EventClient] = []

    def _build(*, dsn: str) -> _EventClient:
        client = _EventClient(dsn, fail=getattr(_build, "fail_next", False))
        _build.fail_next = False
        built.append(client)
        return client

    monkeypatch.setattr(admin, "_build_client", _build)
    monkeypatch.setattr(admin, "_EVENT_CLIENTS", {})
    return admin, built, _build


def test_event_reads_on_one_dsn_build_one_client(event_clients) -> None:
    """Issue #2547: every console Health read used to build a new client."""
    admin, built, _ = event_clients

    for workflow_id in ("wf-1", "wf-2", "wf-3"):
        assert admin.client_get_event(
            dsn="postgres://a", workflow_id=workflow_id, key="doctor_verdict"
        ) == {"workflow_id": workflow_id, "key": "doctor_verdict"}

    assert len(built) == 1
    assert [r[0] for r in built[0].reads] == ["wf-1", "wf-2", "wf-3"]


def test_a_second_dsn_gets_its_own_client(event_clients) -> None:
    admin, built, _ = event_clients

    admin.client_get_event(dsn="postgres://a", workflow_id="wf", key="k")
    admin.client_get_event(dsn="postgres://b", workflow_id="wf", key="k")

    assert [client.dsn for client in built] == ["postgres://a", "postgres://b"]


def test_a_failed_read_raises_and_drops_the_shared_client(event_clients) -> None:
    admin, built, build = event_clients
    build.fail_next = True

    with pytest.raises(RuntimeError, match="system database went away"):
        admin.client_get_event(dsn="postgres://a", workflow_id="wf", key="k")
    assert built[0].destroyed is True

    # The next read builds a fresh client instead of reusing the broken one.
    assert admin.client_get_event(
        dsn="postgres://a", workflow_id="wf", key="k"
    ) == {"workflow_id": "wf", "key": "k"}
    assert len(built) == 2
    assert built[1].destroyed is False


def test_a_client_built_in_a_race_is_closed_and_the_winner_kept(event_clients) -> None:
    """The client is built outside the lock; if another thread stored one for
    the same DSN meanwhile, that one is used and the extra one is closed."""
    admin, built, build = event_clients
    winner = _EventClient("postgres://a")

    def _build_while_another_thread_wins(*, dsn: str) -> _EventClient:
        admin._EVENT_CLIENTS[(dsn, _build_while_another_thread_wins)] = winner
        return build(dsn=dsn)

    admin._build_client = _build_while_another_thread_wins
    admin.client_get_event(dsn="postgres://a", workflow_id="wf", key="k")

    assert winner.reads == [("wf", "k")]
    assert len(built) == 1 and built[0].destroyed is True


def test_the_client_is_not_built_under_the_shared_lock(event_clients) -> None:
    """A hung connect for one DSN must not block reads on another."""
    admin, built, build = event_clients
    held: list[bool] = []

    def _build_checking_lock(*, dsn: str) -> _EventClient:
        held.append(admin._EVENT_CLIENTS_LOCK.locked())
        return build(dsn=dsn)

    admin._build_client = _build_checking_lock
    admin.client_get_event(dsn="postgres://a", workflow_id="wf", key="k")

    assert held == [False]
