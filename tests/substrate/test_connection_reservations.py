"""Host-wide reservations: two live processes cannot over-subscribe.

`req.unify-connection-budget.boot-admission` tier 2. The per-process check asks
"does my demand fit what is free"; two workers booting together each see the
other's connections absent and both pass it. A reservation is a claim recorded
BEFORE any pool opens, which is what closes that race.

Runs against a real Postgres because the whole mechanism is an advisory lock
plus a table, and a mock would test neither.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time

import psycopg
import pytest

from okg.substrate.db import concurrency_budget as budget
from okg.substrate.db import capacity as capacity_mod
from okg.substrate.db.capacity import (
    ServerCapacity,
    release_reservation,
    reserve_process_demand,
)

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _clean(clean_runtime: str):
    budget.clear_overrides()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM okg.connection_reservations")
    try:
        yield
    finally:
        capacity_mod._release_all_process_reservations()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM okg.connection_reservations")
        budget.clear_overrides()


def _capacity(max_connections=40, reserved=3) -> ServerCapacity:
    return ServerCapacity(
        max_connections=max_connections,
        superuser_reserved=reserved,
        active_client_backends=0,
    )


def _claim(conn_dsn, name, demand, *, pid=None):
    """Insert a foreign process's claim directly, since a test cannot fork."""
    with psycopg.connect(conn_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.connection_reservations "
                "  (application_name, pid, declared_demand) "
                "VALUES (%s, %s, %s)",
                (name, pid if pid is not None else os.getpid() + 1000, demand),
            )


def test_a_reservation_is_recorded(clean_runtime: str) -> None:
    demand = reserve_process_demand(
        clean_runtime, application_name="okg-worker-a",
        capacity=_capacity(),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT application_name, declared_demand "
                "  FROM okg.connection_reservations"
            )
            rows = cur.fetchall()
    assert rows == [("okg-worker-a", demand["_total"])]


def test_reserving_twice_updates_rather_than_duplicates(clean_runtime: str) -> None:
    reserve_process_demand(
        clean_runtime, application_name="okg-worker-a", capacity=_capacity(),
    )
    reserve_process_demand(
        clean_runtime, application_name="okg-worker-a", capacity=_capacity(),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.connection_reservations")
            assert cur.fetchone()[0] == 1


def test_another_live_claim_can_refuse_this_process(clean_runtime: str) -> None:
    """The property the whole tier exists for: a second process is refused
    because of a claim it can see, not because connections are already open."""
    cap = _capacity(max_connections=20, reserved=3)  # usable 17
    _claim(clean_runtime, "okg-worker-other", 15)

    with pytest.raises(budget.BudgetError) as exc:
        reserve_process_demand(
            clean_runtime, application_name="okg-worker-mine", capacity=cap,
        )

    message = str(exc.value)
    assert "okg-worker-other" in message, "the refusal must name the holder"
    assert "Short by" in message
    assert "before opening any pool" in message


def test_refusal_leaves_no_partial_claim(clean_runtime: str) -> None:
    """A refused process must not leave its own row behind, or a retry sees
    itself as a competitor."""
    cap = _capacity(max_connections=20, reserved=3)
    _claim(clean_runtime, "okg-worker-other", 15)

    with pytest.raises(budget.BudgetError):
        reserve_process_demand(
            clean_runtime, application_name="okg-worker-mine", capacity=cap,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.connection_reservations "
                " WHERE application_name = 'okg-worker-mine'"
            )
            assert cur.fetchone()[0] == 0


def test_a_stale_claim_does_not_refuse_a_restart(clean_runtime: str) -> None:
    """A crashed worker must not hold its claim forever — otherwise the restart
    of that same worker is refused against its own corpse."""
    # Sized so the DEFAULT declaration fits once the stale claim is swept but
    # not while it stands: default demand is 19, so usable 34 minus a 15-wide
    # stale claim leaves 19 exactly.
    cap = _capacity(max_connections=37, reserved=3)  # usable 34
    _claim(clean_runtime, "okg-worker-crashed", 16)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.connection_reservations "
                "   SET heartbeat_at = now() - interval '1 hour'"
            )

    demand = reserve_process_demand(
        clean_runtime, application_name="okg-worker-mine", capacity=cap,
        stale_after_seconds=300,
    )

    assert demand["_total"] > 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.connection_reservations "
                " WHERE application_name = 'okg-worker-crashed'"
            )
            assert cur.fetchone()[0] == 0, "the stale claim was not swept"


def test_release_frees_the_claim(clean_runtime: str) -> None:
    reserve_process_demand(
        clean_runtime, application_name="okg-worker-a", capacity=_capacity(),
    )
    release_reservation(clean_runtime, application_name="okg-worker-a")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.connection_reservations")
            assert cur.fetchone()[0] == 0


def test_narrowing_the_declaration_makes_room(clean_runtime: str) -> None:
    """The refusal is about the declaration, not a hard wall: shrinking the
    widths lets the same process in alongside the same holder."""
    cap = _capacity(max_connections=20, reserved=3)  # usable 17
    _claim(clean_runtime, "okg-worker-other", 12)

    # Default demand is 19; 12 already held leaves 5, so the default is refused.
    with pytest.raises(budget.BudgetError):
        reserve_process_demand(
            clean_runtime, application_name="okg-worker-mine", capacity=cap,
        )

    # Narrowed to shared 2 + dbos 1 + chronos 2 + 1 = 5, which fits exactly.
    budget.set_override("db.shared_pool", 2)
    budget.set_override("runtime.dbos_pool", 1)
    budget.set_override("chronos.schema_index", 1)
    demand = reserve_process_demand(
        clean_runtime, application_name="okg-worker-mine", capacity=cap,
    )
    assert demand["_total"] <= cap.usable - 12


def _reservation_count(dsn: str, application_name: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.connection_reservations "
                "WHERE application_name = %s",
                (application_name,),
            )
            return int(cur.fetchone()[0])


def test_normal_process_exit_releases_and_replacement_starts(
    clean_runtime: str,
) -> None:
    application_name = "okg-clean-exit-child"
    mine = budget.connection_demand()["_total"]
    capacity = _capacity(max_connections=mine + 3, reserved=3)
    child = textwrap.dedent(
        """
        import sys
        from okg.substrate.db.capacity import ServerCapacity, reserve_process_demand

        reserve_process_demand(
            sys.argv[1],
            application_name=sys.argv[2],
            capacity=ServerCapacity(int(sys.argv[3]), 3, 0),
            heartbeat_interval_seconds=0.1,
            stale_after_seconds=0.5,
        )
        print("reserved", flush=True)
        sys.stdin.read(1)
        """
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child,
            clean_runtime,
            application_name,
            str(capacity.max_connections),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "reserved"
    assert _reservation_count(clean_runtime, application_name) == 1

    assert proc.stdin is not None
    proc.stdin.close()
    assert proc.wait(timeout=10) == 0, (
        proc.stderr.read() if proc.stderr is not None else "child failed"
    )
    assert _reservation_count(clean_runtime, application_name) == 0

    demand = reserve_process_demand(
        clean_runtime,
        application_name="okg-clean-exit-replacement",
        capacity=capacity,
        heartbeat_interval_seconds=None,
    )
    assert demand["_total"] == mine
    release_reservation(
        clean_runtime,
        application_name="okg-clean-exit-replacement",
    )


def test_live_reservation_remains_counted_past_stale_horizon(
    clean_runtime: str,
) -> None:
    mine = budget.connection_demand()["_total"]
    capacity = _capacity(max_connections=mine + 3, reserved=3)
    application_name = "okg-live-past-horizon"
    reserve_process_demand(
        clean_runtime,
        application_name=application_name,
        capacity=capacity,
        heartbeat_interval_seconds=0.1,
        stale_after_seconds=0.5,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT heartbeat_at FROM okg.connection_reservations "
                "WHERE application_name = %s",
                (application_name,),
            )
            first_heartbeat = cur.fetchone()[0]

    deadline = time.monotonic() + 3
    refreshed = False
    while time.monotonic() < deadline:
        time.sleep(0.05)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT heartbeat_at FROM okg.connection_reservations "
                    "WHERE application_name = %s",
                    (application_name,),
                )
                row = cur.fetchone()
        if row is not None and row[0] > first_heartbeat:
            refreshed = True
            break
    assert refreshed, "the live reservation heartbeat never advanced"

    time.sleep(0.55)
    with pytest.raises(budget.BudgetError, match=application_name):
        reserve_process_demand(
            clean_runtime,
            application_name="okg-live-past-horizon-competitor",
            capacity=capacity,
            stale_after_seconds=0.5,
            heartbeat_interval_seconds=None,
        )
    assert _reservation_count(clean_runtime, application_name) == 1
    release_reservation(clean_runtime, application_name=application_name)
    time.sleep(0.2)
    assert _reservation_count(clean_runtime, application_name) == 0


def test_owner_start_failure_deletes_inserted_row(
    clean_runtime: str,
    monkeypatch,
) -> None:
    class BrokenThread:
        daemon = True

        def start(self) -> None:
            raise RuntimeError("injected heartbeat owner start failure")

        def join(self, *, timeout: float) -> None:
            raise RuntimeError(f"not started ({timeout})")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(
        capacity_mod,
        "_new_reservation_heartbeat_thread",
        lambda _owner: BrokenThread(),
    )

    with pytest.raises(
        capacity_mod.ReservationLifecycleError,
        match="owner start failure",
    ):
        reserve_process_demand(
            clean_runtime,
            application_name="okg-owner-start-failure",
            capacity=_capacity(),
            heartbeat_interval_seconds=0.1,
            stale_after_seconds=0.5,
        )
    assert _reservation_count(clean_runtime, "okg-owner-start-failure") == 0


def test_release_wins_against_inflight_heartbeat(clean_runtime: str) -> None:
    application_name = "okg-release-heartbeat-race"
    reasons: list[str] = []
    reserve_process_demand(
        clean_runtime,
        application_name=application_name,
        capacity=_capacity(),
        heartbeat_interval_seconds=None,
    )
    owner = capacity_mod._ReservationOwner(
        dsn=clean_runtime,
        application_name=application_name,
        pid=os.getpid(),
        heartbeat_interval_seconds=0.1,
        stale_after_seconds=0.5,
        lease_lost_handler=reasons.append,
        stop=threading.Event(),
        last_success_monotonic=time.monotonic(),
    )
    update_started = threading.Event()
    allow_update = threading.Event()

    def delayed_update() -> None:
        update_started.set()
        allow_update.wait(timeout=5)
        capacity_mod._refresh_reservation(owner)

    owner.thread = threading.Thread(target=delayed_update, daemon=True)
    key = capacity_mod._reservation_owner_key(
        clean_runtime,
        application_name,
        owner.pid,
    )
    with capacity_mod._RESERVATION_OWNERS_LOCK:
        capacity_mod._RESERVATION_OWNERS[key] = owner
    owner.thread.start()
    assert update_started.wait(timeout=2)

    releaser = threading.Thread(
        target=release_reservation,
        args=(clean_runtime,),
        kwargs={"application_name": application_name},
    )
    releaser.start()
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if _reservation_count(clean_runtime, application_name) == 0:
            break
        time.sleep(0.05)
    assert _reservation_count(clean_runtime, application_name) == 0
    allow_update.set()
    releaser.join(timeout=3)
    owner.thread.join(timeout=3)

    assert not releaser.is_alive()
    assert not owner.thread.is_alive()
    assert reasons == []
    assert _reservation_count(clean_runtime, application_name) == 0
