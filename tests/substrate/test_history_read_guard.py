from __future__ import annotations

from pathlib import Path
import re
from typing import Any, get_type_hints

import pytest

from okg.substrate.history_read_guard import (
    _LEASE_REFRESH_SECONDS,
    _LEASE_SECONDS,
    GenerationReadLeaseBackend,
    GenerationReadLeaseState,
    LeasedGenerationSession,
    acquire_generation_read_lease,
)
from okg.substrate.errors import SubstrateError


REPO_ROOT = Path(__file__).resolve().parents[2]
_INTERVAL_UNITS = {"second": 1, "minute": 60, "hour": 3600}


class _Row(dict):
    """Row supporting the attribute and positional access `_value` expects."""

    def __getattr__(self, name: str) -> Any:
        return self[name]

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return dict.__getitem__(self, key)


class _Result:
    def __init__(self, row: _Row) -> None:
        self._row = row

    def fetchone(self) -> _Row:
        return self._row


class _FakeDB:
    def __init__(self, log: list[str], *, pin_relation_present: bool = True) -> None:
        self._log = log
        self._pin_relation_present = pin_relation_present

    def execute(self, sql: str, params: Any = None) -> _Result:
        flat = " ".join(sql.split())
        self._log.append(flat)
        # The generation is live: its tombstone lookup returns null.
        if "compacted_generation_record(?" in flat:
            return _Result(_Row(record=None))
        if "history_generation_pins" in flat and "to_regclass" in flat:
            return _Result(
                _Row(
                    relation=(
                        "okg.history_generation_pins"
                        if self._pin_relation_present
                        else None
                    )
                )
            )
        if "to_regclass" in flat:
            return _Result(_Row(relation="okg.tbl"))
        if "to_regprocedure" in flat:
            return _Result(_Row(procedure="okg.proc"))
        if "acquire_history_generation_read_lease" in flat:
            return _Result(_Row(acquired=True))
        return _Result(_Row(present=1))


class _FakeContext:
    def __init__(
        self, log: list[str], *, pin_relation_present: bool = True
    ) -> None:
        self.db = _FakeDB(log, pin_relation_present=pin_relation_present)
        self._log = log

    def _commit_autocommit(self) -> None:
        self._log.append("COMMIT")

    def _rollback_autocommit(self) -> None:
        self._log.append("ROLLBACK")


class _FakeSession:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    def query(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        self._log.append("USER_QUERY")
        return []

    def read_relations(self) -> tuple[dict[str, str], dict[str, Any]]:
        self._log.append("READ_RELATIONS")
        return {"okg.tbl": "SELECT 1"}, {"_chronos_branch_point": 1}


def _session(
    log: list[str], lease_backend: Any | None = None, *, pin_name: str = "read:test"
) -> LeasedGenerationSession:
    return LeasedGenerationSession(
        context=_FakeContext(log),
        session=_FakeSession(log),
        checkpoint_id="cp-1",
        deployment_name="d",
        branch="main",
        pin_name=pin_name,
        lease_backend=lease_backend,
    )


class _ScopedBackend:
    def __init__(self, log: list[str]) -> None:
        self.log = log
        self.state: GenerationReadLeaseState | None = None

    def acquire(self, context: Any, **scope: Any) -> str:
        self.log.append(f"SCOPED_ACQUIRE:{scope['checkpoint_id']}")
        self.state = GenerationReadLeaseState(
            pin_name="read:scoped",
            deployment_name=scope["deployment_name"],
            branch=scope["branch"],
            checkpoint_id=scope["checkpoint_id"],
            observed_at_epoch=100.0,
            expires_at_epoch=400.0,
        )
        return "read:scoped"

    def refresh(self, context: Any, **scope: Any) -> None:
        self.log.append(f"SCOPED_REFRESH:{scope['pin_name']}")
        assert self.state is not None
        self.state = GenerationReadLeaseState(
            pin_name=self.state.pin_name,
            deployment_name=self.state.deployment_name,
            branch=self.state.branch,
            checkpoint_id=self.state.checkpoint_id,
            observed_at_epoch=self.state.observed_at_epoch + 1.0,
            expires_at_epoch=self.state.expires_at_epoch + 1.0,
        )

    def release(self, context: Any, **scope: Any) -> None:
        self.log.append(f"SCOPED_RELEASE:{scope['pin_name']}")
        self.state = None

    def inspect(
        self, context: Any, *, pin_name: str
    ) -> GenerationReadLeaseState | None:
        self.log.append(f"SCOPED_INSPECT:{pin_name}")
        return self.state


def _sql_lease_seconds(path: Path) -> list[int]:
    """Every pin expiry interval written by the stored acquire function."""
    text = path.read_text(encoding="utf-8")
    seconds: list[int] = []
    for block in re.findall(
        r"INSERT INTO okg\.history_generation_pins.*?expires_at\s*=\s*EXCLUDED\.expires_at",
        text,
        flags=re.DOTALL,
    ):
        for amount, unit in re.findall(
            r"now\(\)\s*\+\s*interval\s*'(\d+)\s*(second|minute|hour)s?'", block
        ):
            seconds.append(int(amount) * _INTERVAL_UNITS[unit])
    return seconds


def test_refresh_window_is_shorter_than_the_database_lease() -> None:
    """The refresh window must stay well inside the SQL-side pin expiry.

    The Python `_LEASE_SECONDS` is used only by the fallback INSERT; the live
    path calls the stored function, which hardcodes its own interval. Nothing
    else couples the two, so shortening the SQL interval would silently open a
    window of unprotected reads with every other test still green.
    """
    sources = [
        REPO_ROOT / "src/okg/substrate/db/schema.sql",
        *sorted(
            (REPO_ROOT / "src/okg/substrate/db/migrations").glob(
                "*history_read_lease*.sql"
            )
        ),
    ]
    found: list[tuple[str, int]] = []
    for path in sources:
        if not path.is_file():
            continue
        found.extend((path.name, value) for value in _sql_lease_seconds(path))

    assert found, "no history_generation_pins expiry interval found in SQL"
    for name, sql_seconds in found:
        assert sql_seconds == _LEASE_SECONDS, (
            f"{name} pins expire after {sql_seconds}s but _LEASE_SECONDS is "
            f"{_LEASE_SECONDS}s; the fallback INSERT and the stored function "
            "must agree"
        )
        # Renewal has to happen with room to spare, not merely before expiry.
        assert _LEASE_REFRESH_SECONDS <= sql_seconds / 2, (
            f"{name} pins expire after {sql_seconds}s but the refresh window "
            f"is {_LEASE_REFRESH_SECONDS}s; leave at least half the lease as "
            "margin for a slow query or a suspended host"
        )


def test_held_lease_adds_no_per_query_round_trips() -> None:
    """A fresh lease must not be re-written before every query.

    Regression guard: renewing per query cost 7 round trips each -- a write,
    a tombstone read, a checkpoint probe, two catalog probes and a COMMIT --
    which regressed operator-UI cold-read latency past its SLO. The pin
    already protects the generation for the full lease, so restating it on
    every query buys nothing.
    """
    log: list[str] = []
    session = _session(log)
    log.clear()  # drop construction noise; measure steady-state queries only

    for _ in range(5):
        session.query("SELECT 1")

    assert log == ["USER_QUERY"] * 5, (
        f"expected zero lease overhead per query, got: {log}"
    )


def test_scoped_backend_replaces_only_lease_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supported checkout may narrow lease authority without losing validation."""
    log: list[str] = []
    context = _FakeContext(log)
    backend = _ScopedBackend(log)

    pin_name = acquire_generation_read_lease(
        context,
        checkpoint_id="cp-1",
        deployment_name="d",
        branch="main",
        lease_backend=backend,
    )
    assert pin_name == "read:scoped"
    assert "SCOPED_ACQUIRE:cp-1" in log
    assert not any("acquire_history_generation_read_lease" in row for row in log)
    assert any("_chronos_branch_interval_checkpoints" in row for row in log)

    session = _session(log, backend, pin_name="read:scoped")
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.monotonic",
        lambda: session._leased_at + _LEASE_REFRESH_SECONDS + 1,
    )
    session.query("SELECT 1")
    session.close()
    assert "SCOPED_REFRESH:read:scoped" in log
    assert "SCOPED_RELEASE:read:scoped" in log
    assert not any("release_history_generation_read_lease" in row for row in log)


def test_scoped_backend_is_not_skipped_when_generic_pin_relation_is_absent() -> None:
    log: list[str] = []
    backend = _ScopedBackend(log)

    pin_name = acquire_generation_read_lease(
        _FakeContext(log, pin_relation_present=False),
        checkpoint_id="cp-1",
        deployment_name="d",
        branch="main",
        lease_backend=backend,
    )

    assert pin_name == "read:scoped"
    assert "SCOPED_ACQUIRE:cp-1" in log
    assert not any(
        "to_regclass('okg.history_generation_pins')" in row for row in log
    )


@pytest.mark.parametrize("result", [None, "not-a-read-pin", 17])
def test_scoped_backend_invalid_pin_rolls_back_without_fallback(result: Any) -> None:
    log: list[str] = []

    class InvalidBackend(_ScopedBackend):
        def acquire(self, context: Any, **scope: Any) -> Any:
            self.log.append("INVALID_SCOPED_ACQUIRE")
            return result

    with pytest.raises(Exception, match="invalid pin name"):
        acquire_generation_read_lease(
            _FakeContext(log),
            checkpoint_id="cp-1",
            deployment_name="d",
            branch="main",
            lease_backend=InvalidBackend(log),
        )

    assert log[-1] == "ROLLBACK"
    assert not any("acquire_history_generation_read_lease" in row for row in log)
    assert not any("INSERT INTO okg.history_generation_pins" in row for row in log)


def test_scoped_backend_exception_rolls_back_without_fallback() -> None:
    log: list[str] = []

    class FailingBackend(_ScopedBackend):
        def acquire(self, context: Any, **scope: Any) -> str:
            self.log.append("FAILING_SCOPED_ACQUIRE")
            raise RuntimeError("scoped authority refused")

    with pytest.raises(RuntimeError, match="scoped authority refused"):
        acquire_generation_read_lease(
            _FakeContext(log),
            checkpoint_id="cp-1",
            deployment_name="d",
            branch="main",
            lease_backend=FailingBackend(log),
        )

    assert log[-1] == "ROLLBACK"
    assert not any("acquire_history_generation_read_lease" in row for row in log)
    assert not any("INSERT INTO okg.history_generation_pins" in row for row in log)


@pytest.mark.parametrize(
    ("state", "message"),
    [
        (None, "did not persist"),
        (
            GenerationReadLeaseState(
                pin_name="read:scoped",
                deployment_name="d",
                branch="other",
                checkpoint_id="cp-1",
                observed_at_epoch=100.0,
                expires_at_epoch=400.0,
            ),
            "wrong lease target",
        ),
        (
            GenerationReadLeaseState(
                pin_name="read:scoped",
                deployment_name="d",
                branch="main",
                checkpoint_id="cp-1",
                observed_at_epoch=400.0,
                expires_at_epoch=400.0,
            ),
            "expired lease",
        ),
    ],
)
def test_scoped_backend_acquire_requires_exact_active_database_state(
    state: GenerationReadLeaseState | None,
    message: str,
) -> None:
    log: list[str] = []

    class UnattestedBackend(_ScopedBackend):
        def acquire(self, context: Any, **scope: Any) -> str:
            self.log.append("UNATTESTED_SCOPED_ACQUIRE")
            self.state = state
            return "read:scoped"

    with pytest.raises(Exception, match=message):
        acquire_generation_read_lease(
            _FakeContext(log),
            checkpoint_id="cp-1",
            deployment_name="d",
            branch="main",
            lease_backend=UnattestedBackend(log),
        )

    assert log[-1] == "ROLLBACK"
    assert not any("acquire_history_generation_read_lease" in row for row in log)


def test_scoped_backend_noop_refresh_fails_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log: list[str] = []

    class NoopRefreshBackend(_ScopedBackend):
        def refresh(self, context: Any, **scope: Any) -> None:
            self.log.append(f"NOOP_REFRESH:{scope['pin_name']}")

    backend = NoopRefreshBackend(log)
    backend.acquire(
        _FakeContext(log), checkpoint_id="cp-1", deployment_name="d", branch="main"
    )
    session = _session(log, backend, pin_name="read:scoped")
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.monotonic",
        lambda: session._leased_at + _LEASE_REFRESH_SECONDS + 1,
    )

    with pytest.raises(Exception, match="did not extend"):
        session.query("SELECT 1")

    assert log[-1] == "ROLLBACK"
    assert "USER_QUERY" not in log


def test_scoped_backend_noop_release_fails_and_rolls_back() -> None:
    log: list[str] = []

    class NoopReleaseBackend(_ScopedBackend):
        def release(self, context: Any, **scope: Any) -> None:
            self.log.append(f"NOOP_RELEASE:{scope['pin_name']}")

    backend = NoopReleaseBackend(log)
    backend.acquire(
        _FakeContext(log), checkpoint_id="cp-1", deployment_name="d", branch="main"
    )

    with pytest.raises(Exception, match="did not release"):
        _session(log, backend, pin_name="read:scoped").close()

    assert log[-1] == "ROLLBACK"


def test_scoped_backend_absent_release_fails_before_noop() -> None:
    log: list[str] = []
    backend = _ScopedBackend(log)

    with pytest.raises(Exception, match="did not persist"):
        _session(log, backend, pin_name="read:scoped").close()

    assert not any(row.startswith("SCOPED_RELEASE") for row in log)
    assert log[-1] == "ROLLBACK"


def test_public_store_apis_expose_the_scoped_backend_protocol() -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    expected = GenerationReadLeaseBackend | None
    for method in (
        ChronosGraphStore.__init__,
        ChronosGraphStore.connect,
        ChronosGraphStore.from_connection,
    ):
        assert get_type_hints(method)["history_read_lease_backend"] == expected


def test_lease_is_renewed_once_the_refresh_window_elapses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The protection invariant still holds: the pin is renewed before expiry."""
    log: list[str] = []
    session = _session(log)

    clock = {"now": session._leased_at}
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.monotonic", lambda: clock["now"]
    )

    log.clear()
    session.query("SELECT 1")
    assert log == ["USER_QUERY"], "should not renew inside the refresh window"

    clock["now"] += _LEASE_REFRESH_SECONDS + 1
    log.clear()
    session.query("SELECT 1")

    assert any("acquire_history_generation_read_lease" in c for c in log), (
        f"lease must be renewed once the window elapses, got: {log}"
    )
    assert "COMMIT" in log
    assert log[-1] == "USER_QUERY"


def test_read_relations_come_from_the_inner_session_after_one_renewal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_sql's graph path renews once, then reads relations read-only (#2899).

    The renewal is a write, so it happens in `session_for_read_only_query`,
    before the read-only boundary. The relations are then read from the inner
    session, which writes nothing. The proxy refuses `read_relations` itself:
    renewing there would write under read-only, and passing it through would
    read on an unrenewed lease.
    """
    log: list[str] = []
    session = _session(log)
    with pytest.raises(SubstrateError, match="session_for_read_only_query"):
        session.read_relations()
    assert "READ_RELATIONS" not in log

    clock = {"now": session._leased_at}
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.monotonic", lambda: clock["now"]
    )
    clock["now"] += _LEASE_REFRESH_SECONDS + 1
    log.clear()
    inner = session.session_for_read_only_query()
    renewals = [c for c in log if "acquire_history_generation_read_lease" in c]
    assert renewals, f"lease must be renewed before the read, got: {log}"
    log.clear()
    relations, params = inner.read_relations()

    assert relations == {"okg.tbl": "SELECT 1"}
    assert params == {"_chronos_branch_point": 1}
    assert log == ["READ_RELATIONS"], (
        f"reading relations after the renewal must not write, got: {log}"
    )


def test_suspended_host_renews_even_though_monotonic_froze(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A suspended host must not treat a long-expired pin as fresh.

    `time.monotonic()` does not advance across suspend, but `expires_at` is
    database real time. Trusting monotonic alone would let a query after a
    one-hour laptop sleep read a generation compaction had already retired,
    returning partial rows with no error instead of raising.
    """
    log: list[str] = []
    session = _session(log)

    # Monotonic barely moves (host was suspended); wall clock jumps an hour.
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.monotonic",
        lambda: session._leased_at + 5.0,
    )
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.time",
        lambda: session._leased_at_wall + 3600.0,
    )

    log.clear()
    session.query("SELECT 1")

    assert any("acquire_history_generation_read_lease" in c for c in log), (
        f"a suspended host must renew its expired pin, got: {log}"
    )


def test_query_after_close_reacquires_the_released_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """close() deletes the pin, so a later query must not assume it holds one."""
    log: list[str] = []
    session = _session(log)
    session.close()

    log.clear()
    session.query("SELECT 1")

    assert any("acquire_history_generation_read_lease" in c for c in log), (
        f"a query after close must re-acquire its pin, got: {log}"
    )


def test_failed_renewal_does_not_extend_the_assumed_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A renewal that raises must be retried, not treated as a success."""
    log: list[str] = []
    session = _session(log)

    clock = {"now": session._leased_at}
    monkeypatch.setattr(
        "okg.substrate.history_read_guard.time.monotonic", lambda: clock["now"]
    )
    clock["now"] += _LEASE_REFRESH_SECONDS + 1

    boom = RuntimeError("lease write failed")

    def _fail(*_args: Any, **_kwargs: Any) -> None:
        raise boom

    monkeypatch.setattr("okg.substrate.history_read_guard._write_lease", _fail)
    with pytest.raises(RuntimeError):
        session.query("SELECT 1")

    # The clock was not advanced, so the next query retries the renewal
    # rather than assuming a lease it never obtained.
    renewed: list[str] = []
    monkeypatch.setattr(
        "okg.substrate.history_read_guard._write_lease",
        lambda *a, **k: renewed.append("renewed"),
    )
    session.query("SELECT 1")
    assert renewed == ["renewed"]
