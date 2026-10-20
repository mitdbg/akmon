"""Per `reduce-agent-friction` req.friction.environment-truth.

Dogfood status labels DSN identity divergence instead of presenting
recovery hints against the wrong database, and MCP pool acquisition
fails fast naming the role, database, and probable cause.
"""

from __future__ import annotations

import time

import pytest

from okg.substrate.cli.dogfood import _status_payload
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import server as mcp_server


def _environment(identity: str) -> dict:
    return {
        "ok": True,
        "authority": {
            "target": {
                "identity": identity,
                "database": "okg_workspace_51b8fac220"
                if identity == "isolated"
                else "okg_bench_clone",
                "expected_database": "okg_workspace",
                "host": "127.0.0.1",
                "port": 55434,
            }
        },
    }


def _payload(identity: str) -> dict:
    return _status_payload(
        deployment="okg-workspace",
        environment=_environment(identity),
        live={"blockers": [], "warnings": []},
        resolved_dsn=True,
        include_details=True,
    )


def test_identity_mismatch_is_a_labeled_blocker() -> None:
    # The 2026-06-10 incident shape: status recommended a stale clone DSN.
    payload = _payload("mismatch")
    blocker = next(
        b for b in payload["blockers"] if b["id"] == "database.identity"
    )
    assert "mismatch" in blocker["message"]
    assert "okg_bench_clone" in blocker["message"]
    assert "okg_workspace" in blocker["message"]
    assert "do not trust recovery hints" in blocker["message"]
    assert payload["trust_level"] != "ready"


def test_identity_isolated_is_a_labeled_warning_not_blocker() -> None:
    payload = _payload("isolated")
    assert not any(
        b["id"] == "database.identity" for b in payload["blockers"]
    )
    warning = next(
        w for w in payload["warnings"] if w["id"] == "database.identity"
    )
    assert "ISOLATED" in warning["message"]
    assert "okg_workspace_51b8fac220" in warning["message"]


def test_identity_ok_adds_no_label() -> None:
    payload = _payload("ok")
    assert not any(
        b["id"] == "database.identity" for b in payload["blockers"]
    )
    assert not any(
        w.get("id") == "database.identity" for w in payload["warnings"]
    )


def test_identity_unresolved_is_a_labeled_blocker() -> None:
    payload = _payload("unresolved")
    blocker = next(
        b for b in payload["blockers"] if b["id"] == "database.identity"
    )
    assert "unresolved" in blocker["message"]


def test_healthy_but_wrong_database_is_never_stale_context() -> None:
    # A published generation + healthy MCP against the WRONG database must
    # be unavailable, not "usable as stale context".
    payload = _status_payload(
        deployment="okg-workspace",
        environment=_environment("mismatch"),
        live={
            "blockers": [],
            "warnings": [],
            "generation": {"status": "published"},
            "mcp": {"ok": True},
        },
        resolved_dsn=True,
        include_details=True,
    )
    assert payload["trust_level"] == "unavailable"


class _StubPoolCtx:
    # `pooled_connection` sizes the general-admission bound from the pool's
    # width, so a stub pool has to declare one.
    max_size = 8
    name = "stub-pool"

    def __init__(self, conn=None, raises=None):
        self._conn = conn
        self._raises = raises
        self.calls = 0

    def connection(self):
        from contextlib import contextmanager

        self.calls += 1
        if self._raises is not None:
            raise self._raises

        @contextmanager
        def _ctx():
            yield self._conn

        return _ctx()


def test_pool_connection_wrapper_yields_from_the_real_pool() -> None:
    # Regression pin for the adversarial-review P0: the wrapper recursed
    # into itself instead of touching pool.connection(), silently breaking
    # freshness enrichment on every read envelope.
    sentinel = object()
    stub = _StubPoolCtx(conn=sentinel)
    with mcp_server._pool_connection(stub) as conn:
        assert conn is sentinel
    assert stub.calls == 1


def test_pool_connection_wrapper_converts_pool_timeout() -> None:
    from psycopg_pool import PoolTimeout

    stub = _StubPoolCtx(raises=PoolTimeout("wait"))
    stub._okg_role = "okg_mcp"
    stub._okg_database = "okg_workspace"
    stub._okg_timeout = 5.0
    with pytest.raises(SubstrateError) as exc:
        with mcp_server._pool_connection(stub):
            pass
    assert "okg_mcp" in exc.value.message


def test_build_pool_names_the_actual_conninfo_user() -> None:
    pool = mcp_server._build_pool(
        conninfo="postgresql://custom_ro@127.0.0.1:9/okg_workspace",
        role="okg_mcp",
        min_size=0,
        max_size=1,
        open=False,
        name="okg-mcp-user-test",
    )
    try:
        assert pool._okg_role == "custom_ro"
    finally:
        pool.close()


def test_pool_acquire_error_names_role_database_and_wedge() -> None:
    pool = type("FakePool", (), {})()
    pool._okg_role = "okg_mcp"
    pool._okg_database = "okg_workspace"
    pool._okg_timeout = 5.0
    err = mcp_server._pool_acquire_error(pool, TimeoutError("pool wait"))
    assert "okg_mcp" in err.message
    assert "okg_workspace" in err.message
    assert "5" in err.message
    assert "stale-password wedge" in err.fix_hint
    assert err.data["role"] == "okg_mcp"


def test_build_pool_carries_fail_fast_configuration(monkeypatch) -> None:
    monkeypatch.setenv("OKG_MCP_POOL_TIMEOUT_SECONDS", "2")
    pool = mcp_server._build_pool(
        conninfo="postgresql://okg_mcp@127.0.0.1:9/okg_workspace",
        role="okg_mcp",
        min_size=0,
        max_size=1,
        open=False,
        name="okg-mcp-test",
    )
    try:
        assert pool.timeout == 2.0
        assert pool.kwargs["connect_timeout"] == 2
        assert pool._okg_role == "okg_mcp"
        assert pool._okg_database == "okg_workspace"
    finally:
        pool.close()


def test_unconnectable_pool_fails_fast_naming_the_role(monkeypatch) -> None:
    # Live-shape check: an unconnectable server (closed discard port) must
    # fail acquisition within the configured window, not psycopg_pool's
    # blind 30s default, and the error must name role + database.
    monkeypatch.setenv("OKG_MCP_POOL_TIMEOUT_SECONDS", "1.5")
    pool = mcp_server._build_pool(
        conninfo="postgresql://okg_mcp:pw@127.0.0.1:9/okg_workspace",
        role="okg_mcp",
        min_size=0,
        max_size=1,
        open=True,
        name="okg-mcp-test-live",
    )
    started = time.monotonic()
    try:
        with pytest.raises(SubstrateError) as exc:
            mcp_server._pool_getconn(pool)
        elapsed = time.monotonic() - started
        assert elapsed < 10, f"acquisition took {elapsed:.1f}s — not fail-fast"
        assert "okg_mcp" in exc.value.message
        assert "okg_workspace" in exc.value.message
    finally:
        pool.close()
