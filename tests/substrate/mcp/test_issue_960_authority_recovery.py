"""Regression coverage for issue #960.

The read-boundary tests exercise actual MCP tool entry points so an
unevaluated Nomos check cannot regress into a policy-denial error. The shared
pool tests cover both constructor wiring and recovery after pid-scoped backend
termination.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import psycopg
import pytest

pytest.importorskip("mcp")

from psycopg_pool import ConnectionPool, PoolTimeout

from okg.substrate.errors import MCPPoolUnavailable, SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


_CLAIM = {
    "provider_id": "cern-sso",
    "verification_status": "valid",
    "issuer": "https://auth.cern.ch/auth/realms/cern",
    "claims": {
        "sub": "cern-user-960",
        "aud": "akmon-cms",
        "groups": ["cms-comp-ops"],
        "exp": "2026-05-25T13:00:00Z",
    },
}


def _close_server_pools(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


def _raise(exc: Exception):
    def _raiser(*_args, **_kwargs):
        raise exc

    return _raiser


def _publish_fixture_graph(dsn: str) -> dict:
    before_store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        before_checkout = before_store.checkout()
        before_nodes = before_checkout.query(
            "SELECT count(*) AS n FROM okg.graph_nodes", {},
        )[0]["n"]
        before_edges = before_checkout.query(
            "SELECT count(*) AS n FROM okg.graph_edges", {},
        )[0]["n"]
    finally:
        before_store.close()
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id="node:issue-960",
                    subtype="page",
                    attrs={"text": "issue 960 read boundary"},
                    source_record_id={"id": "node:issue-960"},
                    source_revision={"v": 1},
                )
            ],
            source="issue-960-fixture",
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    assert result["phase_b_blocked"] is False
    assert result["nodes_added"] + result["nodes_updated"] >= 1
    assert result["node_count"] >= 1
    after_store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        after_checkout = after_store.checkout_generation(result["generation_id"])
        after_nodes = after_checkout.query(
            "SELECT count(*) AS n FROM okg.graph_nodes", {},
        )[0]["n"]
        after_edges = after_checkout.query(
            "SELECT count(*) AS n FROM okg.graph_edges", {},
        )[0]["n"]
    finally:
        after_store.close()
    assert after_nodes > before_nodes
    assert after_edges >= before_edges
    return result


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("failure", "expected_code", "nested_code"),
    [
        (
            psycopg.OperationalError("AdminShutdown: database restarted"),
            "authority_unavailable",
            "nomos_authority_unavailable",
        ),
        (
            MCPPoolUnavailable("Nomos authority pool exhausted"),
            "authority_unavailable",
            "nomos_authority_unavailable",
        ),
        (
            ValueError("identity evaluator bug"),
            "authority_evaluation_failed",
            "nomos_authority_evaluation_failed",
        ),
    ],
)
def test_read_identity_evaluation_failure_is_not_identity_denial(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected_code: str,
    nested_code: str,
) -> None:
    """A claim evaluator failure is observable as no verdict, not a deny."""
    import okg.substrate.nomos as nomos_module

    _publish_fixture_graph(clean_runtime)
    monkeypatch.setattr(
        nomos_module,
        "deployment_requires_nomos_runtime",
        lambda _deployment: True,
    )
    monkeypatch.setattr(
        nomos_module,
        "evaluate_identity_claim_from_postgres",
        _raise(failure),
    )
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )
    try:
        with pytest.raises(SubstrateError) as exc_info:
            mcp._tool_manager._tools["inspect"].fn(
                target={"kind": "graph"},
                authority={"nomos_identity_claim": _CLAIM},
            )
    finally:
        _close_server_pools(mcp)

    err = exc_info.value
    assert err.code == expected_code
    envelope = err.to_json()["error"]
    assert envelope["code"] == expected_code
    assert envelope["data"]["surface"] == "mcp.tool.call"
    evidence = envelope["data"]["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == nested_code
    assert str(failure) in evidence["nomos"]["message"]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("failure", "expected_code", "nested_code"),
    [
        (
            psycopg.OperationalError("AdminShutdown: database restarted"),
            "authority_unavailable",
            "nomos_authority_unavailable",
        ),
        (
            ValueError("graph-read identity evaluator bug"),
            "authority_evaluation_failed",
            "nomos_authority_evaluation_failed",
        ),
    ],
)
def test_read_identity_graph_boundary_failure_uses_read_surface(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected_code: str,
    nested_code: str,
) -> None:
    """A claim failure at graph.read.mcp is not mislabeled as a tool deny."""
    import okg.substrate.nomos as nomos_module

    _publish_fixture_graph(clean_runtime)

    def _evaluate(*_args, **kwargs):
        if kwargs["action"] == "graph.read.mcp":
            raise failure
        return SimpleNamespace(
            decision=SimpleNamespace(decision_id="decision:issue-960", effect="allow"),
            resolved_subject="subject:cern-user-960",
        )

    monkeypatch.setattr(
        nomos_module,
        "deployment_requires_nomos_runtime",
        lambda _deployment: True,
    )
    monkeypatch.setattr(
        nomos_module,
        "evaluate_identity_claim_from_postgres",
        _evaluate,
    )
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError) as exc_info:
            mcp._tool_manager._tools["inspect"].fn(
                target={"kind": "graph"},
                authority={"nomos_identity_claim": _CLAIM},
            )
    finally:
        _close_server_pools(mcp)

    err = exc_info.value
    assert err.code == expected_code
    envelope = err.to_json()["error"]
    assert envelope["code"] == expected_code
    assert envelope["data"]["surface"] == "graph.read.mcp"
    evidence = envelope["data"]["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == nested_code
    assert str(failure) in evidence["nomos"]["message"]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("failure", "expected_code", "nested_code"),
    [
        (
            psycopg.InterfaceError("connection closed during policy read"),
            "authority_unavailable",
            "nomos_authority_unavailable",
        ),
        (
            RuntimeError("active policy evaluator bug"),
            "authority_evaluation_failed",
            "nomos_authority_evaluation_failed",
        ),
    ],
)
def test_read_active_policy_evaluation_failure_uses_same_envelope(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected_code: str,
    nested_code: str,
) -> None:
    """The no-claim active-policy branch has the same failure taxonomy."""
    import okg.substrate.nomos as nomos_module

    _publish_fixture_graph(clean_runtime)
    monkeypatch.setattr(
        nomos_module,
        "deployment_requires_nomos_runtime",
        lambda _deployment: True,
    )
    monkeypatch.setattr(
        nomos_module,
        "evaluate_runtime_boundary_with_active_policy",
        _raise(failure),
    )
    monkeypatch.setenv("OKG_MCP_NOMOS_SUBJECT", "subject:mcp_client")
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )
    try:
        with pytest.raises(SubstrateError) as exc_info:
            mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        _close_server_pools(mcp)

    err = exc_info.value
    assert err.code == expected_code
    envelope = err.to_json()["error"]
    assert envelope["code"] == expected_code
    assert envelope["data"]["surface"] == "mcp.tool.call"
    evidence = envelope["data"]["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == nested_code
    assert str(failure) in evidence["nomos"]["message"]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("failure", "expected_code", "nested_code"),
    [
        (
            psycopg.InterfaceError("connection closed during graph-read policy"),
            "authority_unavailable",
            "nomos_authority_unavailable",
        ),
        (
            RuntimeError("graph-read active policy evaluator bug"),
            "authority_evaluation_failed",
            "nomos_authority_evaluation_failed",
        ),
    ],
)
def test_read_active_policy_graph_boundary_failure_uses_read_surface(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected_code: str,
    nested_code: str,
) -> None:
    """The active-policy graph-read boundary uses the same envelope."""
    import okg.substrate.nomos as nomos_module

    _publish_fixture_graph(clean_runtime)

    def _evaluate(*_args, **kwargs):
        if kwargs["request"].surface == "graph.read.mcp":
            raise failure
        return SimpleNamespace(
            decision=SimpleNamespace(decision_id="decision:issue-960", effect="allow"),
            allowed=True,
        )

    monkeypatch.setattr(
        nomos_module,
        "deployment_requires_nomos_runtime",
        lambda _deployment: True,
    )
    monkeypatch.setattr(
        nomos_module,
        "evaluate_runtime_boundary_with_active_policy",
        _evaluate,
    )
    monkeypatch.setenv("OKG_MCP_NOMOS_SUBJECT", "subject:mcp_client")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError) as exc_info:
            mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        _close_server_pools(mcp)

    err = exc_info.value
    assert err.code == expected_code
    envelope = err.to_json()["error"]
    assert envelope["code"] == expected_code
    assert envelope["data"]["surface"] == "graph.read.mcp"
    evidence = envelope["data"]["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == nested_code
    assert str(failure) in evidence["nomos"]["message"]


def test_pool_exhaustion_before_authority_is_not_a_policy_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pool acquisition error remains the dedicated infrastructure code."""
    import okg.substrate.mcp.server as server_module

    class _ExhaustedPool:
        closed = False

        @staticmethod
        def check_connection(_conn) -> None:
            return None

        def __init__(self, conninfo: str, **_kwargs) -> None:
            del conninfo
            self._okg_role = "okg_mcp"
            self._okg_database = "issue_960"
            self._okg_timeout = 0.1

        def wait(self, timeout: float) -> None:
            del timeout

        def getconn(self):
            raise PoolTimeout("pool exhausted")

        def putconn(self, _conn) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(server_module, "ConnectionPool", _ExhaustedPool)
    # The pool is fake, so the start-time capability probe (#3151) is too:
    # this test is about the first CALL meeting an exhausted pool.
    from okg.substrate.mcp.capability_probe import CAPABILITY_FIXTURES

    monkeypatch.setattr(
        server_module,
        "probe_capabilities_from_pool",
        lambda *_args, **_kwargs: CAPABILITY_FIXTURES["none"](),
    )
    mcp = build_server(
        dsn="postgres://postgres@127.0.0.1/issue_960",
        deployment_name="bootstrap",
    )
    try:
        with pytest.raises(MCPPoolUnavailable) as exc_info:
            mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        _close_server_pools(mcp)

    assert exc_info.value.code == "mcp_pool_unavailable"


def test_shared_pool_configures_checkout_health_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_pool wires psycopg_pool's connection liveness probe."""
    import okg.substrate.db.pool as pool_module

    class _FakePool:
        closed = False

        @staticmethod
        def check_connection(_conn) -> None:
            return None

        def __init__(self, conninfo: str, **kwargs) -> None:
            del conninfo
            self.kwargs = kwargs

        def wait(self, timeout: float) -> None:
            del timeout

        def close(self) -> None:
            return None

    monkeypatch.setattr(pool_module, "ConnectionPool", _FakePool)
    pool_module.close_all()
    try:
        pool = pool_module.get_pool("postgres://unused", max_size=2)
        assert pool.kwargs["check"] is _FakePool.check_connection
    finally:
        pool_module.close_all()


def _terminate_backend_and_wait(dsn: str, pid: int) -> None:
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            row = admin.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = %s",
                (pid,),
            ).fetchone()
            if row is not None and row[0] == 0:
                return
            time.sleep(0.05)
    raise AssertionError(f"backend {pid} still alive after terminate")


@pytest.mark.postgres
def test_shared_pool_replaces_connections_severed_by_restart(
    okg_migrated: str,
) -> None:
    """The process-shared pool does not hand out idle dead connections."""
    import okg.substrate.db.pool as pool_module

    pool_module.close_all()
    pool = pool_module.get_pool(
        okg_migrated,
        max_size=2,
        application_name="issue-960-heal-test",
    )
    try:
        pool.wait(timeout=10)
        checked_out = [pool.getconn(), pool.getconn()]
        old_pids = {conn.info.backend_pid for conn in checked_out}
        for conn in checked_out:
            pool.putconn(conn)
        for pid in old_pids:
            _terminate_backend_and_wait(okg_migrated, pid)

        with pool.connection() as conn:
            row = conn.execute("SELECT 1").fetchone()
            new_pid = conn.info.backend_pid
        assert row == (1,)
        assert new_pid not in old_pids
    finally:
        pool_module.close_all()


@pytest.mark.postgres
def test_shared_pool_replacement_failure_is_bounded_and_fail_closed(
    okg_migrated: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead idle connection with no reachable replacement fails closed."""
    pool = ConnectionPool(
        conninfo=okg_migrated,
        min_size=1,
        max_size=1,
        timeout=1.0,
        kwargs={"connect_timeout": 1, "autocommit": True},
        check=ConnectionPool.check_connection,
        open=True,
        name="issue-960-fail-closed",
    )
    pool.wait(timeout=10.0)
    with pool.connection() as conn:
        old_pid = conn.info.backend_pid

    def _fail_connect(_timeout=None):
        raise psycopg.OperationalError("replacement database unavailable")

    monkeypatch.setattr(pool, "_connect", _fail_connect)
    _terminate_backend_and_wait(okg_migrated, old_pid)
    started = time.monotonic()
    try:
        with pytest.raises((PoolTimeout, psycopg.Error)):
            with pool.connection():
                pytest.fail("unreachable pool returned a connection")
    finally:
        pool.close()
    assert time.monotonic() - started < 10.0


@pytest.fixture(autouse=True)
def _trust_caller_identity_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """These rows evaluate caller-supplied identity claims on a single-user
    server, which PACT mcp-oidc-principal-policy makes an explicit launch-time
    opt-in; without it the claim is refused (nomos_identity_claim_untrusted)."""
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")
