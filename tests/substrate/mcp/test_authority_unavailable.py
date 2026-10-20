"""Issue #950: infrastructure failures during MCP write authorization
must not be dressed as policy denials, and the MCP pools must heal
after a database restart.

PACT: issue-950-authority-unavailable.

Two tiers:
- Hermetic unit tests (no Postgres) over the module-level classifier
  (`_authority_failure_evidence`), the shared evidence→code mapper
  (`_authority_refusal_code`, used by both record() and propose()), and
  the `_build_pool` checkout-check configuration. These run in per-PR CI.
- `postgres`-marked integration tests: record() end-to-end refusal
  codes, and a `_build_pool` pool surviving pid-scoped
  `pg_terminate_backend` of its own connection.

The rendered-deny counterpart (explicit Nomos deny still refuses with
`unauthorized_write`) is asserted end-to-end by
tests/substrate/mcp/test_operator_surface_integration.py::
test_record_iam_resolved_subject_overwrites_caller_provenance.
"""
from __future__ import annotations

import time

import psycopg
import pytest

pytest.importorskip("mcp")

from psycopg_pool import ConnectionPool

from okg.substrate.errors import MCPPoolUnavailable
from okg.substrate.mcp.server import (
    _authority_failure_evidence,
    _authority_evaluation_error,
    _authority_refusal_code,
    _build_pool,
    _pool_connection,
    build_server,
)


# ---------------------------------------------------------------------------
# Hermetic: failure classification (no verdict is ever claimed)
# ---------------------------------------------------------------------------


def test_operational_error_classified_as_infrastructure() -> None:
    evidence = _authority_failure_evidence(
        "record:observation",
        psycopg.OperationalError(
            "AdminShutdown: terminating connection due to administrator command"
        ),
    )
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_unavailable"
    assert "administrator command" in evidence["nomos"]["message"]


def test_interface_error_classified_as_infrastructure() -> None:
    evidence = _authority_failure_evidence(
        "record:observation", psycopg.InterfaceError("the connection is closed")
    )
    assert evidence["evaluated"] is False
    assert evidence["nomos"]["code"] == "nomos_authority_unavailable"


def test_pool_acquisition_failure_classified_as_infrastructure() -> None:
    evidence = _authority_failure_evidence(
        "record:observation",
        MCPPoolUnavailable("MCP pool acquisition failed after 5s"),
    )
    assert evidence["evaluated"] is False
    assert evidence["nomos"]["code"] == "nomos_authority_unavailable"


def test_non_connection_error_is_evaluation_failure_not_infra() -> None:
    evidence = _authority_failure_evidence(
        "record:observation", ValueError("internal evaluation bug")
    )
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_evaluation_failed"


# ---------------------------------------------------------------------------
# Hermetic: shared evidence → top-level code mapper
# ---------------------------------------------------------------------------

_VERDICT_MESSAGE = "writes require explicit authority"


def test_mapper_infrastructure_evidence_maps_to_authority_unavailable() -> None:
    code, message = _authority_refusal_code(
        _authority_failure_evidence(
            "record:observation", psycopg.OperationalError("AdminShutdown")
        ),
        verdict_message=_VERDICT_MESSAGE,
    )
    assert code == "authority_unavailable"
    assert "infrastructure failure" in message
    assert "AdminShutdown" in message
    assert "policy denial" in message


def test_mapper_nonverdict_evidence_maps_to_evaluation_failed() -> None:
    code, message = _authority_refusal_code(
        _authority_failure_evidence("record:observation", ValueError("boom")),
        verdict_message=_VERDICT_MESSAGE,
    )
    assert code == "authority_evaluation_failed"
    assert "no policy verdict was rendered" in message


def test_mapper_rendered_deny_keeps_unauthorized_write() -> None:
    rendered_deny = {
        "action": "record:observation",
        "authorized": False,
        "nomos_decision": {"effect": "deny", "reason_code": "nomos.test.denied"},
    }
    code, message = _authority_refusal_code(
        rendered_deny, verdict_message=_VERDICT_MESSAGE
    )
    assert code == "unauthorized_write"
    assert message == _VERDICT_MESSAGE


def test_read_operational_error_is_authority_unavailable_without_verdict() -> None:
    err = _authority_evaluation_error(
        "mcp.tool.call",
        psycopg.OperationalError("AdminShutdown: terminating connection"),
    )

    assert err.code == "authority_unavailable"
    assert "read authority" in err.message
    evidence = err.data["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_unavailable"
    assert "AdminShutdown" in evidence["nomos"]["message"]


def test_read_evaluation_error_is_distinct_without_verdict() -> None:
    err = _authority_evaluation_error(
        "mcp.graph.read",
        ValueError("internal evaluation bug"),
    )

    assert err.code == "authority_evaluation_failed"
    evidence = err.data["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_evaluation_failed"


# ---------------------------------------------------------------------------
# Hermetic: pools validate connections at checkout
# ---------------------------------------------------------------------------


def test_build_pool_configures_checkout_health_check() -> None:
    pool = _build_pool(
        conninfo="host=localhost dbname=unused",
        role="okg_mcp",
        min_size=0,
        max_size=1,
        open=False,
        name="check-config-test",
    )
    try:
        assert pool._check is ConnectionPool.check_connection
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Postgres: pool heals after its backend is terminated server-side
# ---------------------------------------------------------------------------


def _terminate_backend_and_wait(dsn: str, pid: int) -> None:
    """pid-scoped termination (shared test DB — never a blanket kill),
    waited on until the backend is really gone."""
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            row = admin.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = %s", (pid,)
            ).fetchone()
            if row is not None and row[0] == 0:
                return
            time.sleep(0.05)
    raise AssertionError(f"backend {pid} still alive after terminate")


@pytest.mark.postgres
def test_build_pool_survives_backend_termination(okg_migrated: str) -> None:
    pool = _build_pool(
        conninfo=okg_migrated,
        role="okg_mcp",
        min_size=1,
        max_size=1,
        open=True,
        name="heal-test",
    )
    try:
        pool.wait(timeout=10)
        with _pool_connection(pool) as conn:
            pid = conn.info.backend_pid
        _terminate_backend_and_wait(okg_migrated, pid)
        # The single idle connection is now dead; the checkout check must
        # discard it and hand out a fresh one instead of raising.
        with _pool_connection(pool) as conn:
            row = conn.execute("SELECT 1").fetchone()
        assert row == (1,)
        assert conn.info.backend_pid != pid
    finally:
        pool.close()


@pytest.mark.postgres
def test_pool_without_checkout_check_hands_out_the_dead_connection(
    okg_migrated: str,
) -> None:
    """Discriminator: proves the heal test above is not vacuous — the
    pre-fix configuration (no `check=`) hands the dead connection to the
    caller, which is exactly the issue #950 failure."""
    pool = ConnectionPool(
        conninfo=okg_migrated,
        min_size=1,
        max_size=1,
        timeout=10,
        kwargs={"autocommit": True},
        open=True,
        name="no-check-test",
    )
    try:
        pool.wait(timeout=10)
        with _pool_connection(pool) as conn:
            pid = conn.info.backend_pid
        _terminate_backend_and_wait(okg_migrated, pid)
        with pytest.raises(psycopg.OperationalError):
            with _pool_connection(pool) as conn:
                conn.execute("SELECT 1")
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Postgres: record() end-to-end refusal codes
# ---------------------------------------------------------------------------


def _close_server_pools(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


_CLAIM = {
    "provider_id": "cern-sso",
    "verification_status": "valid",
    "issuer": "https://auth.cern.ch/auth/realms/cern",
    "claims": {
        "sub": "cern-user-123",
        "aud": "akmon-cms",
        "groups": ["cms-comp-ops"],
        "exp": "2026-05-25T13:00:00Z",
    },
}


@pytest.mark.postgres
def test_record_dead_connection_reports_authority_unavailable(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead DB connection during Nomos evaluation (the post-rebuild
    AdminShutdown of issue #950) surfaces as authority_unavailable with
    the cause at the top level — not unauthorized_write, no claimed
    verdict, no record-template example."""
    import okg.substrate.nomos as nomos_module

    def _raise_admin_shutdown(*_args, **_kwargs):
        raise psycopg.OperationalError(
            "AdminShutdown: terminating connection due to administrator command"
        )

    monkeypatch.setattr(
        nomos_module,
        "evaluate_identity_claim_from_postgres",
        _raise_admin_shutdown,
    )
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="issue-950-test",
        allow_writes=True,
    )
    try:
        refused = mcp._tool_manager._tools["record"].fn(
            kind="observation",
            payload={"summary": "written after a database rebuild"},
            authority={"nomos_identity_claim": _CLAIM},
        )
    finally:
        _close_server_pools(mcp)

    assert refused["status"] == "refused"
    error = refused["error"]
    assert error["code"] == "authority_unavailable"
    assert "administrator command" in error["message"]
    assert "policy denial" not in error["code"]
    assert "example" not in error
    evidence = error["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_unavailable"


@pytest.mark.postgres
def test_record_invalid_claim_reports_evaluation_failed_not_unauthorized(
    clean_runtime: str,
) -> None:
    """A non-infrastructure evaluation failure (malformed identity claim)
    refuses with authority_evaluation_failed — also not a claimed policy
    verdict, also without the template example."""
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="issue-950-test",
        allow_writes=True,
    )
    try:
        refused = mcp._tool_manager._tools["record"].fn(
            kind="observation",
            payload={"summary": "claim is not an object"},
            authority={"nomos_identity_claim": "not-an-object"},
        )
    finally:
        _close_server_pools(mcp)

    assert refused["status"] == "refused"
    error = refused["error"]
    assert error["code"] == "authority_evaluation_failed"
    assert "example" not in error
    evidence = error["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_identity_claim_invalid"


@pytest.fixture(autouse=True)
def _trust_caller_identity_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """These rows evaluate caller-supplied identity claims on a single-user
    server, which PACT mcp-oidc-principal-policy makes an explicit launch-time
    opt-in; without it the claim is refused (nomos_identity_claim_untrusted)."""
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")
