"""okg#1604: the runtime bootstrap must reconcile the MCP login credential.

Migration creates the MCP role as NOLOGIN, so after a volume wipe the role
exists but refuses every MCP pool connection and `okg deployment ready` blocks
on `mcp.graph_read`. Nothing in the bootstrap path repaired that -- operators
had to `ALTER ROLE okg_mcp LOGIN PASSWORD ...` by hand.

These run against the test DSN (a private database, never the shared okg_test
front) and use a per-test throwaway role so the deployment's own okg_mcp
credential is never rewritten by a test run.
"""
from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

from okg.substrate.runtime.bootstrap import (  # noqa: E402
    MCP_RO_DSN_ENV,
    MCPCredentialError,
    reconcile_mcp_login_role,
)


pytestmark = pytest.mark.postgres

PASSWORD = "reconcile-me-4b7f"


@pytest.fixture
def nologin_role(clean_runtime: str) -> Iterator[tuple[str, str, str]]:
    """A NOLOGIN role plus a DSN that authenticates as it. Yields (dsn, role)."""
    role = f"okg_mcp_t{uuid.uuid4().hex[:10]}"
    base = conninfo_to_dict(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        conn.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(str(base["dbname"])), sql.Identifier(role),
            )
        )
    role_dsn = make_conninfo(clean_runtime, user=role, password=PASSWORD)
    try:
        yield clean_runtime, role_dsn, role
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(
                    sql.Identifier(str(base["dbname"])), sql.Identifier(role),
                )
            )
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


def _can_connect(dsn: str) -> bool:
    try:
        with psycopg.connect(dsn, connect_timeout=10):
            return True
    except psycopg.Error:
        return False


def test_reconcile_gives_a_nologin_mcp_role_a_working_credential(
    nologin_role: tuple[str, str, str],
) -> None:
    admin_dsn, role_dsn, role = nologin_role
    assert _can_connect(role_dsn) is False, "NOLOGIN role connected before repair"

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        report = reconcile_mcp_login_role(conn, mcp_dsn=role_dsn)

    assert report["reconciled"] is True, report
    assert report["role"] == role
    assert report["role_can_login_before"] is False
    assert report["verified"] is True
    assert report["connected_as"] == role
    assert _can_connect(role_dsn) is True
    # The test cluster may authenticate with `trust`, which would let a
    # LOGIN-only repair pass. Prove the PASSWORD half from the catalog, and
    # prove the role is otherwise unprivileged.
    with psycopg.connect(admin_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT rolpassword IS NOT NULL, rolsuper, rolcreaterole "
            "FROM pg_authid WHERE rolname = %s",
            (role,),
        )
        assert cur.fetchone() == (True, False, False)


def test_a_working_credential_is_left_alone(
    nologin_role: tuple[str, str, str],
) -> None:
    """A routine re-run must not rotate a credential that already works."""
    admin_dsn, role_dsn, _role = nologin_role
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        first = reconcile_mcp_login_role(conn, mcp_dsn=role_dsn)
        second = reconcile_mcp_login_role(conn, mcp_dsn=role_dsn)
    assert first["reconciled"] is True
    assert second["reconciled"] is False
    assert second["reason"] == "already_usable"
    assert second["verified"] is True
    assert _can_connect(role_dsn) is True


def test_a_dsn_for_another_database_is_refused(
    nologin_role: tuple[str, str, str],
) -> None:
    """OKG_MCP_RO_DSN is process-global; the deployment is a parameter.

    A DSN naming another database must not rotate THIS deployment's role to a
    foreign password and then verify itself against that other server.
    """
    admin_dsn, role_dsn, role = nologin_role
    foreign = make_conninfo(role_dsn, dbname="postgres")
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with pytest.raises(MCPCredentialError) as excinfo:
            reconcile_mcp_login_role(
                conn, mcp_dsn=foreign, deployment_dsn=admin_dsn
            )
    assert "different database" in str(excinfo.value)
    assert _can_connect(role_dsn) is False, "the role was altered anyway"
    assert role in str(excinfo.value) or MCP_RO_DSN_ENV in str(excinfo.value)


def test_absent_dsn_reports_a_loud_skip_naming_the_repair(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = reconcile_mcp_login_role(conn, mcp_dsn=None)
    assert report["reconciled"] is False
    assert report["reason"] == "mcp_dsn_not_configured"
    assert report["env_var"] == MCP_RO_DSN_ENV
    assert "role_can_login" in report
    if report["role_can_login"] is False:
        assert MCP_RO_DSN_ENV in report["fix_hint"]


def test_dsn_without_password_fails_closed(
    nologin_role: tuple[str, str, str],
) -> None:
    admin_dsn, role_dsn, role = nologin_role
    passwordless = make_conninfo(
        "", **{k: v for k, v in conninfo_to_dict(role_dsn).items() if k != "password"}
    )
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with pytest.raises(MCPCredentialError) as excinfo:
            reconcile_mcp_login_role(conn, mcp_dsn=passwordless)
    assert MCP_RO_DSN_ENV in str(excinfo.value)
    assert role in str(excinfo.value)
    # Fail closed: the role is left exactly as it was found.
    assert _can_connect(role_dsn) is False


def test_dsn_naming_an_absent_role_fails_closed(clean_runtime: str) -> None:
    missing = f"okg_mcp_absent_{uuid.uuid4().hex[:8]}"
    dsn = make_conninfo(clean_runtime, user=missing, password=PASSWORD)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(MCPCredentialError) as excinfo:
            reconcile_mcp_login_role(conn, mcp_dsn=dsn)
    assert missing in str(excinfo.value)
