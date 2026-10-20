"""DBOS schema bootstrap entry point.

Wraps the DBOS runtime's schema migration so the substrate's bootstrap
flow can install DBOS-owned tables into a deployment's Postgres database
exactly once. Per Decision 11, the substrate configures DBOS with both
``system_database_url`` and ``database_url`` set to the deployment's
existing OKG DSN — the same Postgres database holds OKG fact tables and
DBOS workflow-state tables, in separate schemas.

Per Phase 1 §2.2.1 the bootstrap also applies the role-split SQL
(``runtime/sql/role_split.sql``) which establishes the
``okg_substrate`` / ``okg_dbos`` / ``okg_deployment_<name>`` roles
with column-level GRANTs that backstop the SDK boundary at the
Postgres layer.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import psycopg
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.db.dsn import redact as redact_dsn
from okg.substrate.db.dsn import redact_argv, redact_text
from okg.substrate.db.role_lock import lock_roles_for_alter
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from .config import RuntimeConfig


__all__ = [
    "BootstrapResult",
    "MCP_RO_DSN_ENV",
    "MCPCredentialError",
    "apply_role_split",
    "bootstrap_dbos_schema",
    "ensure_bootstrap_marker_table",
    "is_bootstrapped",
    "reconcile_mcp_login_role",
    "run_dbos_migrate",
]


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BootstrapResult:
    """Outcome of a bootstrap invocation.

    Attributes
    ----------
    deployment : str
        Deployment name the bootstrap targeted.
    already_bootstrapped : bool
        True if the marker row was already present (no-op invocation).
    schema_name : str
        DBOS schema name as configured (usually ``"dbos"``).
    role_split_applied : bool
        True if the role-split SQL was applied during this invocation.
    mcp_login : dict
        Report from the MCP credential reconciliation (see
        :func:`reconcile_mcp_login_role`). Present on BOTH the fresh and the
        already-bootstrapped path.
    """

    deployment: str
    already_bootstrapped: bool
    schema_name: str
    role_split_applied: bool
    mcp_login: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Marker-table contract
# ---------------------------------------------------------------------------


_MARKER_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS okg.dbos_bootstrap_marker (
    deployment       text PRIMARY KEY,
    schema_name      text NOT NULL,
    sdk_version      text NOT NULL,
    bootstrapped_at  timestamptz NOT NULL DEFAULT now()
)
"""


def ensure_bootstrap_marker_table(conn: psycopg.Connection) -> None:
    """Create ``okg.dbos_bootstrap_marker`` if it does not exist.

    Idempotent. Safe to call on every substrate bootstrap.
    """
    with conn.cursor() as cur:
        cur.execute(_MARKER_TABLE_DDL)


def is_bootstrapped(conn: psycopg.Connection, deployment: str) -> bool:
    """Return True if the deployment has a bootstrap-marker row."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM okg.dbos_bootstrap_marker "
            "WHERE deployment = %s",
            (deployment,),
        )
        return cur.fetchone() is not None


def _record_bootstrap(
    conn: psycopg.Connection,
    *,
    deployment: str,
    schema_name: str,
    sdk_version: str,
) -> None:
    """Insert/upsert the marker row recording this deployment's
    bootstrap completion."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.dbos_bootstrap_marker
              (deployment, schema_name, sdk_version)
            VALUES (%s, %s, %s)
            ON CONFLICT (deployment) DO UPDATE SET
                schema_name = EXCLUDED.schema_name,
                sdk_version = EXCLUDED.sdk_version,
                bootstrapped_at = now()
            """,
            (deployment, schema_name, sdk_version),
        )


# ---------------------------------------------------------------------------
# Role-split SQL application
# ---------------------------------------------------------------------------


def _load_role_split_sql() -> str:
    """Read the role-split SQL template from the package."""
    pkg = resources.files("okg.substrate.runtime") / "sql" / "role_split.sql"
    return pkg.read_text(encoding="utf-8")


def _load_escalation_schema_sql() -> str:
    """Read the Phase 2 escalation-schema additions from the package.

    The file is shipped alongside ``role_split.sql`` and creates the
    five new tables (`escalation_facts`, `escalation_live`,
    `deployment_cost_ledger`, `deployment_cost_caps`,
    `deployment_correlations`) that the SDK depends on. Idempotent
    via CREATE TABLE IF NOT EXISTS and guarded DO-blocks.
    """
    pkg = (
        resources.files("okg.substrate.runtime")
        / "sql" / "escalation_schema.sql"
    )
    return pkg.read_text(encoding="utf-8")


def apply_role_split(
    conn: psycopg.Connection,
    *,
    deployment: str,
    dbos_schema: str,
) -> None:
    """Apply the role-split SQL and the Phase-2 escalation schema for
    ``deployment``.

    Application order is load-bearing:

      1. ``escalation_schema.sql`` runs first so the conditional
         IF-EXISTS grants in ``role_split.sql`` (for escalation_facts,
         deployment_cost_ledger, etc.) can see the tables.
      2. ``role_split.sql`` runs second and applies the per-deployment
         role's grants, including the new Phase-2 grants.

    Idempotent: every CREATE uses ``IF NOT EXISTS``, every DO-block has
    existence checks, REVOKEs are no-ops when the privilege is absent,
    and ON CONFLICT upserts the marker rows. Safe to re-run.

    Both SQL files contain psycopg-style ``:'name'`` placeholders. We
    substitute them by string-replacement *before* execution because
    psycopg-3 does not support binding into top-level DDL via
    parameters.

    Safety: deployment names come from the manifest, which operators
    control; we validate against a conservative regex to refuse
    anything that could escape SQL string quoting.
    """
    import re

    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]{0,62}", deployment):
        raise ValueError(
            f"deployment name {deployment!r} is not a valid identifier; "
            "must match [a-zA-Z_][a-zA-Z0-9_-]{0,62}"
        )
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]{0,62}", dbos_schema):
        raise ValueError(
            f"dbos_schema {dbos_schema!r} is not a valid identifier; "
            "must match [a-zA-Z_][a-zA-Z0-9_]{0,62}"
        )

    # 1. Phase-2 escalation schema (no parameter substitution needed —
    #    schema names are fixed, not deployment-scoped).
    escalation_sql = _load_escalation_schema_sql()

    # 2. Role-split with substitution.
    role_split_sql = _load_role_split_sql()
    role_split_sql = role_split_sql.replace(
        ":'deployment_name'", _quote_literal(deployment),
    )
    role_split_sql = role_split_sql.replace(
        ":'dbos_schema'", _quote_literal(dbos_schema),
    )

    with conn.cursor() as cur:
        cur.execute(escalation_sql)
        cur.execute(role_split_sql)


MCP_RO_DSN_ENV = "OKG_MCP_RO_DSN"
DEFAULT_MCP_ROLE = "okg_mcp"


class MCPCredentialError(RuntimeError):
    """The configured MCP credential cannot be reconciled.

    Raised instead of leaving a role the deployment declares unusable: a
    NOLOGIN ``okg_mcp`` refuses every MCP pool connection and blocks
    ``okg deployment ready`` on ``mcp.graph_read`` (okg#1604).
    """


def reconcile_mcp_login_role(
    conn: psycopg.Connection,
    *,
    mcp_dsn: str | None,
    deployment_dsn: str | None = None,
) -> dict[str, Any]:
    """Reconcile the MCP role's LOGIN + password from the configured DSN.

    Migration creates ``okg_mcp`` as NOLOGIN (src/okg/substrate/db/views.sql),
    so after a volume wipe the role exists but cannot authenticate and NOTHING
    in the bootstrap path restored it -- operators had to run ``ALTER ROLE
    okg_mcp LOGIN PASSWORD ...`` by hand (okg#1604).

    Fails closed when a DSN IS configured but unusable: no user, no password, a
    missing role, or a credential that still cannot connect after the ALTER.
    Reports a loud skip -- never a silent success -- when no MCP DSN is
    configured; a deployment may legitimately have no MCP surface, and the
    report then names the observed role state and the repair.

    ``deployment_dsn`` binds the credential to the database being bootstrapped:
    ``OKG_MCP_RO_DSN`` is a PROCESS-global variable while the deployment is a
    parameter, so a DSN naming another host, port or database is refused rather
    than rotating this deployment's role to a foreign password and then
    "verifying" it against that other server.

    Idempotent, and quiet when there is nothing to do: a credential that already
    connects is left untouched, so a routine bootstrap neither rotates a working
    password nor writes one into the server log through DDL statement logging.
    """
    report: dict[str, Any] = {
        "schema": "okg.runtime_mcp_login_reconcile/v1",
        "reconciled": False,
        "env_var": MCP_RO_DSN_ENV,
    }
    if not mcp_dsn:
        can_login = _role_can_login(conn, DEFAULT_MCP_ROLE)
        report.update(
            reason="mcp_dsn_not_configured",
            role=DEFAULT_MCP_ROLE,
            role_present=can_login is not None,
            role_can_login=can_login,
        )
        if can_login is False:
            report["fix_hint"] = (
                f"role {DEFAULT_MCP_ROLE} is NOLOGIN and no {MCP_RO_DSN_ENV} is "
                "configured; set the deployment's MCP DSN and re-run "
                "`okg runtime bootstrap`"
            )
        return report
    try:
        params = conninfo_to_dict(mcp_dsn)
    except psycopg.Error as exc:
        raise MCPCredentialError(
            f"{MCP_RO_DSN_ENV} is not a parseable connection string: "
            f"{type(exc).__name__}"
        ) from exc
    role = str(params.get("user") or "")
    password = params.get("password")
    report["role"] = role or None
    if not role:
        raise MCPCredentialError(
            f"{MCP_RO_DSN_ENV} names no user; the MCP role cannot be "
            "reconciled from it"
        )
    if not password:
        raise MCPCredentialError(
            f"{MCP_RO_DSN_ENV} carries no password for role {role!r}; the MCP "
            "login cannot be reconciled without one"
        )
    if deployment_dsn:
        target = conninfo_to_dict(deployment_dsn)
        mismatched = {
            key: (params.get(key), target.get(key))
            for key in ("host", "port", "dbname")
            if _normalized(params.get(key)) != _normalized(target.get(key))
        }
        if mismatched:
            raise MCPCredentialError(
                f"{MCP_RO_DSN_ENV} points at a different database than the "
                f"deployment being bootstrapped ({mismatched!r}); export the "
                "MCP DSN for THIS deployment, or pass it explicitly"
            )
    can_login = _role_can_login(conn, role)
    report["role_can_login_before"] = can_login
    if can_login is None:
        raise MCPCredentialError(
            f"{MCP_RO_DSN_ENV} names role {role!r}, which does not exist in "
            "this database; run `okg migrate --apply` first"
        )
    if can_login and _mcp_dsn_connects(mcp_dsn):
        report.update(
            reconciled=False,
            reason="already_usable",
            verified=True,
        )
        return report
    # The role is one cluster-wide row: another deployment's bootstrap, an
    # environment-service install, or any other session altering it at the
    # same moment would fail one of the two ALTERs with "tuple concurrently
    # updated" (issue #1985). Hold the role's cluster lock for the ALTER.
    with conn.transaction():
        lock_roles_for_alter(conn, [role])
        with conn.cursor() as cur:
            # Send a client-computed SCRAM verifier, never the password, so a
            # DDL statement log cannot capture it (issue #3273).
            verifier = conn.pgconn.encrypt_password(
                str(password).encode("utf-8"), role.encode("utf-8"),
                b"scram-sha-256",
            ).decode("ascii")
            cur.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(verifier),
                )
            )
    # Real verification: prove the reconciled credential connects, rather than
    # reporting success from the ALTER alone.
    try:
        with db_connect(mcp_dsn) as probe:
            with probe.cursor() as cur:
                cur.execute("SELECT current_user")
                connected_as = str(cur.fetchone()[0])
    except psycopg.Error as exc:
        raise MCPCredentialError(
            f"role {role!r} still cannot connect with {MCP_RO_DSN_ENV} after "
            f"ALTER ROLE ... LOGIN: {type(exc).__name__}"
        ) from exc
    report.update(
        reconciled=True,
        reason="login_and_password_reconciled",
        verified=True,
        connected_as=connected_as,
    )
    return report


def _normalized(value: Any) -> str:
    """Compare connection-target fields without empty/None skew."""
    return "" if value is None else str(value).strip()


def _mcp_dsn_connects(dsn: str) -> bool:
    """True if the DSN authenticates today (no exception detail is kept)."""
    try:
        with db_connect(dsn):
            return True
    except psycopg.Error:
        return False


def _role_can_login(conn: psycopg.Connection, role: str) -> bool | None:
    """True/False for an existing role's LOGIN attribute; None if absent."""
    with conn.cursor() as cur:
        cur.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", (role,))
        row = cur.fetchone()
    return None if row is None else bool(row[0])


def _quote_literal(s: str) -> str:
    """Return a single-quoted SQL string literal of `s`.

    `s` is restricted to identifier-safe characters by the validators
    above so this single-quote escaping is sufficient.
    """
    return "'" + s.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# DBOS schema migration
# ---------------------------------------------------------------------------


def run_dbos_migrate(*, dsn: str) -> None:
    """Invoke ``dbos migrate`` against the deployment DSN.

    Implementation: spawn the documented ``dbos`` CLI binary as a
    subprocess. This is the operator-facing migration path the DBOS
    docs describe, and it isolates the migration from any side effects
    of the in-process DBOS singleton lifecycle (workflow worker
    threads, scheduler, LISTEN connections, cron registrations) that
    a launch+destroy approach would incur.

    Falls back to ``python -m dbos migrate`` if the ``dbos`` binary is
    not on PATH (e.g., a venv whose entry-point shims are missing).

    DBOS reads the system DB URL from the ``DBOS_SYSTEM_DATABASE_URL``
    env var; we set it for the subprocess. Per Decision 11 the system
    DB URL and application DB URL point to the same DSN.

    Idempotent: DBOS's migration runner inspects the schema and applies
    only missing migrations on rerun.

    Raises
    ------
    RuntimeError
        On non-zero exit. The error message includes the captured
        stderr (truncated to 2 KB) so operators see the failure
        without re-running.
    """
    import shutil
    import subprocess
    import sys

    # The `dbos` binary lives in the same `bin/` directory as the
    # current Python interpreter when both are installed in a venv.
    # `shutil.which` only checks PATH, which the parent process's
    # PATH may not include; deriving from sys.executable is more
    # reliable for venv-based installs.
    # The `dbos` binary lives in the same `bin/` directory as the
    # current Python interpreter when both are in a venv. The DBOS
    # CLI takes `--sys-db-url` and `--db-url` flags directly rather
    # than reading env vars; pass both explicitly. Per Decision 11
    # they are the same DSN.
    venv_dbos = Path(sys.executable).parent / "dbos"
    if venv_dbos.is_file():
        binary = str(venv_dbos)
    elif shutil.which("dbos"):
        binary = shutil.which("dbos")
    else:
        binary = None

    if binary is not None:
        cmd = [
            binary, "migrate",
            "--db-url", dsn,
            "--sys-db-url", dsn,
        ]
    else:
        # Last-resort fallback: invoke the typer app directly.
        # `python -m dbos` does not work because the dbos package
        # has no `__main__.py`.
        cmd = [
            sys.executable,
            "-c",
            (
                "from dbos.cli.cli import app; "
                f"app(['migrate', '--db-url', {dsn!r}, "
                f"'--sys-db-url', {dsn!r}])"
            ),
        ]

    log.info(
        "dbos migrate: cmd=%r dsn=%s",
        cmd[:2] + ["..."],  # redact flags from log
        redact_dsn(dsn),
    )
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    if proc.returncode != 0:
        # `cmd` carries the DSN twice, and the DBOS CLI may echo it: redact
        # before truncating, so a cut cannot leave half a password unmasked.
        shown_cmd = redact_argv(cmd, dsn_flags=("--db-url", "--sys-db-url"))
        raise RuntimeError(
            f"dbos migrate failed (exit {proc.returncode}):\n"
            f"  cmd: {shown_cmd!r}\n"
            f"  stderr: {redact_text(proc.stderr)[:2000]}\n"
            f"  stdout: {redact_text(proc.stdout)[:2000]}"
        )

    log.info(
        "dbos migrate completed (stdout %d bytes)", len(proc.stdout),
    )


# ---------------------------------------------------------------------------
# Bootstrap entry point
# ---------------------------------------------------------------------------


def bootstrap_dbos_schema(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    mcp_dsn: str | None = None,
) -> BootstrapResult:
    """Install the DBOS schema and the role split into ``dsn``.

    Sequence:
      1. Open a psycopg connection to ``dsn`` as the
         ``okg_substrate`` role (the role with permission to create
         schemas and roles). The DSN must be configured to authenticate
         as that role.
      2. Ensure the bootstrap marker and substrate-owned deployment
         bookkeeping tables exist.
      3. If ``is_bootstrapped(conn, deployment)``, return with
         ``already_bootstrapped=True`` after that schema repair.
      4. Otherwise run ``dbos migrate`` (creates ``runtime.dbos_schema`` and the
         DBOS-internal tables).
      5. Apply the role-split SQL from ``runtime/sql/role_split.sql``.
      6. Insert the bootstrap-marker row.

    Returns :class:`BootstrapResult`. Idempotent on re-run.

    Raises
    ------
    psycopg.Error
        If the DSN cannot be used to create roles or schemas. Operators
        seeing this should verify they are connecting as a role with
        ``CREATE ROLE`` and ``CREATE SCHEMA`` privileges.
    ValueError
        If ``deployment`` or ``runtime.dbos_schema`` contains characters
        outside the conservative identifier regex.
    """
    log.info(
        "bootstrap_dbos_schema: deployment=%s schema=%s dsn=%s",
        deployment, runtime.dbos_schema, redact_dsn(dsn),
    )

    # Phase A: install substrate-owned runtime bookkeeping, then check the
    # marker (no DBOS process touched yet).  Worker boot records its desired
    # state in these tables.  Leaving their DDL until the first worker launch
    # changes the database schema *after* an operator may have minted a
    # schema-bound recovery receipt, invalidating that otherwise-current proof.
    # Run this on the already-bootstrapped path too so deployments created
    # before these tables existed converge on their next explicit bootstrap.
    from okg.substrate.deployment_apply import ensure_deployment_apply_schema

    if mcp_dsn is None:
        mcp_dsn = os.environ.get(MCP_RO_DSN_ENV) or None
    if mcp_dsn is None:
        # The owner-only file `okg db role-passwords --apply` writes is the
        # second source of the MCP credential (issue #3273): a volume wipe
        # recreates okg_mcp NOLOGIN, and this restores the recorded password.
        from okg.substrate.local_database_roles import (
            dsn_is_local,
            stored_role_dsn,
        )

        from okg.substrate.db import dsn as dsn_module

        if dsn_is_local(dsn):
            stored = stored_role_dsn(dsn, DEFAULT_MCP_ROLE)
            if stored:
                # Re-anchor on THIS DSN's host spelling so the same-database
                # check below compares like with like (the file is keyed by
                # server, and `localhost` and `127.0.0.1` are one server).
                recorded = dsn_module.parse(stored)
                mcp_dsn = dsn_module.parse(dsn).with_params(
                    user=recorded.user, password=recorded.password,
                ).render()
    with db_connect(dsn, autocommit=True) as conn:
        ensure_bootstrap_marker_table(conn)
        ensure_deployment_apply_schema(conn)
        if is_bootstrapped(conn, deployment):
            # The MCP credential is reconciled on THIS path too: a volume wipe
            # recreates okg_mcp as NOLOGIN while the marker row survives in the
            # operator's records, and an early return here is exactly what left
            # the credential broken with no in-band repair (okg#1604).
            mcp_login = reconcile_mcp_login_role(
                conn, mcp_dsn=mcp_dsn, deployment_dsn=dsn,
            )
            log.info(
                "bootstrap_dbos_schema: deployment=%s already "
                "bootstrapped; runtime bookkeeping schema ensured", deployment,
            )
            return BootstrapResult(
                deployment=deployment,
                already_bootstrapped=True,
                schema_name=runtime.dbos_schema,
                role_split_applied=False,
                mcp_login=mcp_login,
            )

    # Phase B: dbos migrate (creates dbos schema + tables).
    run_dbos_migrate(dsn=dsn)

    # Phase C: role split + marker write.
    with db_connect(dsn, autocommit=True) as conn:
        apply_role_split(
            conn,
            deployment=deployment,
            dbos_schema=runtime.dbos_schema,
        )
        _record_bootstrap(
            conn,
            deployment=deployment,
            schema_name=runtime.dbos_schema,
            sdk_version=runtime.sdk_version,
        )
        # AFTER the marker: a refused MCP credential must not erase the fact
        # that the schema and role split landed, or the next run would repeat
        # the migration to reach the same refusal.
        mcp_login = reconcile_mcp_login_role(
            conn, mcp_dsn=mcp_dsn, deployment_dsn=dsn,
        )

    log.info(
        "bootstrap_dbos_schema: deployment=%s complete (schema=%s)",
        deployment, runtime.dbos_schema,
    )
    return BootstrapResult(
        deployment=deployment,
        already_bootstrapped=False,
        schema_name=runtime.dbos_schema,
        role_split_applied=True,
        mcp_login=mcp_login,
    )
