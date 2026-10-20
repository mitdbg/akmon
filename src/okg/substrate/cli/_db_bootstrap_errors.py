"""Shared classification of database-bootstrap connection errors.

Per ``fix-provision-enforce-bringup`` (req.provision.cold-start-hint): the
most likely cold-start failure is a DSN whose database does not exist yet.
psycopg surfaces that as a connect-phase ``OperationalError`` whose
``sqlstate`` is ``None`` (libpq reports only the message text), so the
classifier matches both the server-side SQLSTATE ``3D000``
(``InvalidCatalogName``) and the connect-phase ``FATAL:  database "<name>"
does not exist`` message.

``okg provision`` consumes this at its first DB touch; ``okg init`` can
adopt the same classifier (it lives here, not in provision.py, for exactly
that reason).
"""
from __future__ import annotations

import re
from typing import Any

# Server message for a connect against a nonexistent database. libpq
# localizes only the "FATAL" severity tag, not the message body, and the
# quoted database name survives multi-host retry concatenation.
_MISSING_DB_PATTERN = re.compile(r'database "(?P<dbname>[^"]+)" does not exist')

#: SQLSTATE for InvalidCatalogName (raised server-side once a session
#: exists; connect-phase failures carry no sqlstate).
INVALID_CATALOG_SQLSTATE = "3D000"


def missing_database_name(exc: BaseException) -> str | None:
    """Database name when ``exc`` is a missing-database failure, else None.

    Recognizes psycopg ``OperationalError`` raised at connect time (message
    match) and ``InvalidCatalogName`` / SQLSTATE ``3D000`` raised by an
    established session. Any other exception classifies as None so callers
    re-raise it unchanged — this helper must never swallow unrelated
    connection failures (auth, refused, timeout) into a wrong hint.
    """
    import psycopg

    is_invalid_catalog = isinstance(exc, psycopg.errors.InvalidCatalogName)
    sqlstate = getattr(exc, "sqlstate", None)
    if not (
        is_invalid_catalog
        or sqlstate == INVALID_CATALOG_SQLSTATE
        or isinstance(exc, psycopg.OperationalError)
    ):
        return None
    match = _MISSING_DB_PATTERN.search(str(exc))
    if match:
        return match.group("dbname")
    if is_invalid_catalog or sqlstate == INVALID_CATALOG_SQLSTATE:
        # Definitely a missing database, but the name is not in the
        # message; the caller falls back to the DSN's dbname.
        return ""
    return None


def createdb_command(dsn: str) -> str:
    """The literal ``createdb`` invocation that creates the DSN's database."""
    fields = _conninfo_fields(dsn)
    parts = ["createdb"]
    host = str(fields.get("host") or "").split(",")[0].strip()
    port = str(fields.get("port") or "").split(",")[0].strip()
    user = str(fields.get("user") or "").strip()
    if host:
        parts.extend(["-h", host])
    if port:
        parts.extend(["-p", port])
    if user:
        parts.extend(["-U", user])
    parts.append(str(fields.get("dbname") or "").strip() or "<dbname>")
    return " ".join(parts)


def classify_missing_database_error(
    exc: BaseException,
    *,
    dsn: str,
    surface: str,
) -> Any | None:
    """A ``SubstrateError`` with the createdb fix when ``exc`` is a
    missing-database failure; None otherwise (caller re-raises).

    ``surface`` names the command for the operator ("okg provision",
    "okg init", ...).
    """
    from okg.substrate.errors import SubstrateError

    dbname = missing_database_name(exc)
    if dbname is None:
        return None
    if not dbname:
        dbname = str(_conninfo_fields(dsn).get("dbname") or "") or "<dbname>"
    err = SubstrateError(
        f"{surface}: database {dbname!r} does not exist on the target "
        "server (the server is reachable; only the database is missing)",
        data={"database": dbname, "sqlstate": INVALID_CATALOG_SQLSTATE},
        fix_hint=(
            f"create it, then re-run: `{createdb_command(dsn)}`"
        ),
    )
    err.code = "database_missing"
    return err


def _conninfo_fields(dsn: str) -> dict[str, Any]:
    import psycopg

    try:
        return psycopg.conninfo.conninfo_to_dict(dsn)
    except psycopg.ProgrammingError:
        return {}
