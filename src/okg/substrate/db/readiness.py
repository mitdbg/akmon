"""One database-readiness answer the entry commands share.

Why this exists
---------------
Before this module, every command assumed a migrated schema and usable
roles, and none of them checked. The measured consequences, from the
literal operator walkthrough of a wheel install (issues #1795, #2128):

- ``okg run --once --apply`` against a scaffolded-but-unmigrated database
  died with a bare ``psycopg.errors.InvalidSchemaName: schema "okg" does
  not exist`` traceback -- no error code, no ``fix_hint``, and nothing
  naming ``okg migrate``.
- ``okg mcp-serve`` against a MIGRATED database exited 0 and served a
  graph it could never read: migration creates ``okg_mcp`` NOLOGIN on
  purpose, so every pool connection failed with ``role "okg_mcp" is not
  permitted to log in`` while the launch logged "starting anyway".

The readiness facts were already discoverable -- ``bootstrap_preflight``
(``okg doctor --check preflight``) knows whether the schema is applied,
and ``okg deployment ready`` knows whether the MCP principal can log in
-- but nothing on the first-run path consulted either. This module is
that one check, expressed as a refusal the entry commands raise before
they touch the graph.

It does not repair anything. In particular it never grants LOGIN: the
NOLOGIN roles are cluster-wide objects and a deliberate design choice
(``src/okg/substrate/db/views.sql`` creates them so migration never
invents a credential), so the correct answer is a refusal that names the
provisioning route, not a silent ``ALTER ROLE``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from okg.substrate.errors import SubstrateError


#: Environment override that supplies an explicit DSN for the MCP read
#: role. Consumed by ``okg.substrate.mcp.server._derive_role_dsn``; when it
#: is set the derived-role path is not used at all, so the login state of
#: the substrate's own ``okg_mcp`` role is irrelevant and not checked.
MCP_RO_DSN_ENV = "OKG_MCP_RO_DSN"

#: The least-privilege role the MCP read pool connects as by default.
MCP_READ_ROLE = "okg_mcp"

#: The deployment name the ad-hoc ``--dsn`` path reports. It is not a
#: manifest, so `okg migrate --deployment default` would be a command the
#: operator cannot run; a hint naming it is worse than no hint.
AD_HOC_DEPLOYMENT_NAME = "default"

#: Relation whose absence means "the schema was never applied". Same probe
#: `bootstrap_preflight` uses, so the two surfaces cannot disagree about
#: whether a database is migrated.
_SCHEMA_APPLIED_RELATION = "okg.graph_generations"


@dataclass(frozen=True)
class RoleLoginState:
    """Whether one database role can be used as a login user."""

    role: str
    present: bool
    can_login: bool

    @property
    def unusable(self) -> bool:
        """True only for a role that exists and cannot authenticate.

        An ABSENT role is not reported as unusable here: the schema check
        already covers "nothing is provisioned", and a deployment may
        legitimately run without the least-privilege split.
        """
        return self.present and not self.can_login


@dataclass(frozen=True)
class MissingSchemaObject:
    """An index `okg migrate --apply` builds through `ensure_schema` that the
    database lacks, or holds only as an invalid (half-built) index.

    The migration ledger does not list these, so before issue #3311 a
    database could read "0 pending" while grep refused every call.
    """

    name: str
    table: str
    state: str  # "absent" or "invalid"
    required_by: str
    effect: str  # "refused" or "sequential_scan"

    def as_dict(self) -> dict[str, str]:
        return {
            "index": self.name,
            "table": self.table,
            "state": self.state,
            "required_by": self.required_by,
            "effect": self.effect,
        }


@dataclass(frozen=True)
class DatabaseReadiness:
    """What one probe of a deployment's database found."""

    schema_present: bool
    schema_applied: bool
    ledger_present: bool
    pending_migrations: tuple[str, ...] = ()
    pending_migration_count: int = 0
    roles: tuple[RoleLoginState, ...] = ()
    #: Tool indexes `ensure_schema` would build that are absent or invalid.
    #: Pending work like a migration: `okg migrate --apply` builds them.
    missing_schema_objects: tuple[MissingSchemaObject, ...] = ()
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def pending_work_count(self) -> int:
        """Migrations plus missing schema objects: what `okg migrate
        --apply` still has to do. Zero means the served tools can work."""
        return self.pending_migration_count + len(self.missing_schema_objects)

    @property
    def migrated(self) -> bool:
        """True when the schema exists, is applied, and has a ledger."""
        return self.schema_present and self.schema_applied and self.ledger_present

    @property
    def unusable_roles(self) -> tuple[RoleLoginState, ...]:
        return tuple(state for state in self.roles if state.unusable)

    def as_dict(self) -> dict[str, Any]:
        """Report shape for a refusal's `data` field or a status payload."""
        return {
            "schema_present": self.schema_present,
            "schema_applied": self.schema_applied,
            "ledger_present": self.ledger_present,
            "migrated": self.migrated,
            "pending_migration_count": self.pending_migration_count,
            "pending_migrations": list(self.pending_migrations),
            "pending_schema_object_count": len(self.missing_schema_objects),
            "pending_schema_objects": [
                obj.as_dict() for obj in self.missing_schema_objects
            ],
            "pending_work_count": self.pending_work_count,
            "roles": [
                {
                    "role": state.role,
                    "present": state.present,
                    "rolcanlogin": state.can_login,
                }
                for state in self.roles
            ],
            **self.data,
        }


def migrate_command(deployment: str | None) -> str:
    """The literal command that makes an unmigrated database ready.

    Named for a WHEEL user: ``okg migrate`` is a top-level console verb, so
    the hint never points into a source checkout. The DSN form is used when
    no deployment name is in hand OR when the name is the ad-hoc ``--dsn``
    placeholder, and the DSN itself is NOT interpolated into the text -- it
    can carry a password.
    """
    if deployment and deployment != AD_HOC_DEPLOYMENT_NAME:
        return f"okg migrate --deployment {deployment} --apply"
    return "okg migrate --dsn <the deployment DSN> --apply"


def read_database_readiness(
    conn: Any,
    *,
    roles: tuple[str, ...] = (),
) -> DatabaseReadiness:
    """Probe readiness over an already-open owner connection.

    Read-only: three catalog queries plus the migration ledger. Never
    raises for a missing schema -- ``to_regclass`` and
    ``information_schema`` answer with NULL / no row rather than erroring,
    which is the whole point of probing here instead of discovering it
    from a failed DDL statement several frames down.
    """
    from psycopg.rows import dict_row

    from okg.substrate.db.management import _migration_status_from_cursor

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'okg'"
        )
        schema_present = cur.fetchone() is not None
        cur.execute("SELECT to_regclass(%s)", (_SCHEMA_APPLIED_RELATION,))
        row = cur.fetchone()
        schema_applied = bool(row and row[0] is not None)
        role_states: list[RoleLoginState] = []
        for role in roles:
            cur.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", (role,)
            )
            role_row = cur.fetchone()
            role_states.append(
                RoleLoginState(
                    role=role,
                    present=role_row is not None,
                    can_login=bool(role_row[0]) if role_row else False,
                )
            )
    # `_migration_status_from_cursor` indexes its rows by COLUMN NAME, so it
    # requires a dict-row cursor. Reading it through a default tuple cursor
    # raised `AttributeError: 'list' object has no attribute 'get'` from
    # inside the ledger scan -- caught here only because this probe was run
    # against a real migrated database. Match `migration_status`, which
    # opens its own cursor the same way.
    with conn.cursor(row_factory=dict_row) as cur:
        status = _migration_status_from_cursor(cur)
    ledger_present = bool(status.get("ledger_exists"))
    pending = tuple(
        str(entry.get("migration_id"))
        for entry in status.get("pending") or []
        if entry.get("migration_id")
    )
    # Identifiers only when a ledger exists to be behind: with no ledger
    # every known migration is "pending" and listing all of them buries
    # the refusal it is attached to. The COUNT is suppressed with the
    # list, not separately: real `okg status --json` output on an empty
    # database read `"pending_migration_count": 33, "pending_migrations":
    # []`, two keys about one fact that contradicted each other, and
    # every reporting surface derives its line from the count.
    reported_pending = pending if ledger_present else ()
    # Only meaningful on an applied schema with a ledger: an unmigrated
    # database is refused or reported as such already, and every object
    # would read "missing".
    missing: tuple[MissingSchemaObject, ...] = ()
    if schema_applied and ledger_present:
        with conn.cursor(row_factory=dict_row) as cur:
            missing = _missing_tool_indexes(cur)
    return DatabaseReadiness(
        schema_present=schema_present,
        schema_applied=schema_applied,
        ledger_present=ledger_present,
        pending_migrations=reported_pending,
        pending_migration_count=len(reported_pending),
        roles=tuple(role_states),
        missing_schema_objects=missing,
    )


def _missing_tool_indexes(cur: Any) -> tuple[MissingSchemaObject, ...]:
    """The tool indexes `ensure_schema` builds that are absent or invalid.

    Expected names come from the same spec `ensure_schema` builds from
    (`graph_store_internal.schema.tool_index_names`) and the interval
    tables Chronos registered; a database with no registered graph tables
    yet expects none. An invalid index (a failed concurrent build) counts:
    the planner cannot use it, so the tool behaves as if it were absent.
    """
    import json

    from okg.substrate.graph_store_internal.common import EDGE_TABLE, NODE_TABLE
    from okg.substrate.graph_store_internal.schema import tool_index_names

    cur.execute("SELECT to_regclass('public._chronos_branch_tables') AS reg")
    row = cur.fetchone()
    if not row or row["reg"] is None:
        return ()
    cur.execute(
        """
        SELECT table_name, physical_table, columns
          FROM public._chronos_branch_tables
         WHERE backend = 'interval'
           AND table_name IN (%s, %s)
        """,
        (NODE_TABLE, EDGE_TABLE),
    )
    tables = {
        r["table_name"]: {
            "physical_table": r["physical_table"],
            "columns": tuple(json.loads(r["columns"])),
        }
        for r in cur.fetchall()
    }
    expected = tool_index_names(tables)
    if not expected:
        return ()
    cur.execute(
        """
        SELECT c.relname AS name, x.indisvalid AS valid
          FROM pg_class AS c
          JOIN pg_index AS x ON x.indexrelid = c.oid
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
           AND c.relname = ANY(%s)
        """,
        ([name for name, _table, _spec in expected],),
    )
    valid = {r["name"]: bool(r["valid"]) for r in cur.fetchall()}
    # An invalid index still appears in `pg_indexes`, so the tools' own
    # guards accept it and the planner then scans: it never refuses.
    return tuple(
        MissingSchemaObject(
            name=name,
            table=table,
            state="absent" if name not in valid else "invalid",
            required_by=spec.required_by,
            effect=spec.effect if name not in valid else "sequential_scan",
        )
        for name, table, spec in expected
        if not valid.get(name, False)
    )


def drop_invalid_index_hint(missing: tuple[MissingSchemaObject, ...]) -> str | None:
    """The step `okg migrate --apply` cannot do for an invalid index.

    `ensure_schema` creates these with `CREATE INDEX IF NOT EXISTS`, which
    keeps an existing invalid index, so it has to be dropped first.
    """
    invalid = [m.name for m in missing if m.state == "invalid"]
    if not invalid:
        return None
    statements = "; ".join(f'DROP INDEX public."{name}"' for name in invalid)
    return f"first drop the invalid {'index' if len(invalid) == 1 else 'indexes'} ({statements})"


def check_database_readiness(
    dsn: str,
    *,
    surface: str,
    roles: tuple[str, ...] = (),
) -> DatabaseReadiness:
    """Connect and probe. Connect failures become named refusals.

    A precheck that answered a traceback would be its own defect, so the
    ways the probe itself can fail are classified: a database that does
    not exist reuses the existing ``database_missing`` classifier (with
    its literal ``createdb`` command), a role that may not read the
    readiness relations becomes ``database_readiness_unreadable``, and
    anything else becomes ``database_unreachable`` naming the host and
    port.

    The privilege case is separated because folding it into
    "unreachable" was measured to be actively misleading: a login role
    holding the substrate's own ``app_ro`` on a fully migrated database
    was refused with "cannot reach the deployment database" and the hint
    "start or reach the Postgres server", when the server was up, the
    database was migrated, and the real cause was ``permission denied
    for table schema_migrations`` (SQLSTATE 42501).
    """
    import psycopg

    from okg.substrate.cli._db_bootstrap_errors import (
        classify_missing_database_error,
    )
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(dsn, connect_timeout=10, autocommit=True) as conn:
            return read_database_readiness(conn, roles=roles)
    except psycopg.errors.InsufficientPrivilege as exc:
        raise _readiness_unreadable_error(exc, dsn=dsn, surface=surface) from exc
    except psycopg.Error as exc:
        missing = classify_missing_database_error(exc, dsn=dsn, surface=surface)
        if missing is not None:
            raise missing from exc
        raise _unreachable_error(exc, dsn=dsn, surface=surface) from exc


def require_database_ready(
    dsn: str,
    *,
    surface: str,
    deployment: str | None = None,
    roles: tuple[str, ...] = (),
) -> DatabaseReadiness:
    """Probe and refuse, in operator words, when the database is not ready.

    Raised codes:

    ``database_not_migrated``
        The ``okg`` schema, its core relations, or the migration ledger are
        absent. ``fix_hint`` is the literal ``okg migrate ... --apply``.
    ``mcp_role_cannot_login``
        A requested role exists with ``rolcanlogin = false`` and no
        explicit DSN override is configured for it.
    ``database_readiness_unreadable``
        The connection works but the role it authenticated as may not
        read the readiness relations, so readiness is undecidable.

    Pending migrations on an otherwise-applied schema are REPORTED in the
    returned readiness and in refusal payloads, never refused on: a
    deployment running one migration behind is a routine, working state
    and turning it into a hard stop would break every deployment mid-roll.
    The caller MUST NOT discard the returned value -- reporting is the
    other half of that decision, and `pending_migration_report` /
    `pending_migration_warning` below turn it into operator output.
    """
    readiness = check_database_readiness(dsn, surface=surface, roles=roles)
    if not readiness.migrated:
        raise _not_migrated_error(
            readiness, surface=surface, deployment=deployment
        )
    unusable = readiness.unusable_roles
    if unusable:
        raise _role_cannot_login_error(
            unusable, readiness, surface=surface, deployment=deployment
        )
    return readiness


def pending_migration_report(
    readiness: DatabaseReadiness,
    *,
    deployment: str | None = None,
) -> dict[str, Any] | None:
    """The `database_readiness` block for a surface that CONTINUES.

    ``None`` when nothing is pending, so a caller says something only
    when there is something to say. The decision of 2026-09-21 for
    issue #2128: a database whose schema is applied but whose ledger is
    behind is reported and the command continues; only an absent schema
    or ledger is refused.
    """
    if readiness.pending_work_count <= 0:
        return None
    block = readiness.as_dict()
    block["migrate_command"] = migrate_command(deployment)
    return block


def pending_migration_warning(
    readiness: DatabaseReadiness,
    *,
    surface: str,
    deployment: str | None = None,
) -> str | None:
    """One operator line: how far behind, which ones, what to run."""
    count = readiness.pending_migration_count
    missing = readiness.missing_schema_objects
    if count <= 0 and not missing:
        return None
    command = migrate_command(deployment)
    if not missing:
        noun = "migration" if count == 1 else "migrations"
        listed = ", ".join(readiness.pending_migrations)
        return (
            f"{surface}: this database is behind by {count} {noun} "
            f"({listed}); continuing, because a deployment one migration "
            "behind is a working state. Run "
            f"`{command}` to catch up"
        )
    # A missing tool index is not "one migration behind": the tool that
    # needs it fails (or scans) on every call, so say which tool.
    behind = ""
    if count > 0:
        noun = "migration" if count == 1 else "migrations"
        behind = (
            f"this database is behind by {count} {noun} "
            f"({', '.join(readiness.pending_migrations)}), and "
        )
    refused = sorted({m.required_by for m in missing if m.effect == "refused"})
    slow = sorted({m.required_by for m in missing if m.effect != "refused"})
    effects = [f"{tool} will refuse every call" for tool in refused]
    effects += [f"{tool} will scan instead of using an index" for tool in slow]
    noun = "index" if len(missing) == 1 else "indexes"
    drop = drop_invalid_index_hint(missing)
    return (
        f"{surface}: {behind}{len(missing)} {noun} that `okg migrate --apply` "
        f"builds {'is' if len(missing) == 1 else 'are'} missing or invalid "
        f"({', '.join(m.name for m in missing)}), so "
        f"{'; '.join(effects)}. "
        + (f"{drop[0].upper()}{drop[1:]}, then run" if drop else "Run")
        + f" `{command}` to build them"
    )


def mcp_read_roles_to_check() -> tuple[str, ...]:
    """``(okg_mcp,)`` unless an explicit MCP read DSN supersedes the role.

    ``OKG_MCP_RO_DSN`` short-circuits ``_derive_role_dsn`` entirely, so when
    it is set the substrate's own role is not the principal and its login
    state says nothing about whether MCP can read.
    """
    if os.environ.get(MCP_RO_DSN_ENV):
        return ()
    return (MCP_READ_ROLE,)


def require_mcp_read_dsn_usable(
    *,
    surface: str,
    deployment: str | None = None,
) -> None:
    """Prove an ``OKG_MCP_RO_DSN`` override can actually be connected.

    ``mcp_read_roles_to_check`` suppresses the derived-role check on the
    mere PRESENCE of the variable. Measured consequence: with the
    variable set to a DSN naming a NOLOGIN role, `okg mcp-serve` passed
    readiness, exited 0, logged "starting anyway" three times and served
    a graph it could never read -- the exact pre-fix behaviour this
    change exists to remove. So the override is probed, not trusted: one
    connect and one trivial query, on a path that already pays one
    connect.
    """
    dsn = os.environ.get(MCP_RO_DSN_ENV)
    if not dsn:
        return
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(dsn, connect_timeout=10, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except SubstrateError:
        raise
    except Exception as exc:
        raise _mcp_read_dsn_unusable_error(
            exc, dsn=dsn, surface=surface, deployment=deployment
        ) from exc


#: The plan-cache mode the MCP read role keeps (PACT lane-s-redesign-s1).
READ_PLAN_CACHE_MODE = "force_custom_plan"


def mcp_read_plan_cache_warning(dsn: str, *, surface: str) -> str | None:
    """A warning when the MCP read connection plans with a generic plan.

    Connects the way the server's read pool does (``OKG_MCP_RO_DSN``, else
    ``okg_mcp`` derived from ``dsn``) and reads
    ``current_setting('plan_cache_mode')``. Returns None when it is
    ``force_custom_plan``, else one sentence naming the login role and the
    value. A check that cannot run is itself a warning, never silence. It
    never refuses (decision rolecheck A of PACT lane-s-redesign-s1): every
    read is still sent unprepared; the role setting is the second guard.
    """
    read_dsn = os.environ.get(MCP_RO_DSN_ENV)
    via = MCP_RO_DSN_ENV
    if not read_dsn:
        via = f"the derived {MCP_READ_ROLE} role"
        try:
            from okg.substrate.mcp.server import _derive_role_dsn

            read_dsn = _derive_role_dsn(dsn, role=MCP_READ_ROLE, env_var=MCP_RO_DSN_ENV)
        except Exception as exc:  # noqa: BLE001 -- reported as a warning, never hidden
            return (
                f"{surface}: could not check plan_cache_mode of the MCP read "
                f"role: {type(exc).__name__}: {exc}"
            )
    fields = _dsn_fields(read_dsn)
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(read_dsn, connect_timeout=10, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_user::text, current_setting('plan_cache_mode')"
                )
                role, mode = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 -- reported as a warning, never hidden
        detail = _without_password(str(exc), fields).strip()
        return (
            f"{surface}: could not check plan_cache_mode of the MCP read role "
            f"{fields['user']} ({via}): {type(exc).__name__}: {detail}"
        )
    if str(mode).lower() == READ_PLAN_CACHE_MODE:
        return None
    return (
        f"{surface}: the MCP read role {role} ({via}) runs with "
        f"plan_cache_mode = {mode}, not {READ_PLAN_CACHE_MODE}; a prepared "
        "read may switch to a generic plan and lose its indexes. Fix: "
        f'ALTER ROLE "{role}" SET plan_cache_mode = {READ_PLAN_CACHE_MODE} '
        "(and RESET any IN DATABASE setting that overrides it); `okg doctor "
        "--check database` lists every read role"
    )


def _not_migrated_error(
    readiness: DatabaseReadiness,
    *,
    surface: str,
    deployment: str | None,
) -> SubstrateError:
    if not readiness.schema_present:
        detail = "the `okg` schema does not exist"
    elif not readiness.schema_applied:
        detail = (
            "the `okg` schema exists but its core relations are missing "
            f"({_SCHEMA_APPLIED_RELATION} is absent)"
        )
    else:
        detail = "the `okg.schema_migrations` ledger does not exist"
    err = SubstrateError(
        f"{surface}: this database is not migrated -- {detail}, so there is "
        "no graph to read or write yet",
        data={
            "surface": surface,
            "deployment": deployment,
            **readiness.as_dict(),
        },
        fix_hint=(
            f"run `{migrate_command(deployment)}` once, then re-run this "
            "command. `okg doctor --check preflight` reports the same "
            "readiness without changing anything"
        ),
    )
    err.code = "database_not_migrated"
    return err


def _role_cannot_login_error(
    unusable: tuple[RoleLoginState, ...],
    readiness: DatabaseReadiness,
    *,
    surface: str,
    deployment: str | None,
) -> SubstrateError:
    names = sorted(state.role for state in unusable)
    listed = ", ".join(names)
    deployment_flag = (
        f" --deployment {deployment}"
        if deployment and deployment != AD_HOC_DEPLOYMENT_NAME
        else " --deployment <name>"
    )
    err = SubstrateError(
        f"{surface}: database role(s) {listed} exist but cannot log in "
        "(rolcanlogin = false), so every connection this surface opens as "
        "them would be refused. Migration creates these least-privilege "
        "roles without a credential on purpose -- it never invents one",
        data={
            "surface": surface,
            "deployment": deployment,
            "roles_unusable": names,
            **readiness.as_dict(),
        },
        fix_hint=(
            "on a local database, run `okg db role-passwords --apply"
            f"{deployment_flag}`: it gives the roles random passwords, proves "
            "each login and records them in an owner-only file this command "
            "reads. Otherwise "
            "give the role a credential and let the substrate verify it: "
            f"export {MCP_RO_DSN_ENV} to a DSN for {names[0]} on THIS "
            "database (user, password, host, port, dbname), then run `okg "
            f"runtime bootstrap{deployment_flag}`, which grants LOGIN with "
            "that password and proves the connection before reporting "
            "success. To use a role you manage yourself instead, point "
            f"{MCP_RO_DSN_ENV} at that role's DSN and re-run this command -- "
            "the override replaces the derived role entirely"
        ),
    )
    err.code = "mcp_role_cannot_login"
    return err


def _dsn_fields(dsn: str) -> dict[str, str]:
    """Host, port, user and dbname from a DSN -- never the password.

    A refusal is operator-visible output and a DSN can carry a
    credential, so only these four keys are ever lifted out of one.
    """
    import psycopg

    try:
        parsed = psycopg.conninfo.conninfo_to_dict(dsn)
    except psycopg.Error:
        parsed = {}
    return {
        "host": str(parsed.get("host") or "") or "<unset>",
        "port": str(parsed.get("port") or "") or "<default>",
        "user": str(parsed.get("user") or "") or "<unset>",
        "dbname": str(parsed.get("dbname") or "") or "<unset>",
        "password": str(parsed.get("password") or ""),
    }


def _without_password(text: str, fields: dict[str, str]) -> str:
    """Driver text with the DSN password, if any, masked.

    A malformed-DSN `ProgrammingError` can echo the connection string
    back, so no driver message reaches an operator surface unmasked.
    """
    password = fields.get("password") or ""
    if password:
        return text.replace(password, "***")
    return text


def _readiness_unreadable_error(
    exc: BaseException,
    *,
    dsn: str,
    surface: str,
) -> SubstrateError:
    """42501: reachable, maybe migrated, but this role cannot tell.

    Distinct from ``database_unreachable`` because the operator action
    is completely different: a grant, not starting a server.
    """
    fields = _dsn_fields(dsn)
    role = fields["user"]
    detail = str(
        getattr(getattr(exc, "diag", None), "message_primary", "") or ""
    ).strip()
    if not detail:
        detail = _without_password(str(exc), fields).strip()
    err = SubstrateError(
        f"{surface}: connected to database {fields['dbname']} at "
        f"{fields['host']}:{fields['port']} as role {role}, but that role "
        f"is not permitted to read the readiness relations ({detail}), so "
        "whether this database is migrated cannot be decided. The server "
        "is reachable and the database may already be migrated -- this is "
        "a privilege gap, not an outage",
        data={
            "surface": surface,
            "host": fields["host"],
            "port": fields["port"],
            "role": role,
            "dbname": fields["dbname"],
            "exception_type": type(exc).__name__,
            "sqlstate": getattr(exc, "sqlstate", None),
            "detail": detail,
        },
        fix_hint=(
            "grant that role read access to the readiness relations, as "
            f"the schema owner: `GRANT USAGE ON SCHEMA okg TO {role}; "
            f"GRANT SELECT ON okg.schema_migrations TO {role};` -- or "
            "point this command at the owning DSN instead. `okg runtime "
            "bootstrap --deployment <name>` provisions the substrate's own "
            "least-privilege roles with the grants they need"
        ),
    )
    err.code = "database_readiness_unreadable"
    return err


def _mcp_read_dsn_unusable_error(
    exc: BaseException,
    *,
    dsn: str,
    surface: str,
    deployment: str | None,
) -> SubstrateError:
    fields = _dsn_fields(dsn)
    detail = _without_password(str(exc), fields).strip()
    deployment_flag = (
        f" --deployment {deployment}"
        if deployment and deployment != AD_HOC_DEPLOYMENT_NAME
        else " --deployment <name>"
    )
    err = SubstrateError(
        f"{surface}: {MCP_RO_DSN_ENV} is set, but a connection with it "
        f"failed -- role {fields['user']} on database {fields['dbname']} "
        f"at {fields['host']}:{fields['port']} ({detail}). That DSN "
        "replaces the derived read role entirely, so this server would "
        "start and then fail every read it was asked for",
        data={
            "surface": surface,
            "deployment": deployment,
            "env_var": MCP_RO_DSN_ENV,
            "host": fields["host"],
            "port": fields["port"],
            "role": fields["user"],
            "dbname": fields["dbname"],
            "exception_type": type(exc).__name__,
            "sqlstate": getattr(exc, "sqlstate", None),
            "detail": detail,
        },
        fix_hint=(
            f"correct {MCP_RO_DSN_ENV} so it names a role that can log in "
            "on THIS database (user, password, host, port, dbname), then "
            f"run `okg runtime bootstrap{deployment_flag}`, which grants "
            "LOGIN with that password and proves the connection before "
            f"reporting success. Unset {MCP_RO_DSN_ENV} to go back to the "
            f"derived {MCP_READ_ROLE} role"
        ),
    )
    err.code = "mcp_read_dsn_unusable"
    return err


def _unreachable_error(
    exc: BaseException,
    *,
    dsn: str,
    surface: str,
) -> SubstrateError:
    fields = _dsn_fields(dsn)
    host = fields["host"]
    port = fields["port"]
    err = SubstrateError(
        f"{surface}: cannot reach the deployment database at {host}:{port} "
        f"({type(exc).__name__}), so its readiness is unknown",
        data={
            "surface": surface,
            "host": host,
            "port": port,
            "exception_type": type(exc).__name__,
            "sqlstate": getattr(exc, "sqlstate", None),
        },
        fix_hint=(
            "start or reach the Postgres server named by the deployment DSN, "
            "then re-run. `okg deployment env doctor <name> --json` shows "
            "which DSN this deployment resolved and where it came from"
        ),
    )
    err.code = "database_unreachable"
    return err


__all__ = [
    "AD_HOC_DEPLOYMENT_NAME",
    "MCP_READ_ROLE",
    "MCP_RO_DSN_ENV",
    "DatabaseReadiness",
    "RoleLoginState",
    "check_database_readiness",
    "mcp_read_roles_to_check",
    "migrate_command",
    "pending_migration_report",
    "pending_migration_warning",
    "read_database_readiness",
    "require_database_ready",
    "require_mcp_read_dsn_usable",
]
