"""Database login roles okg provisions, and where their passwords live.

okg connects to a deployment database as a few fixed roles: ``okg_mcp`` (the
MCP read pool) and ``app_rw`` (the MCP write pool). ``okg migrate`` creates
them, together with ``app_ro`` and ``catalog_admin``, as ``NOLOGIN``. Each one
needs ``LOGIN`` and a password before okg can use it.

Those passwords used to be fixed strings committed in this file, and
``okg mcp-serve`` used them automatically for any loopback database. On a
shared host that let any local account log in as these roles (issue #3273).
Now:

* :func:`provision_role_passwords` (``okg db role-passwords --apply``) gives
  each role a random password and records the role connection strings in an
  owner-only (mode 0600) secrets file. It never prints a password, and it
  sends PostgreSQL only the SCRAM verifier, so DDL statement logging cannot
  capture the password either.
* :func:`resolve_role_dsn` looks a role's connection string up in this order:
  the explicit environment variable (``OKG_MCP_RO_DSN`` / ``OKG_MCP_RW_DSN``),
  then the secrets file, then -- only when ``OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1``
  marks a single-user development machine -- the built-in passwords below.
  Otherwise it refuses and names the command that provisions passwords.
* :func:`builtin_password_findings` (``okg doctor``) reports any login role
  that still accepts its built-in password.

The secrets file is separate from ``.okg/local.env``: that file holds the
deployment's own settings and may be copied or shared with a checkout, while
this one holds credentials for one database server and lives in the user's
configuration directory, keyed by that server.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate.db import dsn as dsn_module
from okg.substrate.errors import SubstrateError


#: The well-known passwords earlier releases set on these roles. They are
#: public, so okg uses them only behind ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, and
#: ``okg doctor`` warns about any role that still accepts one.
LOCAL_DATABASE_ROLE_PASSWORDS = {
    "okg_mcp": "okg_mcp",
    "app_rw": "okg_rw",
    "app_ro": "okg_ro",
    "catalog_admin": "okg_admin",
}

#: Set to ``1`` to let okg derive (and ``--builtin-dev-passwords`` set) the
#: built-in passwords. Only for a database no other account can reach.
ALLOW_BUILTIN_ROLE_PASSWORDS_ENV = "OKG_ALLOW_BUILTIN_ROLE_PASSWORDS"

#: Overrides the secrets file location (one file, one database).
ROLE_SECRETS_FILE_ENV = "OKG_ROLE_SECRETS_FILE"

ROLE_SECRETS_SCHEMA = "okg.role_secrets/v1"

#: Roles okg itself logs in as, and the variable that overrides each one.
ROLE_DSN_ENV = {
    "okg_mcp": "OKG_MCP_RO_DSN",
    "app_rw": "OKG_MCP_RW_DSN",
}

#: Roles okg created with a built-in password in earlier releases. The two in
#: ROLE_DSN_ENV always get LOGIN; the others are rotated only when they
#: already have LOGIN, so provisioning never opens a login nothing uses.
MANAGED_ROLES = ("okg_mcp", "app_rw", "app_ro", "catalog_admin")

ROTATE_COMMAND = "okg db role-passwords --apply"

#: The page whose step 4 sets the two roles up on a database on another host.
REMOTE_ROLES_DOC = "docs/start/quickstart.md"


def remote_roles_hint() -> str:
    """How to give okg's two login roles a password on a non-local server.

    okg refuses to do it itself there (issue #3480): the roles are
    server-wide, so a password set for one deployment would change every
    database on a server okg does not own. The migration creates both roles
    ``NOLOGIN``; an administrator gives them ``LOGIN`` and a password, and
    the operator passes the two connection strings in the environment.
    """
    from okg.substrate.support.user_hints import doc_reference

    return (
        "these roles are server-wide, so set them up by hand. As an "
        "administrator of that server, in psql: `ALTER ROLE okg_mcp WITH "
        "LOGIN;` and `ALTER ROLE app_rw WITH LOGIN;`, then `\\password "
        "okg_mcp` and `\\password app_rw` (psql sends only a hash of each "
        "password). Then put "
        "OKG_MCP_RO_DSN=postgresql://okg_mcp:<password>@<host>:<port>/<dbname> "
        "(reads) and the same with app_rw as OKG_MCP_RW_DSN (writes) in the "
        "deployment's .okg/local.env, readable only by you (chmod 600), and "
        "run `okg agent connect --deployment <name>`; the agent entry "
        "holds only that file's "
        "path. "
        f"Steps: {doc_reference(REMOTE_ROLES_DOC)}, step 4, "
        "'A database on another host'."
    )


_TRUE_VALUES = {"1", "true", "yes", "on"}
_PASSWORD_BYTES = 32
_PROBE_TIMEOUT_SECONDS = 5


def _role_dsn(dsn: str, role: str, password: str) -> str:
    """``dsn`` with its user and password replaced, in the DSN's own form."""
    return dsn_module.parse(dsn).with_params(user=role, password=password).render()


def rotate_command(deployment: str | None = None) -> str:
    """The command that gives this deployment's roles random passwords."""
    if deployment and deployment != "default":
        return f"{ROTATE_COMMAND} --deployment {deployment}"
    return ROTATE_COMMAND


def builtin_role_passwords_allowed(
    environ: Mapping[str, str] | None = None,
) -> bool:
    """True when the operator marked this machine single-user development."""
    env = os.environ if environ is None else environ
    return str(env.get(ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, "")).strip().lower() in (
        _TRUE_VALUES
    )


def _is_local_entry(value: str) -> bool:
    value = value.strip().lower()
    if value.startswith("/") or value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def is_loopback_host(host: str | None) -> bool:
    """True when every host libpq could try is local.

    A Unix socket, ``localhost`` and loopback addresses are local. A
    multi-host value (``/tmp,10.0.0.5``) is local only when EVERY entry is,
    because libpq falls through to the later entries. An empty host means
    libpq's default, which ``PGHOST`` / ``PGHOSTADDR`` can point anywhere, so
    it is local only when those are unset or local too.
    """
    value = (host or "").strip()
    if not value:
        for name in ("PGHOST", "PGHOSTADDR"):
            env_value = os.environ.get(name, "").strip()
            if env_value and not is_loopback_host(env_value):
                return False
        return True
    return all(_is_local_entry(entry) for entry in value.split(",") if entry.strip())


def dsn_is_local(dsn: str) -> bool:
    """True when the DSN can only reach a server on this machine.

    Checks ``host`` and also ``hostaddr``, which libpq connects to instead of
    resolving ``host``.
    """
    parsed = dsn_module.parse(dsn)
    hostaddr = parsed.params.get("hostaddr")
    if hostaddr and not is_loopback_host(hostaddr):
        return False
    return is_loopback_host(parsed.host)


def database_target(dsn: str) -> dict[str, str]:
    """The SERVER a DSN names, normalized for comparison.

    A PostgreSQL role and its password belong to the whole server, not to one
    database, so the secrets are keyed by server: rotating for one database
    must be seen by every other database on that server. Every loopback
    spelling (socket, ``localhost``, ``127.0.0.1``, ``::1``) collapses to
    ``loopback``: they reach the same local server.
    """
    parsed = dsn_module.parse(dsn)
    host = parsed.host or ""
    return {
        "host": "loopback" if is_loopback_host(host) else host.lower(),
        "port": parsed.port or "5432",
    }


def role_secrets_path(
    dsn: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Where the role secrets for the server ``dsn`` names are kept."""
    env = os.environ if environ is None else environ
    override = env.get(ROLE_SECRETS_FILE_ENV)
    if override:
        return Path(override).expanduser()
    config_home = env.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    target = database_target(dsn)
    name = "-".join(
        re.sub(r"[^A-Za-z0-9._]+", "_", target[key]) or "_"
        for key in ("host", "port")
    )
    return Path(config_home).expanduser() / "okg" / "role-secrets" / f"{name}.json"


def _secrets_error(message: str, path: Path, fix_hint: str) -> SubstrateError:
    err = SubstrateError(message, data={"path": str(path)}, fix_hint=fix_hint)
    err.code = "role_secrets_unusable"
    return err


def read_role_secrets(dsn: str, *, path: Path | None = None) -> dict[str, str]:
    """Role name -> connection string from the secrets file, or ``{}``.

    A missing file is ``{}``. A file that exists but is a symlink, is readable
    by another account, belongs to another user, is malformed, or names a
    different server is refused: using it would either leak or misdirect a
    credential.
    """
    secrets_path = path or role_secrets_path(dsn)
    try:
        info = secrets_path.lstat()
    except FileNotFoundError:
        return {}
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise _secrets_error(
            "the role secrets file is not a regular file",
            secrets_path,
            "replace it with a regular file by re-running "
            f"`{ROTATE_COMMAND}`",
        )
    if info.st_uid != os.geteuid():
        raise _secrets_error(
            "the role secrets file belongs to another user",
            secrets_path,
            f"run `{ROTATE_COMMAND}` as the account that serves this database",
        )
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise _secrets_error(
            "the role secrets file is readable or writable by other accounts "
            f"(mode {stat.S_IMODE(info.st_mode):04o}); its passwords must be "
            "treated as exposed",
            secrets_path,
            f"chmod 600 the file, then run `{ROTATE_COMMAND}` to replace the "
            "exposed passwords",
        )
    try:
        payload = json.loads(secrets_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _secrets_error(
            f"the role secrets file cannot be read: {type(exc).__name__}",
            secrets_path,
            f"re-run `{ROTATE_COMMAND}` to rewrite it",
        ) from exc
    roles = payload.get("roles") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != ROLE_SECRETS_SCHEMA
        or not isinstance(roles, dict)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in roles.items()
        )
    ):
        raise _secrets_error(
            "the role secrets file is not an okg role secrets file",
            secrets_path,
            f"move it aside and re-run `{ROTATE_COMMAND}`",
        )
    recorded = payload.get("target")
    expected = database_target(dsn)
    if recorded != expected:
        raise _secrets_error(
            "the role secrets file was written for a different database server "
            f"({recorded!r}, expected {expected!r})",
            secrets_path,
            f"unset {ROLE_SECRETS_FILE_ENV} or point it at this server's "
            f"file, or run `{ROTATE_COMMAND}` for this server",
        )
    return dict(roles)


def prepare_secrets_directory(secrets_path: Path) -> Path:
    """Create the secrets file's directory (0700) and refuse an unsafe one.

    The directory must belong to this user and must not be writable by
    anyone else: another account that can write it could replace the file.
    Called before any role password changes, so a refusal here cannot leave
    the server holding passwords nobody recorded.
    """
    directory = secrets_path.parent
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise _secrets_error(
            "the role secrets directory is not a plain directory",
            directory,
            "replace it with a directory you own, mode 0700",
        )
    if info.st_uid != os.geteuid():
        raise _secrets_error(
            "the role secrets directory belongs to another user",
            directory,
            f"point {ROLE_SECRETS_FILE_ENV} at a path in a directory you own",
        )
    if stat.S_IMODE(info.st_mode) & 0o022:
        raise _secrets_error(
            "the role secrets directory is writable by other accounts "
            f"(mode {stat.S_IMODE(info.st_mode):04o})",
            directory,
            "chmod 700 the directory, then re-run the command",
        )
    return directory


def write_role_secrets(
    dsn: str,
    role_dsns: Mapping[str, str],
    *,
    remove: Iterable[str] = (),
    path: Path | None = None,
) -> Path:
    """Record role connection strings for ``dsn``'s server, mode 0600.

    Merges into an existing file for the same server: entries for other
    roles are kept, ``remove`` drops entries. The write is atomic (a temporary
    file in the same directory, fsynced, then renamed), the directory is
    created mode 0700, and the final mode is checked rather than assumed.
    Reusable by any setup step that provisions role passwords.
    """
    secrets_path = path or role_secrets_path(dsn)
    roles = read_role_secrets(dsn, path=secrets_path)
    for role in remove:
        roles.pop(role, None)
    roles.update(role_dsns)
    payload = {
        "schema": ROLE_SECRETS_SCHEMA,
        "target": database_target(dsn),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "roles": dict(sorted(roles.items())),
    }
    directory = prepare_secrets_directory(secrets_path)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{secrets_path.name}.", suffix=".tmp", dir=directory,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, secrets_path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    directory_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    mode = stat.S_IMODE(secrets_path.lstat().st_mode)
    if mode != 0o600:
        raise _secrets_error(
            f"the role secrets file was written with mode {mode:04o}, not 0600",
            secrets_path,
            "chmod 600 the file",
        )
    return secrets_path


def _not_provisioned_error(role: str, env_var: str, path: Path) -> SubstrateError:
    err = SubstrateError(
        f"no password is provisioned for database role {role!r}: {env_var} is "
        "not set and the role secrets file has no entry for it. okg no longer "
        "uses the built-in role passwords, which are public",
        data={
            "role": role,
            "env_var": env_var,
            "secrets_file": str(path),
            "allow_builtin_env": ALLOW_BUILTIN_ROLE_PASSWORDS_ENV,
        },
        fix_hint=(
            f"run `{ROTATE_COMMAND}` (add --deployment <name>) to give the roles "
            f"random passwords, or set {env_var} to the role's connection "
            f"string. On a single-user development machine only, "
            f"{ALLOW_BUILTIN_ROLE_PASSWORDS_ENV}=1 restores the built-in "
            "passwords"
        ),
    )
    err.code = "role_password_not_provisioned"
    return err


def resolve_role_dsn(dsn: str, *, role: str, env_var: str) -> str:
    """The connection string okg uses for ``role`` on ``dsn``'s database.

    Order: ``env_var``; then, for a loopback database, the secrets file; then
    the built-in password when ALLOW_BUILTIN_ROLE_PASSWORDS_ENV is set.
    Anything else is refused. A non-loopback database always needs
    ``env_var``: managed hosts manage their own role credentials.
    """
    override = os.environ.get(env_var)
    if override:
        return override
    host = dsn_module.parse(dsn).host or ""
    if not dsn_is_local(dsn):
        raise SubstrateError(
            f"{env_var} is required: the database is not only on this "
            f"machine (host {host!r}), and okg sets role passwords only on a "
            f"local database, because "
            f"role {role!r} is shared by every database on the server",
            data={"host": host, "role": role, "env_var": env_var},
            fix_hint=remote_roles_hint(),
        )
    path = role_secrets_path(dsn)
    stored = read_role_secrets(dsn, path=path).get(role)
    if stored:
        # The file is keyed by server; the role logs in to whichever database
        # ``dsn`` names, through the same host spelling.
        recorded = dsn_module.parse(stored)
        return dsn_module.parse(dsn).with_params(
            user=recorded.user or role, password=recorded.password,
        ).render()
    password = LOCAL_DATABASE_ROLE_PASSWORDS.get(role)
    if password is not None and builtin_role_passwords_allowed():
        return _role_dsn(dsn, role, password)
    raise _not_provisioned_error(role, env_var, path)


def stored_role_dsn(dsn: str, role: str) -> str | None:
    """The secrets-file connection string for ``role``, if one is recorded."""
    return read_role_secrets(dsn).get(role) or None


_REJECTION_MARKERS = (
    "password authentication failed",
    "is not permitted to log in",
)


def _login(dsn: str) -> tuple[str, str | None]:
    """``("accepted", current_user)``, ``("rejected", None)`` or
    ``("inconclusive", None)``.

    Only a server that answered "password authentication failed" or "not
    permitted to log in" counts as a rejection. A refused pg_hba rule, an
    unreachable server or a timeout proves nothing about the password, so it is
    reported as inconclusive rather than as a safe role. The markers are the
    server's English messages; a server with another ``lc_messages`` reads as
    inconclusive, the safe direction.
    """
    import psycopg

    from okg.substrate.db.connection import connect as db_connect

    try:
        # The substrate connection factory: the same keepalives
        # and session posture as every other connection. Its errors are
        # psycopg's own (a DSN it cannot parse is a psycopg.ProgrammingError),
        # so the rejection markers below still read the server's message.
        with db_connect(
            dsn, connect_timeout=_PROBE_TIMEOUT_SECONDS, autocommit=True,
            application_name="okg-role-password-probe",
        ) as conn:
            row = conn.execute("SELECT current_user").fetchone()
            return "accepted", (str(row[0]) if row else None)
    except psycopg.Error as exc:
        text = str(exc).lower()
        if any(marker in text for marker in _REJECTION_MARKERS):
            return "rejected", None
        return "inconclusive", None


def _probe_login(dsn: str) -> str | None:
    """``current_user`` if the DSN authenticates, else ``None``."""
    status, user = _login(dsn)
    return user if status == "accepted" else None


def require_role_dsn_usable(dsn: str, *, role: str, env_var: str, surface: str) -> None:
    """Resolve ``role``'s connection string and refuse a password the server rejects.

    A stale secrets-file entry (rotated on the server by another tool) would
    otherwise start a server whose every pool connection fails. Only a
    definite rejection ("password authentication failed" / "not permitted to
    log in") refuses here; an unreachable server or a pg_hba refusal is left
    to the readiness and pool checks that already report those.
    """
    role_dsn = resolve_role_dsn(dsn, role=role, env_var=env_var)
    status, _user = _login(role_dsn)
    if status != "rejected":
        return
    err = SubstrateError(
        f"{surface}: the database server rejected the password okg resolved "
        f"for role {role!r}",
        data={"role": role, "env_var": env_var, "login": status},
        fix_hint=(
            f"run `{ROTATE_COMMAND}` (add --deployment <name>) to set and record "
            f"a fresh password, or set {env_var} to a working connection string"
        ),
    )
    err.code = "role_password_rejected"
    raise err


def _role_login_states(conn: Any) -> dict[str, bool]:
    rows = conn.execute(
        "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles "
        "WHERE rolname = ANY(%s)",
        (list(MANAGED_ROLES),),
    ).fetchall()
    return {str(name): bool(can_login) for name, can_login in rows}


def set_role_passwords(conn: Any, passwords: Mapping[str, str]) -> None:
    """``ALTER ROLE ... LOGIN PASSWORD`` for each role, then commit.

    The password is turned into a SCRAM-SHA-256 verifier on the client
    (libpq ``PQencryptPasswordConn``), so the statement the server executes,
    and any DDL statement log, carries the verifier and never the password.
    Every role's cluster-wide lock is held for the ALTERs (issue #1985).
    ``conn`` must not be in autocommit mode.
    """
    from psycopg import sql

    from okg.substrate.db.role_lock import lock_roles_for_alter

    verifiers = {
        role: conn.pgconn.encrypt_password(
            password.encode("utf-8"), role.encode("utf-8"), b"scram-sha-256",
        ).decode("ascii")
        for role, password in passwords.items()
    }
    with conn.transaction():
        lock_roles_for_alter(conn, verifiers)
        for role, verifier in verifiers.items():
            conn.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(verifier),
                )
            )
    conn.commit()


def provision_role_passwords(
    dsn: str,
    *,
    builtin: bool = False,
    deployment: str | None = None,
    secrets_path: Path | None = None,
) -> dict[str, Any]:
    """Give okg's login roles fresh passwords and record them.

    ``dsn`` must connect as a role allowed to alter these roles (the local
    superuser, or ``CREATEROLE`` with ``ADMIN OPTION``). For each managed role:
    a role named by an explicit environment variable is left to the operator;
    ``okg_mcp`` and ``app_rw`` get ``LOGIN``; ``app_ro`` and ``catalog_admin``
    are rotated only if they already have ``LOGIN``. Each new password is
    random (``builtin=True`` sets the built-in one instead, and requires
    ALLOW_BUILTIN_ROLE_PASSWORDS_ENV), is sent to the server only as a SCRAM
    verifier, and is proved by a real login before it is recorded.

    Returns a report with no password in it: per role, the action, the
    password length, and the login check.
    """
    from okg.substrate.db.connection import connect as db_connect

    host = dsn_module.parse(dsn).host or ""
    if not dsn_is_local(dsn):
        err = SubstrateError(
            f"okg sets role passwords only on a local database, because "
            f"these roles are shared by every database on the server; "
            f"the database is not only on this machine (host {host!r})",
            data={"host": host},
            fix_hint=remote_roles_hint(),
        )
        err.code = "role_passwords_managed_host"
        raise err
    if builtin and not builtin_role_passwords_allowed():
        err = SubstrateError(
            "--builtin-dev-passwords sets public passwords and is refused "
            f"unless {ALLOW_BUILTIN_ROLE_PASSWORDS_ENV}=1 marks this machine as "
            "single-user development",
            data={"allow_builtin_env": ALLOW_BUILTIN_ROLE_PASSWORDS_ENV},
            fix_hint="drop --builtin-dev-passwords to set random passwords",
        )
        err.code = "builtin_role_passwords_refused"
        raise err

    path = secrets_path or role_secrets_path(dsn)
    # Fail before touching any role if the existing file or its directory is
    # unusable: a refusal after the ALTER would leave passwords nobody holds.
    read_role_secrets(dsn, path=path)
    prepare_secrets_directory(path)

    report: dict[str, Any] = {
        "schema": "okg.role_passwords_apply/v1",
        "mode": "builtin" if builtin else "random",
        "target": database_target(dsn),
        "roles": {},
    }
    planned: dict[str, str] = {}
    with db_connect(dsn, deployment=deployment) as conn:
        states = _role_login_states(conn)
        for role in MANAGED_ROLES:
            env_var = ROLE_DSN_ENV.get(role)
            if role not in states:
                report["roles"][role] = {"action": "skipped", "reason": "role_missing"}
            elif env_var and os.environ.get(env_var):
                report["roles"][role] = {
                    "action": "skipped",
                    "reason": "env_override",
                    "env_var": env_var,
                }
            elif role in ROLE_DSN_ENV or states[role]:
                planned[role] = (
                    LOCAL_DATABASE_ROLE_PASSWORDS[role]
                    if builtin
                    else secrets.token_urlsafe(_PASSWORD_BYTES)
                )
            else:
                report["roles"][role] = {"action": "skipped", "reason": "nologin"}
        if not planned:
            if not any(role in states for role in ROLE_DSN_ENV):
                err = SubstrateError(
                    "this database has none of okg's login roles yet",
                    data={"roles": report["roles"]},
                    fix_hint="run `okg migrate --apply` first, then re-run",
                )
                err.code = "role_passwords_roles_missing"
                raise err
        else:
            set_role_passwords(conn, planned)

    role_dsns: dict[str, str] = {}
    failed: list[str] = []
    for role, password in planned.items():
        role_dsn = _role_dsn(dsn, role, password)
        connected_as = _probe_login(role_dsn)
        report["roles"][role] = {
            "action": "set_builtin" if builtin else "rotated",
            "login": True,
            "password_length": len(password),
            "login_verified": connected_as == role,
        }
        if connected_as == role:
            role_dsns[role] = role_dsn
        else:
            failed.append(role)

    if builtin:
        # Built-in passwords are derived, not stored: drop stale entries so
        # the file cannot shadow them.
        if path.exists() and planned:
            write_role_secrets(dsn, {}, remove=planned, path=path)
    elif planned:
        # A role whose new login failed loses its entry: the recorded password
        # is no longer the server's, and a stale entry would look usable.
        write_role_secrets(dsn, role_dsns, remove=failed, path=path)
    report["secrets_file"] = str(path)
    if path.exists():
        report["secrets_file_mode"] = f"{stat.S_IMODE(path.stat().st_mode):04o}"
    if failed:
        err = SubstrateError(
            "the new password was set but a login with it failed for: "
            + ", ".join(sorted(failed)),
            data=report,
            fix_hint=(
                "check pg_hba.conf allows password logins for these roles from "
                f"this host, then re-run `{ROTATE_COMMAND}`"
            ),
        )
        err.code = "role_password_login_failed"
        raise err
    report["ok"] = True
    return report


def builtin_password_findings(dsn: str) -> list[dict[str, Any]]:
    """Login roles on ``dsn``'s server that accept a built-in password.

    Each role with ``LOGIN`` is probed with a real login using its built-in
    password. A role that also accepts a random wrong password is reported
    separately: the server is not checking passwords for it (``trust``
    authentication), so rotating the password would not help.
    """
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(
        dsn, connect_timeout=_PROBE_TIMEOUT_SECONDS, autocommit=True,
        application_name="okg-role-password-doctor",
    ) as conn:
        states = _role_login_states(conn)
    findings: list[dict[str, Any]] = []
    for role in MANAGED_ROLES:
        if not states.get(role):
            continue
        builtin = LOCAL_DATABASE_ROLE_PASSWORDS[role]
        status, user = _login(_role_dsn(dsn, role, builtin))
        if status == "inconclusive":
            findings.append({
                "code": "role_password_probe_inconclusive",
                "role": role,
                "message": (
                    f"could not tell whether role {role!r} accepts its built-in "
                    "password: the login attempt failed for another reason "
                    "(pg_hba rule, connection, or timeout)"
                ),
                "fix_hint": (
                    "re-run `okg doctor` with a TCP DSN (127.0.0.1) to the same "
                    f"server, or run `{ROTATE_COMMAND}` to replace the password"
                ),
            })
            continue
        if status != "accepted" or user != role:
            continue
        wrong = secrets.token_urlsafe(_PASSWORD_BYTES)
        if _probe_login(_role_dsn(dsn, role, wrong)) == role:
            findings.append({
                "code": "role_password_not_checked",
                "role": role,
                "message": (
                    f"role {role!r} logs in with any password: the server does "
                    "not check passwords for it (trust authentication)"
                ),
                "fix_hint": (
                    "change this role's pg_hba.conf method to scram-sha-256"
                ),
            })
            continue
        findings.append({
            "code": "builtin_role_password",
            "role": role,
            "message": (
                f"role {role!r} still accepts its built-in password, which is "
                "public; any account that can reach this server can log in as it"
            ),
            "fix_hint": f"run `{ROTATE_COMMAND}` (add --deployment <name>)",
        })
    return findings


__all__ = [
    "ALLOW_BUILTIN_ROLE_PASSWORDS_ENV",
    "LOCAL_DATABASE_ROLE_PASSWORDS",
    "MANAGED_ROLES",
    "ROLE_DSN_ENV",
    "ROLE_SECRETS_FILE_ENV",
    "ROLE_SECRETS_SCHEMA",
    "ROTATE_COMMAND",
    "builtin_password_findings",
    "builtin_role_passwords_allowed",
    "database_target",
    "dsn_is_local",
    "is_loopback_host",
    "prepare_secrets_directory",
    "provision_role_passwords",
    "read_role_secrets",
    "require_role_dsn_usable",
    "resolve_role_dsn",
    "role_secrets_path",
    "rotate_command",
    "set_role_passwords",
    "stored_role_dsn",
    "write_role_secrets",
]
