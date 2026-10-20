"""DSN resolution + connection helpers.

Per docs/reference/security.md: the substrate NEVER logs the password
portion of a DSN. This module resolves env-var references, never
embeds literals.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import psycopg

from chronos_core.pg_session import (
    DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
    SEARCH_PATH_OPTION,
    set_idle_in_transaction_timeout_resolver,
)
from okg.substrate.db import dsn as _shared_dsn
from okg.substrate.errors import SubstrateError


_DSN_ENV_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
logger = logging.getLogger(__name__)


def dsn_from_env(env_name: str) -> str:
    """Resolve an env var name to a DSN string.

    Raises SubstrateError with code 'dsn_not_set' if unset.
    """
    if not _DSN_ENV_PATTERN.match(env_name):
        raise SubstrateError(
            f"invalid dsn_env name: {env_name!r}",
            data={"env_name": env_name},
            fix_hint="dsn_env must match ^[A-Z][A-Z0-9_]*$",
        )
    val = os.environ.get(env_name)
    if not val:
        err = SubstrateError(
            f"dsn env var {env_name} is not set",
            data={"env_name": env_name},
            fix_hint=f"export {env_name}=<dsn>",
        )
        err.code = "dsn_not_set"
        raise err
    return val


#: The password masker for ONE DSN string: the shared module's ``redact``.
#: It was a private regex masker here until the DSN migration (design section
#: 1.6 item 2): a raw ``@`` in a URL password showed the part after it, and
#: ``sslpassword=``, ``?sslpassword=`` and ``PGPASSWORD=`` were not masked.
#: ``okg.substrate.db.dsn`` masks all of them. Kept as a name because callers
#: import it; they move to ``okg.substrate.db.dsn`` in later migration PRs.
#:
#: This stays the SINGLE-DSN masker on purpose: an unquoted keyword password
#: runs to the next `` key=`` token or the end of the input, even across a
#: line break, so a pasted DSN whose password holds a raw line break
#: (``password=my\nsecret dbname=okg``) cannot show its second line. Callers
#: that mask multi-line text (stderr, exception text) should call
#: ``okg.substrate.db.dsn.redact_text``, which keeps every line.
redact = _shared_dsn.redact
_redact = _shared_dsn.redact

def redact_value(value: Any) -> Any:
    """Mask DSN secrets in every string inside dicts, lists and tuples.

    Each string goes through ``okg.substrate.db.dsn.redact_text``, which
    works line by line, so a diagnostic no longer loses the lines after an
    unquoted ``password=`` (design section 1.6 item 1). It is called with
    NO whole-value flag rule (``dsn_flags=()``): this runs over resume
    commands and error hints that name ``--dsn`` on purpose, such as
    ``--dsn "${OKG_DEPLOYMENT_UP_DSN:?export ...}"`` and "pass a valid
    --dsn or use ...". Replacing the word after ``--dsn`` would make the
    first unrunnable and the second nonsense. A DSN after ``--dsn`` still
    has its password masked by the URL and keyword rules. Dictionary keys
    are kept; other types pass through.
    """
    if isinstance(value, str):
        return _shared_dsn.redact_text(value, dsn_flags=())
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    return value


# TCP keepalive defaults — per
# `add-publisher-leases-with-heartbeat` (2026-04-29). Sets a
# ~60s upper bound on orphan-Postgres-backend lifetime when the
# client process dies abnormally (kill -9, parent crash, network
# drop). Without these, OS defaults can leave a backend in `idle`
# for minutes-hours, holding any locks the dead process held.
#
# Math: keepalives_idle (30s) + keepalives_count (3) *
# keepalives_interval (10s) = 60s upper bound on detection.
_KEEPALIVE_KWARGS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}

# Default connect-time deadline. Without this, psycopg.connect blocks
# indefinitely on TCP-level hangs — observed during Phase 0/1 cutover
# verification: worker process alive with 0 CPU + 0 active queries,
# publisher_cycle workflow stuck PENDING for 9+ minutes because a
# `ThreadPoolExecutor.__exit__` was joining a pool worker stuck in
# `psycopg.connect` with no timeout. 10 seconds is much larger than
# any legitimate local Postgres connect (typically < 100ms) but small
# enough that the stuck case fails fast and the cycle errors out
# cleanly rather than holding the partition-queue slot forever.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10

# Bound on how long one of okg's own sessions may sit `idle in transaction`
# (issue #1310). A psycopg connection opened with autocommit=False starts a
# transaction on its first statement; a caller that reads and then does slow
# non-database work leaves the session idle in transaction, and any advisory
# or row lock the transaction took stays held for the whole wait. Observed on
# okg-workspace: two sessions on `okg.deployment_promotion_fences` held
# transaction-scoped advisory locks for ~57 minutes with 41 connections queued
# behind them, and the same signature reproduced during a clean `okg provision`
# at 266 seconds.
#
# `idle_in_transaction_session_timeout` makes that self-healing: PostgreSQL
# terminates the offending backend, releases its locks, and the caller gets a
# loud SQLSTATE 25P03 instead of a silently jammed pool. It only fires while
# the session is waiting on the client, never while a statement is running, so
# it cannot interrupt legitimate work.
#
# The default matches the posture already applied by hand to the okg-workspace
# cluster during the 2026-08-23 outage recovery, so shipping it neither weakens
# that deployment nor depends on `postgresql.conf`. A deployment that genuinely
# needs a longer hold raises it in its own `deployment.yaml`:
#
#     postgres:
#       idle_in_transaction_session_timeout_ms: 900000
#
# `0` disables the bound for that deployment (or for one process, via the env
# override), and is the only way to get the previous unbounded behavior back.
#
# The number itself now lives in `chronos_core.pg_session`, re-exported here
# under its established name. Chronos's own adapter factory opens okg sessions
# too -- ~18 production call sites reach it through `ChronosGraphStore.connect`
# -- and carried no bound at all until issue #1997. One literal, imported by
# both factories, is what keeps them from drifting apart again.
IDLE_IN_TRANSACTION_TIMEOUT_ENV = "OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS"


def _non_negative_int_env(name: str, default: int | None) -> int | None:
    """Read a non-negative integer env knob, refusing a set-but-invalid value.

    An empty string means "unset" (the ``VAR= cmd`` shell idiom for clearing an
    inherited value). Every other non-integer or negative value raises rather
    than falling back to the default — a masked default here would silently
    restore the unbounded behavior this knob exists to remove.
    """
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{name} must be a non-negative integer, got {raw!r}",
            data={"env": name, "value": raw},
            fix_hint=f"unset {name} or set it to a non-negative integer",
        ) from exc
    if value < 0:
        raise SubstrateError(
            f"{name} must be a non-negative integer, got {raw!r}",
            data={"env": name, "value": raw},
            fix_hint=f"unset {name} or set it to a non-negative integer",
        )
    return value


#: Resolved manifest values, keyed by deployment name. `resolve_deployment` is
#: NOT cached — it walks the search roots, re-reads `.okg/local.env`, and
#: rewrites `sys.path` (evicting stale deployment modules) on every call. The
#: worker polls `promotion_fence_status` every two seconds for the whole life
#: of a promotion fence, so doing that per connection would put filesystem
#: discovery and module eviction on a hot path. This is a boot-time tuning
#: knob: a manifest edit takes effect on the next process, not mid-run.
_MANIFEST_IDLE_TIMEOUT_CACHE: dict[str, int | None] = {}


def _manifest_idle_in_transaction_timeout_ms(deployment: str) -> int | None:
    """Read ``postgres.idle_in_transaction_session_timeout_ms`` for a deployment.

    Returns None when the deployment name does not resolve to a manifest.
    Connecting is not the place to refuse an unknown deployment name — every
    caller that needs the manifest resolves it itself and refuses there — so
    that one case falls back to the built-in default.

    A manifest that IS found and declares a malformed value raises
    ``DeploymentConfigError`` through this function. Swallowing it would leave
    the operator believing a bound they wrote is in force when it is not.
    """
    if deployment in _MANIFEST_IDLE_TIMEOUT_CACHE:
        return _MANIFEST_IDLE_TIMEOUT_CACHE[deployment]
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    try:
        manifest = resolve_deployment(deployment)
    except DeploymentResolutionError:
        logger.debug(
            "deployment %r does not resolve to a manifest; using the "
            "built-in idle-in-transaction bound",
            deployment,
            exc_info=True,
        )
        _MANIFEST_IDLE_TIMEOUT_CACHE[deployment] = None
        return None
    value = getattr(manifest, "postgres_idle_in_transaction_timeout_ms", None)
    _MANIFEST_IDLE_TIMEOUT_CACHE[deployment] = value
    return value


def idle_in_transaction_timeout_ms(
    *, deployment: str | None = None,
) -> int:
    """Resolve the idle-in-transaction bound in milliseconds (0 disables).

    Precedence, highest first: the process env override, the deployment
    manifest, then `DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS`. Explicit process
    env winning over the manifest matches the `chronos:` block's documented
    precedence.

    An unnamed `deployment` falls back to ``OKG_DEPLOYMENT``, which the release
    launcher stamps into every worker environment. Without that fallback the
    shared connection pool — which knows a DSN but not a deployment name —
    could never see the manifest value, and the manifest key would advertise a
    reach it did not have.
    """
    from_env = _non_negative_int_env(IDLE_IN_TRANSACTION_TIMEOUT_ENV, None)
    if from_env is not None:
        return from_env
    name = deployment or os.environ.get("OKG_DEPLOYMENT") or ""
    if name.strip():
        from_manifest = _manifest_idle_in_transaction_timeout_ms(name.strip())
        if from_manifest is not None:
            return from_manifest
    return DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS


# Chronos's own adapter factory opens okg sessions too (every
# `ChronosGraphStore.connect`, and Chronos's interval GC). `chronos_core` must
# not import okg, so it cannot read the env override or the manifest key
# itself; it asks the resolver installed here. Without this line those
# sessions carried the bare 5-minute default whatever the operator configured
# (issue #2920). Installed at import: every okg path that opens a Chronos
# session imports this module first.
set_idle_in_transaction_timeout_resolver(idle_in_transaction_timeout_ms)


def _dsn_options(dsn: str | None) -> str:
    """Return the libpq ``options`` already carried by a DSN, or "".

    Parsed by ``okg.substrate.db.dsn`` (cached per DSN string, so the two
    reads per connect cost one libpq parse).
    """
    if not dsn:
        return ""
    parsed = _shared_dsn.try_parse(dsn)
    if parsed is None:
        # An unparseable DSN is psycopg.connect's error to raise, with its
        # own message. "" here only means "no options to preserve"; it never
        # suppresses a connection failure.
        logger.debug("could not read options from the DSN")
        return ""
    return parsed.options or ""


#: Matches a `search_path` set through a libpq ``options`` fragment, in either
#: spelling libpq accepts (`-c search_path=...` and `-csearch_path=...`).
_SUPPLIED_SEARCH_PATH = re.compile(r"-c\s*search_path=(\S+)")


def _warn_if_supplied_search_path_demotes_public(*sources: str) -> None:
    """Warn when a caller's or DSN's own `search_path` does not resolve
    `public` first.

    The pin is a default, not a lock: an explicit `search_path` in the DSN or
    in a caller's `options` is emitted after it and wins, which is deliberate.
    But Chronos's physical tables live in `public` and the substrate names them
    there in 51 places, so a supplied path that resolves something else first
    breaks reads and writes in ways whose first symptom is a confusing
    `UndefinedTable` or `DuplicateColumn` far from the connection that caused
    it (issue #1968).

    Containing `public` is not enough. `okg,public` reproduces #1968 exactly:
    `public` is on the path, but the `okg` schema the migration creates
    shadows it, and unqualified `_chronos_*` DDL lands in the wrong place.
    `public,okg` is fine and must stay silent.

    A leading `pg_catalog` or `pg_temp` does not count against `public`:
    PostgreSQL searches `pg_catalog` first anyway, and naming it explicitly is
    the normal way to write a hardened path. `pg_catalog,public,okg` is
    therefore silent, and `pg_catalog,okg` is not.

    One line at connection setup, naming the issue, is the difference between
    a five-minute diagnosis and the one that produced #1968. It is a warning,
    not a refusal: the override stays available to whoever needs it.
    """
    for source in sources:
        if not source:
            continue
        for match in _SUPPLIED_SEARCH_PATH.finditer(source):
            value = match.group(1)
            schemas = [
                item.strip().strip('"').lower()
                for item in value.split(",")
                if item.strip()
            ]
            # Skip the leading implicit-anyway entries, then `public` must be
            # what resolves first.
            while schemas and schemas[0] in ("pg_catalog", "pg_temp"):
                schemas.pop(0)
            if schemas[:1] != ["public"]:
                logger.warning(
                    "search_path %r was supplied for this connection and does "
                    "not resolve 'public' first. Chronos's physical tables "
                    "live in public and the substrate names them there, so "
                    "unqualified reads and writes can resolve to the wrong "
                    "schema. Put `public` first in the supplied search_path.",
                    value,
                )


def connection_kwargs(
    *,
    connect_timeout: int | None = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    deployment: str | None = None,
    dsn: str | None = None,
    options: str | None = None,
) -> dict[str, Any]:
    """Return psycopg kwargs shared by direct connections and pools.

    `deployment` names the deployment whose manifest supplies the
    idle-in-transaction bound; omit it to take `OKG_DEPLOYMENT`, the env
    override, or the built-in default.

    `dsn` must be passed whenever one is available. An `options` KEYWORD
    replaces — never merges with — an `options` carried in the connection
    string, so setting one here without reading the DSN first would silently
    drop a `statement_timeout`, `lock_timeout`, or `search_path` a caller
    encoded in its DSN.

    Every session carries the pinned `search_path` from
    `chronos_core.pg_session` (issue #1968), so a login role whose NAME
    matches a schema the migration creates — a role named `okg` — cannot put
    that schema ahead of `public` and send unqualified `_chronos_*` DDL
    somewhere the readers do not look. It is emitted FIRST, so a `search_path`
    the DSN or the caller supplies still wins: libpq applies repeated `-c`
    settings left to right.

    `options` carries extra libpq ``options`` settings a caller needs on the
    session (a `statement_timeout`, say). They are APPENDED to the settings
    above rather than replacing them, so asking for one setting never drops
    the DSN's own options or the idle-in-transaction bound.
    """
    kwargs: dict[str, Any] = dict(_KEEPALIVE_KWARGS)
    if connect_timeout is not None:
        kwargs["connect_timeout"] = connect_timeout
    timeout_ms = idle_in_transaction_timeout_ms(deployment=deployment)
    parts = [
        part for part in (
            SEARCH_PATH_OPTION,
            _dsn_options(dsn),
            (
                f"-c idle_in_transaction_session_timeout={timeout_ms}"
                if timeout_ms > 0 else ""
            ),
            options or "",
        ) if part
    ]
    _warn_if_supplied_search_path_demotes_public(_dsn_options(dsn), options or "")
    if parts:
        kwargs["options"] = " ".join(parts)
    return kwargs


def connect_target(dsn: Any) -> dict[str, str]:
    """Host, port and database a DSN names -- never the password."""
    parsed_dsn = _shared_dsn.try_parse(dsn) if isinstance(dsn, str) else None
    parsed = dict(parsed_dsn.params) if parsed_dsn is not None else {}
    host = str(parsed.get("host") or os.environ.get("PGHOST") or "") or "<default>"
    port = str(parsed.get("port") or os.environ.get("PGPORT") or "") or "5432"
    dbname = str(parsed.get("dbname") or os.environ.get("PGDATABASE") or "") or "<default>"
    if "@" in host or not port.replace(",", "").isdigit():
        # A password with an unescaped "@" or "/" splits the URL in the wrong
        # place, and pieces of it land in these fields: show none of them.
        host, port, dbname = "<unparseable URL>", "<unparseable URL>", "<unparseable URL>"
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": str(parsed.get("user") or "") if "@" not in host else "",
    }


def connect(
    dsn: str, *, autocommit: bool = False,
    application_name: str | None = None,
    connect_timeout: int | None = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    deployment: str | None = None,
    options: str | None = None,
    row_factory: Any = None,
) -> psycopg.Connection:
    """Open a psycopg connection with TCP keepalives + pgvector
    types registered.

    Short-lived connection intended for substrate bootstrap work
    (migrate, doctor, `okg run --once`). Long-lived worker pools
    use psycopg_pool directly (added at M2).

    `application_name` is set on the connection so
    `pg_stat_activity` shows which substrate component owns it
    (e.g. `okg-publisher`, `okg-lease-heartbeat`).

    `connect_timeout` caps how long the underlying TCP/handshake step
    can block. Defaults to 10 seconds — see
    DEFAULT_CONNECT_TIMEOUT_SECONDS for rationale. Pass `None` to
    revert to psycopg's default (no timeout) only when an operator
    explicitly needs unbounded blocking (e.g. waiting on a slow
    remote primary).

    `deployment` names the deployment whose manifest supplies the
    idle-in-transaction bound (issue #1310). Pass it whenever the caller
    knows which deployment the session belongs to; omitting it takes the
    env override or the built-in default rather than no bound at all.

    `options` adds libpq ``options`` settings (for example a
    `statement_timeout`) on top of the ones this factory and the DSN already
    carry. Passing `options=` straight to `psycopg.connect` would REPLACE
    them, silently dropping the idle-in-transaction bound; going through here
    appends instead.

    `row_factory` is forwarded to `psycopg.connect` unchanged so a caller
    that needs `dict_row` (or any other row factory) on the connection can
    still come through this factory instead of calling `psycopg.connect`
    directly and losing the keepalives, connect timeout, and
    idle-in-transaction bound above. Leaving it `None` passes nothing, so
    psycopg's own default row factory stays in force.

    A DSN libpq cannot parse is refused before `psycopg.connect`, as the
    `psycopg.ProgrammingError` psycopg itself raises, but with
    `okg.substrate.db.dsn`'s sanitized message and no chained error:
    libpq's own message quotes the text it choked on, which is often part
    of the password (`missing "=" after "<word>"`, #2285 item 5). A
    non-string is left to `psycopg.connect`'s own type error.
    """
    refusal = _shared_dsn.parse_refusal(dsn) if isinstance(dsn, str) else None
    if refusal is not None:
        # Raised outside any `except`, so no `__context__` carries libpq's text.
        raise psycopg.ProgrammingError(refusal)
    kwargs = connection_kwargs(
        connect_timeout=connect_timeout, deployment=deployment, dsn=dsn,
        options=options,
    )
    profile_ctx = None
    try:
        from okg.substrate.profiling import current_profile
        profile_ctx = current_profile()
    except Exception:  # noqa: BLE001 — profiling is optional instrumentation; absence must not block a connection.
        profile_ctx = None
    if profile_ctx is not None:
        base = application_name or "okg"
        application_name = f"{base};profile={profile_ctx.profile_run_id}"
    if application_name:
        kwargs["application_name"] = application_name
    if row_factory is not None:
        kwargs["row_factory"] = row_factory
    try:
        conn = psycopg.connect(dsn, autocommit=autocommit, **kwargs)
    except psycopg.OperationalError as exc:
        # Issue #3582: a command that does not catch this crashed with a
        # traceback. Record where it tried to connect (never the password)
        # so the CLI can print one line naming the host.
        exc.okg_connect_target = connect_target(dsn)  # type: ignore[attr-defined]
        raise
    if profile_ctx is not None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                row = cur.fetchone()
                if row:
                    profile_ctx.backend_pids.add(int(row[0]))
        except Exception:  # noqa: BLE001 — backend-pid registration is profiling metadata; debug-log and continue with the conn.
            logger.debug("failed to register profiling backend pid", exc_info=True)
    # pgvector type registration lives in okg.substrate.db.vectors
    # to avoid pulling it in when not needed (e.g. `okg doctor`
    # pre-migrate, when the extension may not be installed yet).
    return conn
