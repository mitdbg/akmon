"""The session ``search_path`` every okg/Chronos PostgreSQL session pins.

Chronos's physical relations (``_chronos_*``) live in the ``public`` schema,
and the substrate names ``public._chronos*`` outright in 51 places across 17
files -- including three released migrations, ``schema.sql``, ``views.sql``
and the ``public._chronos%`` grant pattern in ``runtime/sql/role_split.sql``.
That contract is deliberate; what breaks it is the *session*, not the readers.

PostgreSQL's factory default ``search_path`` is ``"$user", public``. When the
login role's own name matches a schema the migration creates -- a role named
``okg`` -- ``"$user"`` expands to the ``okg`` schema and puts it ahead of
``public``. Unqualified DDL then lands in ``okg`` while every reader still
looks in ``public``, and a fresh ``okg migrate --apply`` dies partway through
with ``DuplicateColumn`` or ``UndefinedTable`` (issue #1968, defect 2 of
#1922).

Pinning the session path removes ``"$user"`` from the resolution order, so a
role's *name* can no longer decide where an unqualified relation is created.

Why exactly ``public`` and nothing else:

* It reproduces the effective path a role NOT named after a schema already
  has today. ``"$user"`` expands to nothing for such a role, leaving
  ``public``. Every existing deployment therefore resolves names exactly as
  before -- the pin is a no-op for them and a fix only for the colliding case.
* ``pg_catalog`` is searched first implicitly whenever it is not named, so it
  does not need to appear here.
* The ``okg`` schema is deliberately NOT on the path. Objects in it are
  schema-qualified everywhere in the codebase, and
  ``okg/substrate/mcp/sql_safety.py`` requires that a bare ``v_nodes`` NOT
  resolve to ``okg.v_nodes``. Adding ``okg`` here would create resolutions no
  deployment has today, which is the opposite of leaving them unchanged.
* The ten functions pinned to ``SET search_path = pg_catalog, okg`` in
  ``okg/substrate/attribute_integrity.py``, and the audit in
  ``external_actions/store.py`` that checks for exactly that string, are
  unaffected: a function-level ``SET search_path`` replaces the session's for
  the duration of the call, and the audit reads ``pg_proc.proconfig`` rather
  than the session.

Callers must keep this ahead of any ``options`` a DSN carries, so an operator
who encodes an explicit ``search_path`` in a connection string still wins:
libpq applies repeated ``-c`` settings left to right, and the last one stands.
"""

from __future__ import annotations

import logging
from typing import Callable


logger = logging.getLogger(__name__)

#: The session ``search_path`` okg pins on every connection it opens.
SESSION_SEARCH_PATH = "public"

#: The same value as a libpq ``options`` fragment.
SEARCH_PATH_OPTION = f"-c search_path={SESSION_SEARCH_PATH}"


#: Bound, in milliseconds, on how long one of okg's or Chronos's own sessions
#: may sit ``idle in transaction`` (issues #1310 and #1997).
#:
#: A psycopg connection opened with ``autocommit=False`` starts a transaction
#: on its first statement. A session that then waits on the client keeps that
#: transaction open, and with it every advisory or row lock the transaction
#: took. Worse, once the transaction has written anything it holds an
#: in-progress transaction id, and that holds back the CLUSTER-wide vacuum
#: horizon: ``VACUUM`` in every database on the instance stops marking pages
#: all-visible and stops removing newer dead rows for as long as the session
#: sits there.
#:
#: ``idle_in_transaction_session_timeout`` makes that self-healing. PostgreSQL
#: terminates the offending backend, releases its locks, and the caller gets a
#: loud SQLSTATE 25P03 instead of a silently jammed pool and a frozen vacuum
#: horizon. It fires ONLY while the session is waiting on the client, never
#: while a statement is running, so it cannot interrupt legitimate work -- a
#: long fork or checkpoint of a large branch is a running statement and is not
#: at risk.
#:
#: The value lives HERE, in the package both sides can import, because okg's
#: connection factory and Chronos's own adapter factory must not disagree
#: about it. ``okg.substrate.db.connection`` imports it and layers the process
#: env override (``OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS``) and the per-deployment
#: manifest key on top. Chronos cannot read either itself -- this package must
#: not import okg -- so okg registers its resolver here at import
#: (``set_idle_in_transaction_timeout_resolver``) and Chronos's factory asks
#: it for the value. Before issue #2920 the factory used this default and
#: ignored both, and a cms-kb re-ingest whose Chronos session waited longer
#: than five minutes between statements was killed with SQLSTATE 25P03. A
#: caller that needs a different bound still passes its own ``options``
#: (libpq takes the last ``-c`` for a setting).
DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS = 300_000

#: The embedding application's resolver for the bound, or None. okg installs
#: ``okg.substrate.db.connection.idle_in_transaction_timeout_ms``.
_IDLE_IN_TRANSACTION_TIMEOUT_RESOLVER: Callable[[], int] | None = None


def set_idle_in_transaction_timeout_resolver(
    resolver: Callable[[], int] | None,
) -> None:
    """Install the function that resolves the idle-in-transaction bound.

    Chronos-owned sessions then carry the value it returns, so the embedding
    application's own configuration (for okg: the env override, then the
    deployment manifest, then the default) reaches them too. ``None`` restores
    the built-in default.
    """
    global _IDLE_IN_TRANSACTION_TIMEOUT_RESOLVER
    _IDLE_IN_TRANSACTION_TIMEOUT_RESOLVER = resolver


def resolved_idle_in_transaction_timeout_ms() -> int:
    """The bound a Chronos-owned session gets: the resolver's, else the default.

    A resolver that raises (a set-but-malformed override, say) propagates: a
    masked default would leave the operator believing their bound is in force.
    """
    resolver = _IDLE_IN_TRANSACTION_TIMEOUT_RESOLVER
    if resolver is None:
        return DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS
    return resolver()


def idle_in_transaction_option(timeout_ms: int | None = None) -> str:
    """The idle-in-transaction bound as a libpq ``options`` fragment.

    ``None`` (the default) uses ``resolved_idle_in_transaction_timeout_ms()``.
    ``0`` returns ``""``: no fragment is emitted, which leaves the server's own
    setting in force. That is the only way to ask for the unbounded behavior.
    """
    if timeout_ms is None:
        timeout_ms = resolved_idle_in_transaction_timeout_ms()
    if timeout_ms < 0:
        raise ValueError(
            "idle_in_transaction_session_timeout must not be negative, "
            f"got {timeout_ms}"
        )
    return (
        f"-c idle_in_transaction_session_timeout={timeout_ms}"
        if timeout_ms > 0
        else ""
    )


def dsn_options(dsn: str | None) -> str:
    """Return the libpq ``options`` already carried by a DSN, or ``""``."""
    if not dsn:
        return ""
    try:
        from psycopg.conninfo import conninfo_to_dict

        return str(conninfo_to_dict(dsn).get("options") or "")
    except Exception:  # noqa: BLE001 -- an unparseable DSN is psycopg.connect's
        # error to raise, with its own message. Returning "" here only means
        # "no options to preserve"; it never suppresses a connection failure.
        logger.debug("could not read options from the DSN", exc_info=True)
        return ""


def session_options(dsn: str | None = None, extra: str | None = None) -> str:
    """Build the libpq ``options`` string for a Chronos PostgreSQL session.

    An ``options`` KEYWORD replaces -- never merges with -- an ``options``
    carried in the connection string, so the DSN's own settings are read back
    and re-appended here. The pin goes FIRST so a ``search_path`` an operator
    encoded in the DSN, or one a caller passes in ``extra``, overrides it.
    """
    parts = [
        part for part in (SEARCH_PATH_OPTION, dsn_options(dsn), extra or "")
        if part
    ]
    return " ".join(parts)
