"""Which spelling a Postgres DSN is written in, and how to refuse the wrong one.

libpq accepts two spellings of the same connection string: the URL
(``postgresql://user:password@host:port/dbname``) and the keyword/value form
(``host=... dbname=...``).  They are interchangeable to libpq.  They are NOT
interchangeable here: Chronos selects its storage backend by URL SCHEME, and
the keyword form has none, so it used to be routed to the SQLite adapter and
the publish path wrote a FILE named after the whole connection string.  The
operator then met a ``sqlite3`` UNIQUE-constraint error that never mentioned
the DSN (issue #1971).

:func:`chronos_core.branching.sql_adapters.connect_sql_database` refuses that
string for every caller, but it refuses in Chronos' terms and only once a
write is attempted.  This module lets each place an operator actually SETS a
DSN refuse it first, naming the setting they edited and the URL to write
instead (issue #2001).

Nothing here rewrites a DSN behind the operator's back.  The URL is offered in
the message and never substituted: the two spellings are not interchangeable
in this system, and silently normalising one into the other hides that.

Every name here is now a thin re-export of :mod:`okg.substrate.db.dsn`, the
one DSN implementation (Sprint 14 DSN migration). The names stay until the
last migration pull request moves their importers onto that module.
"""

from __future__ import annotations

from okg.substrate.db import dsn as _dsn
from okg.substrate.db.dsn import LIBPQ_KEYWORD_PARAM


def is_keyword_form_dsn(dsn: str) -> bool:
    """True when ``dsn`` is a scheme-less libpq keyword/value string.

    A value carrying ``://`` anywhere is left to the layer that understands
    its scheme, as before. ``okg.substrate.db.dsn.form_of`` is libpq's own
    rule and also calls ``host=h application_name=http://x`` keyword form;
    callers switch to it when they move onto that module.
    """
    return "://" not in dsn and _dsn.form_of(dsn) == "keyword"


def postgresql_url(netloc: str, dbname: str, query: str) -> str:
    """Assemble a ``postgresql://`` URL from already-encoded parts.

    ``urlunsplit`` omits an empty authority, so a DSN carrying no host and no
    user came back as ``postgresql:/okg_test``, which libpq rejects. libpq
    spells a hostless URL with three slashes, so the empty authority is
    written out. Code that starts from a DSN uses
    ``dsn.parse(value).to_url()`` instead.
    """
    from urllib.parse import urlunsplit

    url = urlunsplit(("postgresql", netloc, f"/{dbname}", query, ""))
    if not netloc:
        url = url.replace("postgresql:/", "postgresql:///", 1)
    return url


def dsn_with_dbname(base_dsn: str, dbname: str) -> str:
    """``base_dsn``'s server, a different database, as a URL.

    ``dsn.parse(base_dsn).with_dbname(dbname).to_url()``: a URL, never a
    ``make_conninfo`` keyword string, checked by parsing it back. Raises
    ``okg.substrate.db.dsn.DsnError`` when libpq cannot parse ``base_dsn`` or
    the URL would not mean the same connection.
    """
    return _dsn.parse(base_dsn).with_dbname(dbname).to_url()


def url_form_suggestion(dsn: str) -> str | None:
    """``dsn`` as a URL with its password masked, for a message; or None.

    For READING, never for connecting: the password is ``***``. The URL is
    validated in its real form (``to_url`` parses it back) before it is
    masked, so the operator is only shown a shape libpq accepts. A DSN libpq
    cannot parse, or one with no database name, gets no suggestion.
    """
    parsed = _dsn.try_parse(dsn)
    if parsed is None or not parsed.dbname:
        return None
    try:
        return _dsn.redact(parsed.to_url())
    except _dsn.DsnError:
        return None


def keyword_form_refusal(source: str, dsn: str) -> str:
    """The refusal text for a keyword-form ``dsn`` set through ``source``.

    The same text ``okg.substrate.db.dsn.require_url_form`` raises. Secrets
    are masked inside the builder, because not every surface that prints it
    redacts. A DSN libpq cannot parse is not echoed at all: in such a string
    the masker cannot tell a password's second word from the next key
    (``password=my secret=x`` used to show ``secret=x``).
    """
    return _dsn._keyword_form_refusal(source, dsn)


__all__ = [
    "LIBPQ_KEYWORD_PARAM",
    "dsn_with_dbname",
    "is_keyword_form_dsn",
    "keyword_form_refusal",
    "postgresql_url",
    "url_form_suggestion",
]
