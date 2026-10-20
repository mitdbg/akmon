"""A libpq keyword-form DSN must never be read as a SQLite file path.

The defect (issue #1971, seen while proving PR #1957): libpq accepts two
DSN spellings -- the URL form ``postgresql://user@host:5432/db`` and the
keyword form ``user=... host=... dbname=...``. Chronos selects its storage
backend by URL scheme, and the keyword form parses to an EMPTY scheme, so it
was routed to the SQLite adapter. The publish path then created a file in
the working directory named after the whole connection string (a 48 KiB file
called ``user=postgres password=okg dbname=... host=127.0.0.1 port=5671``),
and the visible symptom was a ``sqlite3.IntegrityError`` about a UNIQUE
constraint that never mentioned the DSN.

Two layers close it, and both are tested here because each catches what the
other cannot:

* :func:`chronos_core.branching.sql_adapters.connect_sql_database` refuses a
  scheme-less string that parses as a keyword DSN. This covers EVERY caller,
  production included -- a SQLite path containing ``dbname=`` is never what
  anyone meant.
* :func:`tests.substrate.conftest.require_okg_test_dsn` refuses a
  keyword-form ``OKG_TEST_DSN`` up front, so a contributor who sets the form
  libpq documents is told which form to use instead of meeting the refusal
  several layers down inside a publish.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import psycopg
import pytest

from chronos_core.branching.sql_adapters import connect_sql_database
from tests.substrate.conftest import (
    OKG_TEST_DSN_ENV,
    OKG_TEST_DSN_TEMPLATE_ENV,
    dsn_with_dbname,
    require_okg_test_dsn,
)

#: The form libpq documents and an operator may reasonably set. Test input to
#: a refusal; nothing connects with it.
KEYWORD_DSN = (
    "user=postgres password=okg dbname=okg_test host=127.0.0.1 port=5671"
)


def test_connect_sql_database_refuses_a_keyword_dsn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The refusal names the form, and no file is written."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError) as excinfo:
        connect_sql_database(KEYWORD_DSN)

    message = str(excinfo.value)
    assert "keyword" in message.lower()
    assert "postgresql://" in message
    assert os.listdir(tmp_path) == [], (
        "a refused DSN must not leave a SQLite file behind"
    )


def test_connect_sql_database_still_opens_a_sqlite_path(tmp_path: Path) -> None:
    """The refusal is narrow: an ordinary scheme-less path still works."""
    adapter = connect_sql_database(str(tmp_path / "graph.db"))

    try:
        assert (tmp_path / "graph.db").exists()
    finally:
        adapter.close()


def test_require_okg_test_dsn_refuses_a_keyword_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(OKG_TEST_DSN_ENV, KEYWORD_DSN)
    monkeypatch.delenv(OKG_TEST_DSN_TEMPLATE_ENV, raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        require_okg_test_dsn()

    message = str(excinfo.value)
    assert OKG_TEST_DSN_ENV in message
    # The message names the URL to set instead, rebuilt from the operator's
    # own value rather than invented -- with the password masked. It used to
    # be echoed in cleartext here and in the DSN the message quotes, which
    # reached every surface that does not run the CLI's `--json` redactor:
    # plain `okg db status`, the publisher's exception, and the MCP
    # deployment entry (found reviewing PR #2039).
    assert "postgresql://postgres:***@127.0.0.1:5671/okg_test" in message
    assert "okg_test" in message
    assert "password=okg" not in message
    assert ":okg@" not in message


def test_require_okg_test_dsn_refuses_a_keyword_template(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The template path resolves to a DSN too, so it gets the same check."""
    monkeypatch.delenv(OKG_TEST_DSN_ENV, raising=False)
    monkeypatch.setenv(
        OKG_TEST_DSN_TEMPLATE_ENV,
        "user=postgres dbname=okg_test_{workerid} host=127.0.0.1 port=5671",
    )

    with pytest.raises(RuntimeError) as excinfo:
        require_okg_test_dsn()

    assert OKG_TEST_DSN_TEMPLATE_ENV in str(excinfo.value)


def test_require_okg_test_dsn_returns_a_url_dsn_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Bound inside the test case, not at module scope: an escaping DSN
    # literal is what tests/substrate/test_dsn_env_guard.py refuses.
    url_dsn = "postgresql://postgres:okg@127.0.0.1:5671/okg_test"
    monkeypatch.setenv(OKG_TEST_DSN_ENV, url_dsn)

    assert require_okg_test_dsn() == url_dsn


# --------------------------------------------------------------------------
# The refusal message itself. Both of these were untested paths: the refusal
# worked for a well-formed keyword DSN naming a host, and fell apart on the
# two inputs an operator is most likely to arrive with -- a typo, and a
# hostless DSN.
# --------------------------------------------------------------------------

#: The suggestion, as the message spells it.
_SUGGESTION = re.compile(r"Set it to (\S+) instead\.")


def test_require_okg_test_dsn_names_the_variable_for_a_malformed_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unquoted space is a typo, not a psycopg traceback.

    ``password=my secret`` -- a password pasted without quoting -- made
    libpq's own parser raise ``ProgrammingError: missing "=" after
    "secret"``, from inside the code whose whole job is to explain what is
    wrong with this variable.
    """
    monkeypatch.setenv(
        OKG_TEST_DSN_ENV, "host=127.0.0.1 password=my secret dbname=okg_test"
    )
    monkeypatch.delenv(OKG_TEST_DSN_TEMPLATE_ENV, raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        require_okg_test_dsn()

    assert OKG_TEST_DSN_ENV in str(excinfo.value)


@pytest.mark.parametrize(
    "keyword_dsn",
    [
        KEYWORD_DSN,
        "service=okg dbname=okg_test",
        "dbname=okg_test",
        "host=127.0.0.1 password=my secret dbname=okg_test",
    ],
)
def test_the_refusal_only_suggests_a_url_psycopg_accepts(
    keyword_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A suggestion psycopg rejects is worse than no suggestion.

    A DSN with no host and no user rebuilt to ``postgresql:/okg_test`` --
    urlunsplit omits an empty authority, and libpq's hostless URL needs
    three slashes. The operator would have pasted it and been refused by
    psycopg instead.
    """
    monkeypatch.setenv(OKG_TEST_DSN_ENV, keyword_dsn)
    monkeypatch.delenv(OKG_TEST_DSN_TEMPLATE_ENV, raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        require_okg_test_dsn()

    match = _SUGGESTION.search(str(excinfo.value))
    if match is None:
        # No specific suggestion: the generic hint must still name the form.
        assert "postgresql://user:password@host:port/dbname" in str(
            excinfo.value
        )
        return

    suggested = match.group(1)
    parsed = psycopg.conninfo.conninfo_to_dict(suggested)
    assert parsed.get("dbname") == "okg_test"


@pytest.mark.parametrize(
    "hostless_dsn", ["service=okg dbname=okg_test", "dbname=okg_test"]
)
def test_dsn_with_dbname_builds_a_parsable_hostless_url(
    hostless_dsn: str,
) -> None:
    """The rebuild itself, without the refusal wrapped around it."""
    url = dsn_with_dbname(hostless_dsn, "okg_test")

    assert psycopg.conninfo.conninfo_to_dict(url).get("dbname") == "okg_test"


@pytest.mark.parametrize(
    ("dsn", "secret"),
    [
        (KEYWORD_DSN, "okg"),
        # libpq cannot parse an unquoted password with a space in it.
        ("host=127.0.0.1 password=my secret dbname=okg_test", "secret"),
        ("mysql://reader:hunter2@db.example:3306/graph", "hunter2"),
        ("mysql://reader@db.example/graph?password=hunter2", "hunter2"),
    ],
)
def test_connect_sql_database_refusals_never_quote_a_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dsn: str, secret: str,
) -> None:
    """Issue #2285 item 5: a refusal names host, port, database and user only.

    The keyword-DSN refusal used to quote the whole connection string, and the
    unsupported-scheme refusal the whole URL, password included.
    """
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError) as excinfo:
        connect_sql_database(dsn)

    message = str(excinfo.value)
    assert f"password={secret}" not in message, message
    assert f":{secret}@" not in message, message
    assert secret not in message.replace("okg_test", ""), message


def test_connect_sql_database_refusal_still_names_the_target() -> None:
    """Redaction keeps what an operator needs to find the setting."""
    with pytest.raises(ValueError) as excinfo:
        connect_sql_database(KEYWORD_DSN)

    message = str(excinfo.value)
    for part in (
        "host=127.0.0.1", "port=5671", "dbname=okg_test", "user=postgres",
    ):
        assert part in message, message


_PARSE_SENTINEL = "S3CRET2311"


def _error_text_everywhere(error: BaseException) -> str:
    """Every place an error's text can surface: str, repr and the chain."""
    parts = [str(error), repr(error)]
    for linked in (error.__cause__, error.__context__):
        if linked is not None:
            parts.extend([str(linked), repr(linked)])
    return "\n".join(parts)


@pytest.mark.parametrize(
    "dsn",
    [
        # libpq cannot parse an unquoted password with a space in it, and its
        # error quotes the word after the space: missing "=" after "...".
        f"host=127.0.0.1 port=5671 password=my {_PARSE_SENTINEL} dbname=okg_test",
        # An invalid percent escape: libpq quotes the whole token.
        f"postgresql://reader:%zz{_PARSE_SENTINEL}@127.0.0.1:5671/okg_test",
    ],
)
def test_postgres_adapter_parse_errors_never_quote_the_password(
    monkeypatch: pytest.MonkeyPatch, dsn: str,
) -> None:
    """Issue #2285 item 5: libpq's parse error echoed password fragments.

    ``psycopg.connect`` re-raised libpq's own message, which quotes the text
    around the fault. The adapter now parses first and refuses with a
    message naming only host, port, database and user, and with no chained
    error behind it.
    """
    from chronos_core.branching import sql_adapters

    def refuse_to_connect(*args: object, **kwargs: object) -> object:
        raise AssertionError("an unparseable DSN must be refused before connect")

    monkeypatch.setattr(sql_adapters.psycopg, "connect", refuse_to_connect)

    with pytest.raises(psycopg.ProgrammingError) as excinfo:
        sql_adapters.PostgresDatabaseAdapter.connect(dsn)

    text = _error_text_everywhere(excinfo.value)
    assert _PARSE_SENTINEL not in text, text
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
    assert "could not parse" in str(excinfo.value)


def test_connect_sql_database_url_parse_error_names_the_target_only() -> None:
    """The okg route (``connect_sql_database``) gets the same redaction."""
    dsn = f"postgresql://reader:%zz{_PARSE_SENTINEL}@127.0.0.1:5671/okg_test"

    with pytest.raises(psycopg.ProgrammingError) as excinfo:
        connect_sql_database(dsn)

    text = _error_text_everywhere(excinfo.value)
    assert _PARSE_SENTINEL not in text, text
    for part in ("host=127.0.0.1", "port=5671", "dbname=okg_test", "user=reader"):
        assert part in str(excinfo.value), str(excinfo.value)


@pytest.mark.parametrize(
    ("dsn", "options"),
    [
        (f"sqlite:///tmp/graph.db?password={_PARSE_SENTINEL}", "-c x=1"),
        (f"sqlite://reader:{_PARSE_SENTINEL}@remote.example/graph.db", None),
    ],
)
def test_sqlite_refusals_never_quote_the_raw_url(
    dsn: str, options: str | None,
) -> None:
    """The SQLite-side refusals quoted the raw URL; they now describe it."""
    with pytest.raises(ValueError) as excinfo:
        connect_sql_database(dsn, options=options)

    assert _PARSE_SENTINEL not in _error_text_everywhere(excinfo.value)
