"""Table tests for `okg.substrate.db.dsn`: parse, rewrite, expand and redact.

Every edge case in the DSN-module design (Sprint 14) and in issues #1980,
#2230, #2040 and #2285 has a row here. The #2285 defects are pinned against
the NEW functions: each test names the defect it reproduces. No database is
needed; `parse` uses libpq's own parser through `psycopg.conninfo`.
"""

from __future__ import annotations

import gc
import math
import shlex
import signal
import subprocess
import sys
import time

import pytest

from okg.substrate.db import dsn
from okg.substrate.db.dsn import (
    MASK,
    REDACTED_DSN,
    Dsn,
    DsnFormError,
    DsnParseError,
    DsnPlaceholderEmpty,
    DsnPlaceholderUnset,
    DsnPlaceholderUnsupported,
    DsnRenderError,
)

SECRET = "s3cr3tPW"


# ---------------------------------------------------------------------------
# Import cost
# ---------------------------------------------------------------------------


def test_importing_the_module_does_not_import_psycopg() -> None:
    """The module and the lazy package init load no psycopg (installer --help path)."""
    probe = (
        "import sys\n"
        "import okg.substrate.db.dsn as d\n"
        "d.redact_text('postgresql://u:p@h/db')\n"
        "d.expand('${X}', {'X': 'y'})\n"
        "d.form_of('host=h')\n"
        "loaded = sorted(m for m in sys.modules if m == 'psycopg' or m.startswith('psycopg.'))\n"
        "assert not loaded, loaded\n"
        "assert 'okg.substrate.db.connection' not in sys.modules\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_parse_imports_psycopg_lazily_and_package_exports_still_resolve() -> None:
    probe = (
        "import sys\n"
        "import okg.substrate.db.dsn as d\n"
        "d.parse('postgresql://u:p@h/db')\n"
        "assert 'psycopg.conninfo' in sys.modules\n"
        "import okg.substrate.db as db\n"
        "from okg.substrate.db import connect, dsn_from_env\n"
        "assert connect.__module__ == 'okg.substrate.db.connection'\n"
        "assert db.connection.redact is not None\n"
        "try:\n"
        "    db.no_such_name\n"
        "except AttributeError:\n"
        "    print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


# ---------------------------------------------------------------------------
# Form detection and the URL-form requirement (#2040)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "form"),
    [
        ("postgresql://u:p@h/db", "url"),
        ("postgres://u:p@h/db", "url"),
        ("postgresql://h/db  ", "url"),
        # libpq takes the URL form only with this exact lowercase prefix.
        ("POSTGRESQL://h/db", None),
        ("  postgresql://h/db", None),
        ("postgresql:///db", "url"),
        ("host=h dbname=db", "keyword"),
        ("dbname=okg_test", "keyword"),
        ("  password = x", "keyword"),
        ("mysql://u:p@h/db", None),
        ("sqlite:///tmp/x.db", None),
        ("/tmp/okg.sqlite", None),
        ("${OKG_DSN}", None),
        ("", None),
        # libpq decides by the URL prefix alone; `://` inside a keyword value
        # does not make the string a URL (review of PR 2309, B1).
        ("host=h password='a://b' dbname=d", "keyword"),
        ("host=h dbname=d application_name=http://x", "keyword"),
    ],
)
def test_form_of(value: str, form: str | None) -> None:
    assert dsn.form_of(value) == form


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # The case Hypothesis shrank to under --hypothesis-seed=20260922.
        (
            "user=0 password=*** host=db.example dbname=:// sslmode=prefer",
            {
                "user": "0",
                "password": "***",
                "host": "db.example",
                "dbname": "://",
                "sslmode": "prefer",
            },
        ),
        (
            "host=h password='a://b' dbname=d",
            {"host": "h", "password": "a://b", "dbname": "d"},
        ),
        (
            "host=h dbname=d application_name=http://x",
            {"host": "h", "dbname": "d", "application_name": "http://x"},
        ),
    ],
)
def test_keyword_dsn_with_scheme_text_in_a_value_parses(
    value: str, expected: dict[str, str]
) -> None:
    """Review of PR 2309, B1: `form_of` refused any keyword DSN holding `://`."""
    parsed = dsn.parse(value)
    assert parsed.form == "keyword"
    assert dict(parsed.params) == expected
    assert dsn.parse(parsed.render()) == parsed
    with pytest.raises(DsnFormError):
        dsn.require_url_form(value, source="OKG_DSN")


def test_require_url_form_returns_a_url_unchanged() -> None:
    value = "postgresql://u:p@h:5432/db?sslmode=require"
    assert dsn.require_url_form(value, source="OKG_DSN") is value


@pytest.mark.parametrize(
    "value",
    [
        f"host=h password={SECRET} dbname=okg",
        f"password='{SECRET} x' host=h dbname=okg",
    ],
)
def test_require_url_form_refuses_keyword_form_by_name_without_the_password(
    value: str,
) -> None:
    with pytest.raises(DsnFormError) as raised:
        dsn.require_url_form(value, source="manifest postgres.dsn")
    error = raised.value
    assert error.code == "dsn_keyword_form"
    assert "manifest postgres.dsn" in error.message
    assert "keyword/value" in error.message
    assert "postgresql://" in error.message
    assert "The *** stands for your real password" in error.message
    assert SECRET not in error.message
    assert SECRET not in repr(error.data)
    assert error.data == {"source": "manifest postgres.dsn"}


def test_require_url_form_refuses_a_keyword_dsn_libpq_cannot_parse() -> None:
    value = f"host=h password=my {SECRET} dbname=x"
    with pytest.raises(DsnFormError) as raised:
        dsn.require_url_form(value, source="OKG_DSN")
    assert SECRET not in raised.value.message
    assert (
        "URL form: postgresql://user:password@host:port/dbname" in raised.value.message
    )


@pytest.mark.parametrize(
    "value", ["sqlite:///tmp/x.db", "/tmp/okg.sqlite", "${OKG_DSN}", ""]
)
def test_require_url_form_refuses_non_postgres_values(value: str) -> None:
    with pytest.raises(DsnParseError) as raised:
        dsn.require_url_form(value, source="OKG_DSN")
    assert raised.value.code == "dsn_parse_error"
    assert "OKG_DSN" in raised.value.message


@pytest.mark.parametrize(
    ("value", "secret", "shown"),
    [
        # Review of PR 2425, should-fix 1: a missing colon after the scheme,
        # and a URL with no scheme at all. No parser tokenizes either, so the
        # maskers cannot find the password, and the refusal used to repeat it.
        ("postgresql//u:s3cr3tA@h/db", "s3cr3tA", "the value is not shown"),
        ("u:s3cr3tI@h/db", "s3cr3tI", "the value is not shown"),
        # Review of PR 2424: the same path through require_url_form.
        ("u:PWSECRET@h/d", "PWSECRET", "the value is not shown"),
        (" postgresql://u:s3cr3tB@h/db", "s3cr3tB", "the value is not shown"),
        ("mysql://u:s3cr3tM@h/db", "s3cr3tM", "the scheme mysql://"),
        ("POSTGRESQL://u:s3cr3tP@h/db", "s3cr3tP", "the scheme POSTGRESQL://"),
        ("   ", None, "the value is empty"),
    ],
)
def test_unrecognized_form_errors_never_repeat_the_value(
    value: str, secret: str | None, shown: str
) -> None:
    for call in (dsn.parse, lambda v: dsn.require_url_form(v, source="OKG_DSN")):
        with pytest.raises(DsnParseError) as raised:
            call(value)
        error = raised.value
        text = " ".join(
            [str(error), repr(error), repr(error.data), str(error.fix_hint)]
        )
        assert shown in error.message
        if secret is not None:
            assert secret not in text
            assert "u:" not in text
        assert error.data.get("form") is None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # Percent-encoding is decoded (the password is the real one).
        (
            "postgresql://u:p%40q%2Fr%3As@h/d%20b",
            {"user": "u", "password": "p@q/r:s", "host": "h", "dbname": "d b"},
        ),
        # ?password= overrides the userinfo password, as in libpq.
        (
            "postgresql://u:first@h/db?password=second",
            {"user": "u", "password": "second", "host": "h", "dbname": "db"},
        ),
        # A query password with no userinfo password.
        (
            "postgresql://u@h/db?password=zz",
            {"user": "u", "password": "zz", "host": "h", "dbname": "db"},
        ),
        # An empty user is valid libpq.
        ("postgresql://:pw@h/db", {"password": "pw", "host": "h", "dbname": "db"}),
        # Raw `@` in a URL password: libpq reads password `a`, host `b@h`.
        (
            "postgresql://u:a@b@h/db",
            {"user": "u", "password": "a", "host": "b@h", "dbname": "db"},
        ),
        # Raw `/` in a URL password: libpq reads host `u`, port `a`.
        ("postgresql://u:a/b@h/db", {"host": "u", "port": "a", "dbname": "b@h/db"}),
        # Repeated keyword: the last one wins.
        ("host=a host=b dbname=x", {"host": "b", "dbname": "x"}),
        # Double quotes are not libpq quoting.
        ('password="x" host=h', {"password": '"x"', "host": "h"}),
        # Single quotes quote, with \' and \\ escapes.
        ("password='a \\' b\\\\' host=h", {"password": "a ' b\\", "host": "h"}),
        # Multiple hosts and ports.
        ("postgresql://h1:1,h2:2/db", {"host": "h1,h2", "port": "1,2", "dbname": "db"}),
        # IPv6 and unix-socket hosts.
        ("postgresql://[::1]:5433/db", {"host": "::1", "port": "5433", "dbname": "db"}),
        ("postgresql:///db?host=%2Ftmp", {"host": "/tmp", "dbname": "db"}),
        # `+` is not a space to libpq.
        (
            "postgresql://h/db?options=-c+x",
            {"host": "h", "dbname": "db", "options": "-c+x"},
        ),
    ],
)
def test_parse_follows_libpq(value: str, expected: dict[str, str]) -> None:
    assert dict(dsn.parse(value).params) == expected


def test_parse_records_form_and_scheme() -> None:
    url = dsn.parse("postgres://h/db")
    assert (url.form, url.scheme) == ("url", "postgres")
    keyword = dsn.parse("host=h dbname=db")
    assert (keyword.form, keyword.scheme) == ("keyword", None)


@pytest.mark.parametrize(
    ("value", "must_not_contain", "kept"),
    [
        # #2285 item 5: libpq quotes the password fragment it choked on.
        (
            f"host=h password=my {SECRET} dbname=x",
            SECRET,
            'missing "=" after "<fragment>"',
        ),
        (
            f"host=h password=my {SECRET}=x",
            SECRET,
            "invalid connection option (name not shown)",
        ),
        (f"host=h password='{SECRET}", SECRET, "unterminated quoted string"),
        (
            f"postgresql://u:{SECRET}@h/db?bogus=1",
            SECRET,
            "invalid URI query parameter: (name not shown)",
        ),
    ],
)
def test_parse_error_never_repeats_a_secret(
    value: str, must_not_contain: str, kept: str
) -> None:
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(value)
    error = raised.value
    text = " ".join([str(error), error.message, repr(error.data), str(error.fix_hint)])
    assert must_not_contain not in text
    assert error.__cause__ is None
    assert error.__suppress_context__ is True
    assert kept in error.message


@pytest.mark.parametrize(
    "value",
    [
        f"host=h password=my {SECRET} dbname=x",
        f"postgresql://u:a {SECRET}@h/db",
        f"host=h password='{SECRET}",
    ],
)
def test_parse_error_chain_holds_no_libpq_text(value: str) -> None:
    """Review of PR 2309, S1: `__context__` kept libpq's error, which quotes the password."""
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(value)
    seen: list[BaseException] = []
    pending: list[BaseException | None] = [raised.value]
    while pending:
        current = pending.pop()
        if current is None or any(current is s for s in seen):
            continue
        seen.append(current)
        pending.extend([current.__cause__, current.__context__])
    assert seen == [raised.value]
    assert raised.value.__context__ is None
    assert all(SECRET not in str(error) for error in seen)


@pytest.mark.parametrize(
    "value",
    [
        # A raw `&` inside a query password: libpq reads the tail as a key.
        f"postgresql://u@h/db?password=ab&{SECRET}word=1",
        f"postgresql://u@h/db?sslpassword=ab&{SECRET}word=1",
    ],
)
def test_url_parse_error_withholds_a_key_that_is_really_a_password_tail(
    value: str,
) -> None:
    """Review of PR 2309, S2: the URL branch echoed `SECRETword` as the bad key."""
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(value)
    error = raised.value
    assert SECRET not in error.message
    assert SECRET not in repr(error.data)
    assert "key" not in error.data


def test_keyword_form_refusal_withholds_a_string_libpq_cannot_parse() -> None:
    """Review of PR 2309, S2: the refusal showed `password=*** SECRET=1`."""
    with pytest.raises(DsnFormError) as raised:
        dsn.require_url_form(f"host=h password=a {SECRET}=1", source="OKG_DSN")
    assert SECRET not in raised.value.message
    assert "not shown" in raised.value.message


def test_parse_does_not_name_an_unknown_key() -> None:
    """Review of PR 2425 (final), S3: an unknown option can be a password
    word, so it is never named."""
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(f"host=h nosuchkey=1 password={SECRET}")
    assert raised.value.data == {"form": "keyword"}
    assert "invalid connection option (name not shown)" in raised.value.message
    assert "nosuchkey" not in raised.value.message


@pytest.mark.parametrize(
    "value",
    [
        'host=h password="hunter two=2" dbname=x',
        'sslpassword="zq two=:',
        "postgresql://u:zq?two=",
        "postgresql://u:zq@h/db?two=1",
    ],
)
def test_parse_error_never_names_a_password_word_as_a_key(value: str) -> None:
    """Review of PR 2425 (final), S3: libpq reads a double quote literally, so
    `two` is the second word of the password, not a key."""
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(value)
    error = raised.value
    for surface in (error.message, str(error), repr(error), repr(error.data)):
        assert "two" not in surface
    assert "key" not in error.data


def test_parse_error_names_a_libpq_keyword_an_older_client_lacks() -> None:
    """A keyword from a newer libpq (`require_auth` before 16) is not a
    password word, so it is still named."""
    message, key = dsn._sanitized_libpq_message(
        'invalid connection option "require_auth"',
        "host=h require_auth=scram-sha-256 dbname=x",
        "keyword",
    )
    assert message == 'invalid connection option "require_auth"'
    assert key == "require_auth"


@pytest.mark.parametrize(
    ("value", "password"),
    [
        ("password=\xa0", "\xa0"),
        ("password=zq\xa0", "zq\xa0"),
        ("host=h password=000000:\xa0 dbname=x", "000000:\xa0"),
    ],
)
def test_parse_of_a_split_non_ascii_blank_is_a_clean_parse_error(
    value: str, password: str
) -> None:
    """Review of PR 2425 (final), S2: libpq splits U+00A0 with C isspace and
    psycopg then raised UnicodeDecodeError; the parse error hides the value.
    The macOS libpq splits it; the Linux one keeps it in the value, and then
    the whole password must survive and stay hidden -- no skip either way."""
    try:
        parsed = dsn.parse(value)
    except DsnParseError as error:
        assert error.data == {"form": "keyword"}
        assert "non-ASCII" in error.message
        assert error.__cause__ is None and error.__context__ is None
    except UnicodeError as error:  # pragma: no cover - the defect this pins
        pytest.fail(f"dsn.parse let {type(error).__name__} escape")
    else:
        assert parsed.params["password"] == password
        # repr() writes U+00A0 as the four characters \xa0; check both spellings.
        escaped = password.encode("unicode_escape").decode("ascii")
        for surface in (repr(parsed), str(parsed)):
            assert password not in surface
            assert escaped not in surface


def test_parse_hides_a_key_that_is_really_a_password_word() -> None:
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(f"host=h password=my {SECRET}=x")
    assert "key" not in raised.value.data


@pytest.mark.parametrize(
    "value", ["", "not a dsn", "sqlite:///x.db", "mysql://u:secretpw@h/db"]
)
def test_parse_refuses_non_dsn_text_without_echoing_secrets(value: str) -> None:
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(value)
    assert "secretpw" not in raised.value.message


def test_try_parse_returns_none_instead_of_raising() -> None:
    assert dsn.try_parse(f"host=h password=my {SECRET} dbname=x") is None
    assert dsn.try_parse("postgresql://h/db") == dsn.parse("postgresql://h/db")


# ---------------------------------------------------------------------------
# The Dsn object
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        f"postgresql://u:{SECRET}@h:5432/db?sslmode=require",
        f"host=h password={SECRET} dbname=db",
        f"host=h password='{SECRET} x' dbname=db",
        f"postgresql://u@h/db?password={SECRET}",
    ],
)
def test_repr_and_str_never_show_the_password(value: str) -> None:
    parsed = dsn.parse(value)
    assert SECRET not in repr(parsed)
    assert SECRET not in str(parsed)
    assert SECRET not in repr(parsed.target())
    assert MASK in str(parsed)


def test_target_names_host_port_user_dbname_only() -> None:
    parsed = dsn.parse(f"postgresql://u:{SECRET}@h:5433/db?sslmode=require")
    assert parsed.target() == {"host": "h", "port": "5433", "user": "u", "dbname": "db"}


def test_properties() -> None:
    parsed = dsn.parse(
        "postgresql://u:p@h:5/db?options=-c%20a%3Db&application_name=app"
    )
    assert (parsed.host, parsed.port, parsed.user, parsed.dbname, parsed.password) == (
        "h",
        "5",
        "u",
        "db",
        "p",
    )
    assert (parsed.options, parsed.application_name) == ("-c a=b", "app")


def test_equality_and_hash_ignore_param_order() -> None:
    a = Dsn(form="url", scheme="postgresql", params={"host": "h", "dbname": "d"})
    b = Dsn(form="url", scheme="postgresql", params={"dbname": "d", "host": "h"})
    assert a == b
    assert hash(a) == hash(b)
    assert a != Dsn(form="keyword", scheme=None, params={"host": "h", "dbname": "d"})


def test_params_are_read_only() -> None:
    parsed = dsn.parse("postgresql://h/db")
    with pytest.raises(TypeError):
        parsed.params["host"] = "x"  # type: ignore[index]


@pytest.mark.parametrize(
    "value",
    [
        "postgresql://u:p%40q@h:5432/db?sslmode=require&options=-c%20search_path%3Dokg",
        "postgresql://:p@h/db",
        "postgresql:///db",
        "postgresql://",
        "postgresql://h1:1,h2:2/db",
        "postgresql://h1,h2/db",
        "postgresql://[::1]:5433/db",
        "postgresql:///db?host=%2Ftmp&port=5433",
        "postgresql://u@h/d%20b?application_name=a%20b",
        "postgres://h/db",
    ],
)
def test_url_render_round_trips_and_stays_a_url(value: str) -> None:
    parsed = dsn.parse(value)
    rendered = parsed.render()
    assert rendered.startswith(f"{parsed.scheme}://")
    assert dsn.parse(rendered) == parsed


@pytest.mark.parametrize(
    "value",
    [
        "host=h port=5432 dbname=db user=u password=p",
        "host=h password='a b\\'c' dbname=db",
        "host=/tmp dbname=db",
        "host=h1,h2 port=5432 dbname=db",
        "service=okg dbname=db",
    ],
)
def test_keyword_render_round_trips_and_stays_keyword(value: str) -> None:
    parsed = dsn.parse(value)
    rendered = parsed.render()
    assert dsn.form_of(rendered) == "keyword"
    assert dsn.parse(rendered) == parsed


@pytest.mark.parametrize(
    ("value", "url"),
    [
        (
            "host=h port=5432 dbname=db user=u password='a b'",
            "postgresql://u:a%20b@h:5432/db",
        ),
        ("host=/tmp port=5433 dbname=db", "postgresql:///db?host=%2Ftmp&port=5433"),
        ("host=::1 dbname=db", "postgresql://[::1]/db"),
        ("host=h1,h2 port=5432 dbname=db", "postgresql:///db?host=h1%2Ch2&port=5432"),
        (
            "host=h1,h2,h3 port=1,2 dbname=db",
            "postgresql:///db?host=h1%2Ch2%2Ch3&port=1%2C2",
        ),
        ("service=okg dbname=db", "postgresql:///db?service=okg"),
        ("dbname=db options='-c a=b'", "postgresql:///db?options=-c%20a%3Db"),
    ],
)
def test_to_url_is_the_explicit_conversion_and_means_the_same(
    value: str, url: str
) -> None:
    parsed = dsn.parse(value)
    assert parsed.to_url() == url
    assert dict(dsn.parse(url).params) == dict(parsed.params)


def test_to_url_refuses_a_spelling_that_changes_the_connection() -> None:
    # An explicitly empty host has no URL spelling: the URL would drop it.
    odd = dsn.parse("host='' dbname=d")
    assert odd.params == {"host": "", "dbname": "d"}
    with pytest.raises(DsnRenderError) as raised:
        odd.to_url()
    assert raised.value.data == {"keys": ["host"]}


def test_make_conninfo_style_rewrite_keeps_the_url_form() -> None:
    """#2285 item 6: `make_conninfo(url, dbname=x)` turned a URL into keyword form."""
    parsed = dsn.parse(
        f"postgresql://u:{SECRET}@h:5432/okg?sslmode=require&options=-c%20search_path%3Dokg"
    )
    rewritten = parsed.with_dbname("postgres")
    assert rewritten.form == "url"
    assert rewritten.render().startswith("postgresql://")
    assert dict(dsn.parse(rewritten.render()).params) == {
        **parsed.params,
        "dbname": "postgres",
    }
    # The query string (sslmode, options) survives, unlike deployment_isolation._admin_dsn.
    assert rewritten.params["sslmode"] == "require"
    assert rewritten.options == "-c search_path=okg"


def test_keyword_rewrite_keeps_the_keyword_form() -> None:
    parsed = dsn.parse("host=h dbname=a password=p")
    assert parsed.with_dbname("b").form == "keyword"
    assert dsn.form_of(parsed.with_dbname("b").render()) == "keyword"


def test_with_params_sets_removes_and_keeps_order() -> None:
    parsed = dsn.parse("postgresql://u:p@h/db?sslmode=require")
    changed = parsed.with_params(user="v", password=None, connect_timeout=5)
    assert changed.user == "v"
    assert changed.password is None
    assert changed.params["connect_timeout"] == "5"
    assert parsed.password == "p"  # the original is untouched


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("postgresql://h/db", "okg-store"),
        ("postgresql://h/db?application_name=mine", "mine"),
        ("postgresql://h/db?application_name=", "okg-store"),
        ("host=h dbname=db application_name=mine", "mine"),
    ],
)
def test_with_params_if_absent_lets_the_dsn_keep_its_own_name(
    value: str, expected: str
) -> None:
    """The store rule: a DSN that names itself keeps its name (#2027's okg side)."""
    parsed = dsn.parse(value)
    stamped = parsed.with_params(
        application_name="okg-store", if_absent={"application_name"}
    )
    assert stamped.application_name == expected


def test_with_params_without_if_absent_lets_the_caller_win() -> None:
    """The connection.connect rule: the component's own name wins."""
    parsed = dsn.parse("postgresql://h/db?application_name=mine")
    assert (
        parsed.with_params(application_name="okg-worker").application_name
        == "okg-worker"
    )


def test_with_params_refuses_if_absent_for_a_key_it_does_not_change() -> None:
    with pytest.raises(ValueError, match="application_name"):
        dsn.parse("postgresql://h/db").with_params(
            if_absent={"application_name"}, dbname="x"
        )


@pytest.mark.parametrize(
    ("value", "fragments", "expected"),
    [
        (
            "postgresql://h/db?options=-c%20search_path%3Dokg",
            ("-c statement_timeout=5",),
            "-c search_path=okg -c statement_timeout=5",
        ),
        ("postgresql://h/db", ("-c a=1", "", "  ", "-c b=2"), "-c a=1 -c b=2"),
        (
            "host=h dbname=db options='-c x=1'",
            ("-c default_transaction_read_only=on",),
            "-c x=1 -c default_transaction_read_only=on",
        ),
        ("postgresql://h/db?options=-c%20a%3D1", (), "-c a=1"),
    ],
)
def test_with_options_appends_and_never_replaces(
    value: str, fragments: tuple[str, ...], expected: str
) -> None:
    assert dsn.parse(value).with_options(*fragments).options == expected


def test_with_options_of_nothing_leaves_no_options_key() -> None:
    assert "options" not in dsn.parse("postgresql://h/db").with_options("").params


def test_without_password() -> None:
    parsed = dsn.parse(f"postgresql://u:{SECRET}@h/db").without_password()
    assert parsed.password is None
    assert SECRET not in parsed.render()


def test_to_libpq_env_keeps_options_and_passfile() -> None:
    """Two of the three private copies dropped `options` and `passfile`."""
    parsed = dsn.parse(
        "postgresql://u:p@h:5433/db?options=-c%20search_path%3Dokg&passfile=%2Fpf&sslmode=require"
        "&application_name=app&connect_timeout=3&target_session_attrs=read-write"
    )
    assert parsed.to_libpq_env() == {
        "PGUSER": "u",
        "PGPASSWORD": "p",
        "PGDATABASE": "db",
        "PGHOST": "h",
        "PGPORT": "5433",
        "PGOPTIONS": "-c search_path=okg",
        "PGPASSFILE": "/pf",
        "PGSSLMODE": "require",
        "PGAPPNAME": "app",
        "PGCONNECT_TIMEOUT": "3",
        "PGTARGETSESSIONATTRS": "read-write",
    }


@pytest.mark.parametrize("extra", ["sslpassword=x", "keepalives=1"])
def test_to_libpq_env_refuses_a_parameter_with_no_environment_variable(
    extra: str,
) -> None:
    with pytest.raises(DsnRenderError) as raised:
        dsn.parse(f"host=h dbname=db {extra}").to_libpq_env()
    key = extra.split("=")[0]
    assert raised.value.data == {"keys": [key]}
    assert "x" not in raised.value.data["keys"]


class _FakeInfo:
    """The two members of psycopg's ConnectionInfo that the rebuild reads."""

    def __init__(self, params: dict[str, str], password: str | None) -> None:
        self._params = params
        self.password = password

    def get_parameters(self) -> dict[str, str]:
        return dict(self._params)


@pytest.mark.parametrize(
    ("params", "url_prefix"),
    [
        (
            {"host": "::1", "port": "5433", "user": "u", "dbname": "db"},
            "postgresql://u:",
        ),
        ({"host": "/tmp", "user": "u@x", "dbname": "db"}, "postgresql://u%40x:"),
        (
            {"host": "h", "user": "u", "dbname": "db", "sslmode": "require"},
            "postgresql://u:",
        ),
    ],
)
def test_from_connection_info_rebuilds_ipv6_sockets_and_sslmode(
    params: dict[str, str], url_prefix: str
) -> None:
    """#2285 item 6: the hand-written conn.info rebuilders broke all three."""
    rebuilt = Dsn.from_connection_info(_FakeInfo(params, f"{SECRET}/:@"))
    url = rebuilt.render()
    assert url.startswith(url_prefix)
    assert dict(dsn.parse(url).params) == {**params, "password": f"{SECRET}/:@"}
    assert SECRET not in repr(rebuilt)


def test_from_connection_info_keyword_form_and_no_password() -> None:
    rebuilt = Dsn.from_connection_info(
        _FakeInfo({"host": "h", "dbname": "db"}, None), form="keyword"
    )
    assert rebuilt.form == "keyword"
    assert rebuilt.password is None
    assert dict(dsn.parse(rebuilt.render()).params) == {"host": "h", "dbname": "db"}


@pytest.mark.parametrize(
    "password", ["p@ss/w:rd?#&=", "a b'c\\d", "${NOT_A_VAR}", "ünïcødé"]
)
def test_build_quotes_every_part(password: str) -> None:
    built = Dsn.build(
        host="db.example",
        port=5433,
        user="u@x",
        password=password,
        dbname="my db",
        sslmode="require",
    )
    parsed = dsn.parse(built.render())
    assert dict(parsed.params) == {
        "user": "u@x",
        "password": password,
        "host": "db.example",
        "port": "5433",
        "dbname": "my db",
        "sslmode": "require",
    }


# ---------------------------------------------------------------------------
# Placeholders (#1980)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("template", "names"),
    [
        ("${OKG_DSN}", ["OKG_DSN"]),
        ("postgresql://${USER}:${PW}@${HOST}/db?x=${USER}", ["HOST", "PW", "USER"]),
        ("$OKG_DSN", ["OKG_DSN"]),
        ("  $OKG_DSN  ", ["OKG_DSN"]),
        ("postgresql://u:pa$word@h/db", []),
        ("$OKG_DSN/extra", []),
        ("${X:-default}", []),
        ("${ X }", []),
        ("$(cat pw)", []),
        ("no placeholders", []),
    ],
)
def test_placeholders(template: str, names: list[str]) -> None:
    assert dsn.placeholders(template) == names


@pytest.mark.parametrize(
    ("template", "env", "value"),
    [
        ("${OKG_DSN}", {"OKG_DSN": "postgresql://h/db"}, "postgresql://h/db"),
        # #1980 item 1: the whole-value `$NAME` spelling expands.
        ("$OKG_DSN", {"OKG_DSN": "postgresql://h/db"}, "postgresql://h/db"),
        (" $OKG_DSN\n", {"OKG_DSN": "postgresql://h/db"}, "postgresql://h/db"),
        ("postgresql://u:${PW}@h/db", {"PW": "p%40ss"}, "postgresql://u:p%40ss@h/db"),
        # An embedded bare `$word` is a literal password, not a placeholder.
        ("postgresql://u:pa$word@h/db", {}, "postgresql://u:pa$word@h/db"),
        # #1980 item 3's workaround: percent-encode a literal ${WORD}.
        ("postgresql://u:a%24%7Bb%7Dc@h/db", {}, "postgresql://u:a%24%7Bb%7Dc@h/db"),
        # Expanded values are not rescanned.
        ("${A}", {"A": "${B}", "B": "no"}, "${B}"),
    ],
)
def test_expand_resolves(template: str, env: dict[str, str], value: str) -> None:
    result = dsn.expand(template, env)
    assert result.ok
    assert result.value == value
    assert dsn.require_expanded(template, source="test", lookup=env) == value


def test_expand_reads_os_environ_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_DSN_TEST_DSN1", "postgresql://h/db")
    assert dsn.expand("${OKG_DSN_TEST_DSN1}").value == "postgresql://h/db"
    monkeypatch.delenv("OKG_DSN_TEST_DSN1")
    assert dsn.expand("${OKG_DSN_TEST_DSN1}").unset == ("OKG_DSN_TEST_DSN1",)


def test_expand_accepts_a_callable_lookup_that_owns_precedence() -> None:
    process = {"OKG_DSN": ""}
    local_env = {"OKG_DSN": "postgresql://local/db"}

    def lookup(name: str) -> str | None:
        return process.get(name) or local_env.get(name)

    assert dsn.expand("${OKG_DSN}", lookup).value == "postgresql://local/db"


def test_expand_separates_unset_from_set_but_empty() -> None:
    """#1980 item 2: an empty variable was reported as unset."""
    result = dsn.expand("postgresql://${U}:${P}@${H}/db", {"U": "", "H": "h"})
    assert result.value is None
    assert result.unset == ("P",)
    assert result.empty == ("U",)
    assert result.resolved == ("H",)


@pytest.mark.parametrize(
    ("unsupported", "described"),
    [
        ("${OKG_DSN:-postgresql://h/db}", "${OKG_DSN:-...}"),
        ("${OKG_DSN-x}", "${OKG_DSN-...}"),
        ("${OKG_DSN:=x}", "${OKG_DSN:=...}"),
        ("${OKG_DSN:?missing}", "${OKG_DSN:?...}"),
        ("${ OKG_DSN }", "${...}"),
        ("${1ABC}", "${...}"),
        ("${OKG_DSN", "${... (unterminated)"),
        ("$(cat /pw)", "$(...)"),
    ],
)
def test_expand_refuses_shell_operators_by_name(
    unsupported: str, described: str
) -> None:
    """#1980 item 1: `${OKG_DSN:-...}` passed through to libpq untouched."""
    result = dsn.expand(unsupported, {"OKG_DSN": "postgresql://h/db", "ABC": "x"})
    assert result.value is None
    assert result.unsupported == (described,)
    with pytest.raises(DsnPlaceholderUnsupported) as raised:
        dsn.require_expanded(
            unsupported, source="--postgres-dsn", lookup={"OKG_DSN": "x"}
        )
    assert raised.value.code == "dsn_placeholder_unsupported"
    assert "--postgres-dsn" in raised.value.message


def test_require_expanded_unset_names_the_variable_and_the_workaround() -> None:
    with pytest.raises(DsnPlaceholderUnset) as raised:
        dsn.require_expanded(
            "postgresql://u:a${b}c@h/db", source="--postgres-dsn", lookup={}
        )
    error = raised.value
    assert error.code == "dsn_placeholder_unset"
    assert "b" in error.data["unset"]
    assert "%24%7BWORD%7D" in error.message
    assert error.data == {
        "source": "--postgres-dsn",
        "unset": ["b"],
        "empty": [],
        "unsupported": [],
    }


def test_require_expanded_empty_says_set_but_empty() -> None:
    with pytest.raises(DsnPlaceholderEmpty) as raised:
        dsn.require_expanded("${OKG_DSN}", source="OKG_DSN", lookup={"OKG_DSN": ""})
    assert raised.value.code == "dsn_placeholder_empty"
    assert "set but empty" in raised.value.message


def test_require_expanded_never_shows_a_value_or_a_secret_in_a_default() -> None:
    template = f"${{OKG_DSN:-postgresql://u:{SECRET}@h/db}}"
    with pytest.raises(DsnPlaceholderUnsupported) as raised:
        dsn.require_expanded(template, source="manifest", lookup={})
    text = raised.value.message + repr(raised.value.data)
    assert SECRET not in text
    assert "postgresql" not in text
    assert "${OKG_DSN:-...}" in text


# Review of PR 2309, B2: unsupported syntax inside a password repeated the
# password in the message, `data`, the traceback and `repr(Expansion)`.
PLACEHOLDER_LEAK_CASES = [
    (f"postgresql://u:pa${{{SECRET}@h/db", "${... (unterminated)", None),
    (f"postgresql://u:${{PW:-{SECRET}}}@h/db", "${PW:-...}", "PW"),
    (f"postgresql://u:a$({SECRET})@h/db", "$(...)", None),
    (f"postgresql://u:${{ {SECRET} }}@h/db", "${...}", None),
    (f"postgresql://u:${{PW:?{SECRET} x}}@h/db", "${PW:?...}", "PW"),
]


@pytest.mark.parametrize(
    ("template", "described", "name"),
    PLACEHOLDER_LEAK_CASES,
    ids=["unterminated", "default", "subshell", "blanks", "error-word"],
)
def test_placeholder_errors_name_the_construct_never_template_text(
    template: str, described: str, name: str | None
) -> None:
    import traceback

    result = dsn.expand(template, {"PW": "x"})
    assert result.unsupported == (described,)
    assert SECRET not in repr(result)
    assert SECRET not in "".join(result.unsupported)
    with pytest.raises(DsnPlaceholderUnsupported) as raised:
        dsn.require_expanded(template, source="OKG_DSN", lookup={"PW": "x"})
    error = raised.value
    printed = "".join(traceback.format_exception(error))
    for surface in (error.message, str(error), repr(error.data), printed):
        assert SECRET not in surface
        assert "@h/db" not in surface
    assert error.data["unsupported"] == [described]
    if name is not None:
        assert name in error.message


def test_require_expanded_error_does_not_repeat_resolved_values() -> None:
    with pytest.raises(DsnPlaceholderUnset) as raised:
        dsn.require_expanded(
            "postgresql://u:${PW}@${HOST}/db", source="x", lookup={"PW": SECRET}
        )
    assert SECRET not in raised.value.message + repr(raised.value.data)


def test_expansion_repr_hides_the_value() -> None:
    result = dsn.expand("${PW}", {"PW": SECRET})
    assert SECRET not in repr(result)


# ---------------------------------------------------------------------------
# Redaction (#2230, #2285)
# ---------------------------------------------------------------------------

REDACT_TEXT_CASES = [
    # --- URL userinfo -----------------------------------------------------
    ("postgresql://u:pw@h/db", "postgresql://u:***@h/db"),
    (
        "postgres://u:pw@h:5432/db?sslmode=require",
        "postgres://u:***@h:5432/db?sslmode=require",
    ),
    # Empty user (#2230 item 4).
    ("postgresql://:secret@h/db", "postgresql://:***@h/db"),
    # #2285 item 2: raw `@` in the password.
    ("postgresql://u:a@b@h/db", "postgresql://u:***@h/db"),
    # #2230 item 2 / #2285 item 4: raw `/` in the password.
    ("postgresql://u:a/b@h/db", "postgresql://u:***@h/db"),
    # #2285 item 3: `:` in the password.
    ("postgresql://u:a:b:c@h/db", "postgresql://u:***@h/db"),
    ("postgresql://u:pa?ss@h/db", "postgresql://u:***@h/db"),
    ("postgresql://u:pa#ss@h/db", "postgresql://u:***@h/db"),
    ("postgresql://u:pa'ss@h/db", "postgresql://u:***@h/db"),
    ("postgresql://u:p%40%2Fss@h/db", "postgresql://u:***@h/db"),
    # A raw blank in a URL password.
    ("postgresql://u:my secret@h/db ok", "postgresql://u:***@h/db ok"),
    # Any scheme: https tokens are masked too.
    (
        "https://user:ghp_token@github.example.com/org/repo",
        "https://user:***@github.example.com/org/repo",
    ),
    # #2230 item 3, last clause: accepted fail-safe over-mask of a port.
    ("file://host:8080@x/y", "file://host:***@x/y"),
    # Not a password: host:port and IPv6.
    ("postgresql://h:5432/db", "postgresql://h:5432/db"),
    ("postgresql://[::1]:5432/db", "postgresql://[::1]:5432/db"),
    ("postgresql://h1:1,h2:2/db", "postgresql://h1:1,h2:2/db"),
    ("postgresql://u@h/db", "postgresql://u@h/db"),
    # Inside a placeholder default.
    ("${OKG_DSN:-postgresql://u:pw@h/db}", "${OKG_DSN:-postgresql://u:***@h/db}"),
    # --- query parameters (#2230 item 3) -----------------------------------
    # A query password runs to the end of the line (or the closing quote of a
    # quoted DSN): a raw `&` inside it must not end the mask. Over-masks the
    # parameters after it.
    (
        "postgresql://u@h/db?password=zz&sslmode=require",
        "postgresql://u@h/db?password=***",
    ),
    (
        "failed: 'postgresql://u@h/db?password=zz&sslmode=require' at 3",
        "failed: 'postgresql://u@h/db?password=***' at 3",
    ),
    # #2285 item 3: a query password with no userinfo password keeps the scheme.
    ("postgresql://h/db?password=zz", "postgresql://h/db?password=***"),
    # A query password runs to the next `&` or the end of the line; a raw
    # `#` or blank inside it does not end it, so a real fragment is masked too.
    (
        "postgresql://h/db?sslmode=require&PASSWORD=zz#frag",
        "postgresql://h/db?sslmode=require&PASSWORD=***",
    ),
    ("postgresql://h/db?sslpassword=zz", "postgresql://h/db?sslpassword=***"),
    ("postgresql://h/db?passfile=%2Fhome%2Fpf", "postgresql://h/db?passfile=***"),
    # --- keyword form -------------------------------------------------------
    ("host=h password=pw dbname=x", "host=h password=*** dbname=x"),
    ("host=h password = pw dbname=x", "host=h password = *** dbname=x"),
    ("host=h password=my secret dbname=x", "host=h password=*** dbname=x"),
    ("host=h password='a b\\' c' dbname=x", "host=h password=*** dbname=x"),
    ('host=h password="a b" dbname=x', "host=h password=*** dbname=x"),
    ("host=h password='ab'cd dbname=x", "host=h password=*** dbname=x"),
    ("host=h password='unterminated dbname=x", "host=h password=***"),
    # libpq skips blanks after `=`, so `dbname=x` IS the password here.
    ("password= dbname=x", "password= ***"),
    # #2285 item 2: sslpassword was not masked.
    ("host=h sslpassword=pw dbname=x", "host=h sslpassword=*** dbname=x"),
    ("host=h passfile=/home/u/.pgpass dbname=x", "host=h passfile=*** dbname=x"),
    (
        "host=h oauth_client_secret=pw dbname=x",
        "host=h oauth_client_secret=*** dbname=x",
    ),
    ("--password=pw --host h", "--password=***"),
    # --- environment assignments (#2285 item 2) ----------------------------
    # Masked to the end of the line: `env` never quotes a value, so a blank
    # does not end it (review of PR 2309, B3). Over-masks what follows.
    ("export PGPASSWORD=pw && psql", "export PGPASSWORD=***"),
    ("PGSSLPASSWORD=pw pg_dump", "PGSSLPASSWORD=***"),
    ("PGPASSWORD: 'a b'", "PGPASSWORD: ***"),
    ('"PGPASSWORD": "pw", "PGHOST": "h"', '"PGPASSWORD": ***'),
    # Prefixed names ending in PASSWORD or PASSWD (review of PR 2309, S3).
    ("OKG_PGPASSWORD=pw", "OKG_PGPASSWORD=***"),
    ("DBPASSWORD=pw", "DBPASSWORD=***"),
    ("POSTGRES_PASSWORD: pw", "POSTGRES_PASSWORD: ***"),
    ("export DB_PASSWD=pw", "export DB_PASSWD=***"),
    # Not a secret assignment: a path variable, a lower-case word.
    ("PGPASSWORD_FILE=/run/pw ok", "PGPASSWORD_FILE=/run/pw ok"),
    ("OKG_PASSWORDLESS=1 ok", "OKG_PASSWORDLESS=1 ok"),
    # --- DSN flags (#2230 item 1) ------------------------------------------
    (
        "okg install --postgres-dsn postgresql://u:pw@h/db --yes",
        "okg install --postgres-dsn <redacted-dsn> --yes",
    ),
    ("okg run --dsn=host=h --yes", "okg run --dsn=<redacted-dsn> --yes"),
    # Over-mask: the keyword rule cannot see the flag's closing quote.
    ("okg run --dsn 'host=h password=pw' --yes", "okg run --dsn <redacted-dsn>"),
    ("okg run --dsn 'host=h dbname=x' --yes", "okg run --dsn <redacted-dsn> --yes"),
    ("okg run --dsn-file /x", "okg run --dsn-file /x"),
    # --- nothing to mask ----------------------------------------------------
    ("", ""),
    (
        "plain text with an email a@b.c and a time 12:30",
        "plain text with an email a@b.c and a time 12:30",
    ),
    ("pa$word and ${VAR}", "pa$word and ${VAR}"),
]

# Review of PR 2309, B3: whole passwords shown. SECRET stands for the password.
REVIEW_2309_LEAK_CASES = [
    (
        f"HOME=/x\nPGPASSWORD=my {SECRET} pw\nUSER=u",
        "HOME=/x\nPGPASSWORD=***\nUSER=u",
    ),
    (f"postgresql://u:2024 {SECRET}@h/db", "postgresql://u:***@h/db"),
    (f"postgresql://u@h/db?password=a {SECRET}", "postgresql://u@h/db?password=***"),
    (f"postgresql://u:p@{SECRET} w@rd@h/db", "postgresql://u:***@h/db"),
    (
        f"postgresql://u@h/db?password=a {SECRET}&sslmode=require",
        "postgresql://u@h/db?password=***",
    ),
    # Coordinator follow-up: a raw `&` inside a query password.
    (
        f"postgresql://u@h/db?password=ab&{SECRET}word=1",
        "postgresql://u@h/db?password=***",
    ),
    (
        f'x "postgresql://h/db?sslpassword=a&{SECRET} b", y',
        'x "postgresql://h/db?sslpassword=***", y',
    ),
    (f"OKG_PGPASSWORD={SECRET}\nnext", "OKG_PGPASSWORD=***\nnext"),
    (f"DBPASSWORD={SECRET}", "DBPASSWORD=***"),
    # Re-review of PR 2309, N2: an escaped quote inside a quoted query
    # password is not its closing quote (json.dumps and repr write these).
    (
        f'{{"dsn": "postgresql://u@h/db?password=ZQ\\"{SECRET}", "x": 1}}',
        '{"dsn": "postgresql://u@h/db?password=***", "x": 1}',
    ),
    (
        f"'postgresql://u@h/db?password=ZQ\\'\"{SECRET}'",
        "'postgresql://u@h/db?password=***'",
    ),
    (
        repr(f"postgresql://u@h/db?password=ZQ'\"{SECRET}"),
        "'postgresql://u@h/db?password=***'",
    ),
]


@pytest.mark.parametrize(
    ("text", "expected"),
    REVIEW_2309_LEAK_CASES,
    ids=[
        "env-dump-blank",
        "digits-first-password",
        "query-password-blank",
        "many-at-signs",
        "query-password-blank-then-param",
        "query-password-raw-ampersand",
        "quoted-dsn-query-password-raw-ampersand",
        "prefixed-pgpassword",
        "prefixed-dbpassword",
        "json-escaped-quote-query-password",
        "repr-escaped-quote-query-password",
        "python-repr-both-quote-kinds",
    ],
)
def test_review_2309_leaks_are_masked_by_every_masker(text: str, expected: str) -> None:
    assert dsn.redact_text(text) == expected
    assert SECRET not in dsn.redact(text)
    assert SECRET not in repr(dsn.redact_value({"k": [text]}))
    assert not any(SECRET in token for token in dsn.redact_argv([text]))


@pytest.mark.parametrize(
    "text",
    [
        "redis://h:6379/0 contact admin@example.com",
        "error at https://h:8443/x?a=1 -- mail ops@example.com for help",
        "GET https://api.github.com:443/repos/x 200 user=bot@example.com",
        "postgresql://h:5432/db failed; ask dba@example.com",
    ],
)
def test_url_with_a_port_then_an_at_sign_is_over_masked(text: str) -> None:
    """Coordinator decision after review of PR 2426: a port-shaped run with an
    `@` later on the line may be a password with blanks (`u:12/a b c@h`), so
    it is masked to the last `@`. This costs the prose after a port that
    names an e-mail address, the over-mask the re-review of PR 2309 (S1)
    first objected to; fail-closed wins."""
    head = text.split("://", 1)[0] + "://"
    for masked in (dsn.redact_text(text), dsn.redact(text)):
        assert masked != text
        assert masked.startswith(head)
        assert "***@" in masked


@pytest.mark.parametrize(
    "text",
    [
        "redis://h:6379/0 contact the admin",
        "postgresql://h:5432/db failed; retry",
        "GET https://api.github.com:443/repos/x 200",
    ],
)
def test_url_with_a_port_and_no_at_sign_is_intact(text: str) -> None:
    assert dsn.redact_text(text) == text
    assert dsn.redact(text) == text


def test_digits_then_blank_password_is_still_masked_next_to_the_port_rule() -> None:
    """Re-review of PR 2309, S1: the port rule must not undo the B3 fix."""
    text = f"postgresql://u:2024 {SECRET}@h/db contact dba@example.com"
    assert SECRET not in dsn.redact_text(text)
    assert dsn.redact_text(f"postgresql://u:5432/{SECRET}@h/db") == (
        "postgresql://u:***@h/db"
    )


@pytest.mark.parametrize(
    "text",
    [
        # Review of PR 2426, B1 (a): the `@` is in the next word.
        f"postgresql://u:12/{SECRET} x@h/db",
        f"postgresql://u:12/{SECRET} x@h",
        # The `@` is further on, but followed by a host and `/ : ?`.
        f"postgresql://u:12/{SECRET} two words@h/db",
        f"postgresql://u:12/{SECRET} two words@h:5432",
    ],
)
def test_digits_slash_blank_password_is_masked(text: str) -> None:
    for masked in (dsn.redact_text(text), dsn.redact(text)):
        assert SECRET not in masked
        assert masked.startswith("postgresql://u:***@h")


@pytest.mark.parametrize(
    "text",
    [
        f"postgresql://u:12/{SECRET} b c@h",
        f"postgresql://u:12/a {SECRET} c d@h is down",
        f"okg --dsn 'postgresql://u:12/{SECRET} b c@h/db' --json",
    ],
)
def test_digits_slash_password_with_several_blanks_is_masked(text: str) -> None:
    """Coordinator decision after review of PR 2426: the shape the port rule
    used to keep (`u:12/a b c@h`) is masked to the last `@`."""
    for masked in (
        dsn.redact_text(text),
        dsn.redact(text),
        dsn.redact_value([text])[0],
    ):
        assert SECRET not in masked
        assert " b c@" not in masked


@pytest.mark.parametrize(
    "argv",
    [
        ["psql", f"host=h password='{SECRET} zqb2' dbname=x", "-c", "select 1"],
        ["psql", f"host=h password='{SECRET} zqb2'"],
        ["pg_dump", "-d", f"host=h password='a b {SECRET}' dbname=x"],
    ],
)
def test_shlex_joined_command_masks_the_whole_quoted_password(argv: list[str]) -> None:
    """Review of PR 2426, B1 (b): `shlex.join` writes an inner single quote as
    '"'"', and the masker stopped at the first bare quote."""
    joined = shlex.join(argv)
    for masked in (
        dsn.redact_text(joined),
        dsn.redact(joined),
        dsn.redact_value({"k": [joined]})["k"][0],
    ):
        assert SECRET not in masked
        assert "zqb2" not in masked
        assert "password=***" in masked
    posix = joined.replace("'\"'\"'", "'\\''")
    assert SECRET not in dsn.redact_text(posix)
    assert "zqb2" not in dsn.redact_text(posix)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # Review of PR 2426, B1 (c): libpq accepts a line break inside a
        # quoted value.
        (
            f"host=h password='{SECRET}\nzqb2' dbname=x",
            "host=h password=***\n*** dbname=x",
        ),
        (
            f"stage failed\nconnect 'host=h password=\\'{SECRET}\nzqb2\\' dbname=x'\nhint",
            None,
        ),
        # Review of PR 2426, B1 (d): a password wrapped across log lines.
        (
            f"conn=postgresql://u:{SECRET}\nzqb2@h/db failed",
            "conn=postgresql://u:***\n***@h/db failed",
        ),
        (
            f"password={SECRET}\nzqb2 dbname=x",
            "password=***\n*** dbname=x",
        ),
    ],
)
def test_password_across_a_line_break_is_masked(
    text: str, expected: str | None
) -> None:
    for masked in (dsn.redact_text(text), dsn.redact_value({"k": text})["k"]):
        assert SECRET not in masked
        assert "zqb2" not in masked
        if expected is not None:
            assert masked == expected
    assert SECRET not in dsn.redact(text) and "zqb2" not in dsn.redact(text)


def test_a_wrapped_unquoted_password_masks_at_most_the_next_line() -> None:
    """Coordinator decision after #2285 was found reopened: the wrapped
    remainder can only land on the next line, so only that line is masked."""
    text = f"password={SECRET[:4]}\n{SECRET[4:]}\nunrelated line"
    for masked in (dsn.redact_text(text), dsn.redact_value({"k": text})["k"]):
        assert masked == "password=***\n***\nunrelated line"


def test_a_wrapped_password_before_a_traceback_keeps_the_later_lines() -> None:
    text = "password=abc\nTraceback line 2\nline 3 dbname=x"
    assert dsn.redact_text(text) == "password=***\n***\nline 3 dbname=x"
    assert dsn.redact_value([text]) == ["password=***\n***\nline 3 dbname=x"]


_DONT_RETRY_TAIL = "\nKEEP2 File x\nKEEP3 hint: don't retry\nKEEP4"
_DONT_RETRY_LOG = (
    "FATAL: bad conninfo host=h password='zqa1\nKEEP1 Traceback" + _DONT_RETRY_TAIL
)
_DONT_RETRY_MASKED = "FATAL: bad conninfo host=h password=***\n***" + _DONT_RETRY_TAIL


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # Review of PR 2425 (final), S1: the mask ran to the next apostrophe
        # anywhere in the text (`don't`), hiding the lines in between.
        (
            _DONT_RETRY_LOG,
            _DONT_RETRY_MASKED,
        ),
        (
            "sslpassword='zqa1\nKEEP1\nKEEP2 user's\nKEEP3",
            "sslpassword=***\n***\nKEEP2 user's\nKEEP3",
        ),
        # No closing quote anywhere: the same one-line bound.
        (
            f"host=h password='{SECRET}\nnext line\nlast",
            "host=h password=***\n***\nlast",
        ),
        # A ` key=` on the next line ends the mask there; one at its start
        # ends it at the line break.
        ("x password='a\nb c=d\nKEEP", "x password=***\n*** c=d\nKEEP"),
        ("password='a\ndbname=x\nKEEP", "password=***\ndbname=x\nKEEP"),
        # A quote closed on its own line is unchanged.
        ("host=h password='a b' dbname=x\nKEEP", "host=h password=*** dbname=x\nKEEP"),
    ],
)
def test_an_unclosed_quote_masks_at_most_the_next_line(
    text: str, expected: str
) -> None:
    assert dsn.redact_text(text) == expected
    assert dsn.redact_value([text]) == [expected]


# Re-review of PR 2309, N1: libpq quotes the fragment without escaping an
# inner `"`, so pairing quotes printed password text between the pairs.
DOUBLE_QUOTE_PARSE_CASES = [
    f'postgresql://u:a"{SECRET} b@h/db',
    f'postgresql://u:a"{SECRET}" b@h/db',
    f'postgresql://u@h/db?password=a"{SECRET} b',
    f'host=h password=x "{SECRET}" y dbname=d',
]


@pytest.mark.parametrize(
    "value",
    DOUBLE_QUOTE_PARSE_CASES,
    ids=["url-userinfo", "url-userinfo-two-quotes", "url-query", "keyword"],
)
def test_parse_error_with_a_double_quote_in_the_password_shows_no_fragment(
    value: str,
) -> None:
    import traceback

    with pytest.raises(DsnParseError) as raised:
        dsn.parse(value)
    error = raised.value
    printed = "".join(traceback.format_exception(error))
    for surface in (error.message, str(error), repr(error), repr(error.data), printed):
        assert SECRET not in surface
    assert '"<fragment>"' in error.message
    assert dsn.try_parse(value) is None
    # to_url goes through parse, so it raises the same clean error.
    with pytest.raises(DsnParseError) as via_to_url:
        dsn.parse(value).to_url()
    assert SECRET not in via_to_url.value.message
    if value.startswith("host="):
        with pytest.raises(DsnFormError) as refused:
            dsn.require_url_form(value, source="OKG_DSN")
        assert SECRET not in refused.value.message


def test_parse_error_keeps_libpq_fixed_tokens() -> None:
    """The `"="` and `"]"` libpq writes itself survive the first-to-last rule."""
    with pytest.raises(DsnParseError) as raised:
        dsn.parse(f"host=h password=my {SECRET} dbname=x")
    assert 'missing "=" after "<fragment>"' in raised.value.message
    with pytest.raises(DsnParseError) as bracket:
        dsn.parse(f"postgresql://u:{SECRET}@[::1/db")
    assert 'matching "]" in' in bracket.value.message
    assert SECRET not in bracket.value.message


@pytest.mark.parametrize(("text", "expected"), REDACT_TEXT_CASES)
def test_redact_text_table(text: str, expected: str) -> None:
    assert dsn.redact_text(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # #2285 item 1: the lines after an unquoted password= were deleted.
        # An unquoted password that reaches the end of its line may go on
        # onto the next line (review of PR 2426), so that one line is masked;
        # every line after it is kept.
        (
            "password=abc\nTraceback line 2\nline 3 dbname=x",
            "password=***\n***\nline 3 dbname=x",
        ),
        (
            "host=h password=abc\r\nnext line\rlast",
            "host=h password=***\r\n***\rlast",
        ),
        # A key at the start of the next line ends the password there.
        ("password=abc\ndbname=x\nkeep", "password=***\ndbname=x\nkeep"),
        ("host=h password=abc dbname=x\nkeep", "host=h password=*** dbname=x\nkeep"),
        # #2230 item 1: a flag at the end of a line does not eat the next line.
        (
            "failed: okg install --postgres-dsn\nfollowing words stay",
            "failed: okg install --postgres-dsn\nfollowing words stay",
        ),
        ("--dsn\n--other", "--dsn\n--other"),
        ("PGPASSWORD=\nnext", "PGPASSWORD=***\nnext"),
        # Child output with a keyword-form DSN (#2230 item 3).
        (
            "stage migrate failed\nconnection to 'host=db password=pw dbname=okg' refused\nhint: retry",
            "stage migrate failed\nconnection to 'host=db password=*** dbname=okg' refused\nhint: retry",
        ),
        # Review of PR 2426, B1 (d): a URL password wrapped onto the next
        # line is masked up to that line's `@`; the rest of the line stays.
        ("a postgresql://u:x\nb@h", "a postgresql://u:***\n***@h"),
        # A wrapped unquoted password with no `key=` on the next line: nothing
        # marks where it ends, so that whole line is masked, and no more.
        ("password=abc\nnext words\nmore", "password=***\n***\nmore"),
        ("password=abc\n\nkeep", "password=***\n\nkeep"),
    ],
)
def test_redact_text_across_line_breaks(text: str, expected: str) -> None:
    assert dsn.redact_text(text) == expected


def test_redact_text_custom_flags() -> None:
    assert (
        dsn.redact_text("x --db-url postgresql://h/db", dsn_flags=("--db-url",))
        == f"x --db-url {REDACTED_DSN}"
    )
    assert dsn.redact_text("x --dsn v", dsn_flags=()) == "x --dsn v"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("postgresql://u:a@b@h/db", "postgresql://u:***@h/db"),
        ("host=h password=abc\ndbname=x", "host=h password=***"),
        ("host=h password='a\nb' dbname=x", "host=h password=*** dbname=x"),
        ("host=h dbname=x", "host=h dbname=x"),
    ],
)
def test_redact_treats_its_input_as_one_dsn(value: str, expected: str) -> None:
    assert dsn.redact(value) == expected


@pytest.mark.parametrize("value", [None, 5, b"postgresql://u:p@h/db", ["x"]])
def test_redact_and_redact_text_pass_non_strings_through(value: object) -> None:
    assert dsn.redact(value) is value  # type: ignore[arg-type]
    assert dsn.redact_text(value) is value  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (
            ["okg", "install", "--postgres-dsn", "postgresql://u:pw@h/db"],
            ["okg", "install", "--postgres-dsn", REDACTED_DSN],
        ),
        (["okg", "--dsn=host=h password=pw"], ["okg", f"--dsn={REDACTED_DSN}"]),
        (
            ["okg", "--dsn", "host=h password=pw", "--yes"],
            ["okg", "--dsn", REDACTED_DSN, "--yes"],
        ),
        # A keyword-form DSN token outside a flag (#2230 item 3).
        (
            ["psql", "host=h password=pw dbname=x"],
            ["psql", "host=h password=*** dbname=x"],
        ),
        (["psql", "postgresql://u:a/b@h/db"], ["psql", "postgresql://u:***@h/db"]),
        (["env", "PGPASSWORD=pw", "pg_dump"], ["env", "PGPASSWORD=***", "pg_dump"]),
        (["okg", "--dsn"], ["okg", "--dsn"]),
        ([], []),
    ],
)
def test_redact_argv(argv: list[str], expected: list[str]) -> None:
    assert dsn.redact_argv(argv) == expected


def test_redact_argv_custom_flags() -> None:
    assert dsn.redact_argv(
        ["x", "--url", "postgresql://h/db"], dsn_flags=("--url",)
    ) == ["x", "--url", REDACTED_DSN]


def test_redact_value_recurses_and_keeps_keys() -> None:
    value = {
        "dsn": f"postgresql://u:{SECRET}@h/db",
        "argv": ["psql", f"host=h password={SECRET}"],
        "pair": (f"PGPASSWORD={SECRET}", 3),
        "nested": {"password=x": None, "n": 1.5},
    }
    redacted = dsn.redact_value(value)
    assert SECRET not in repr(redacted)
    assert redacted["nested"] == {"password=x": None, "n": 1.5}
    assert isinstance(redacted["pair"], tuple)
    assert redacted["dsn"] == "postgresql://u:***@h/db"


# CPU seconds under which a 1 MB redaction passes without a growth check. The
# slowest shape measures about 0.9 s on the laptop.
_LINEAR_REDACTION_CPU_BUDGET_SECONDS = 5.0
# CPU seconds at or over which a 1 MB redaction fails outright, whatever its
# growth: about 33 times the laptop's slowest shape, so a runner would have to
# be that slow, while a linear pass 50 times slower or an n^1.5 regression
# large enough to reach it fails.
_REDACTION_CPU_CEILING_SECONDS = 30.0
# Between the budget and the ceiling: the largest accepted exponent from a
# quarter-size input to the full one (linear 1.0, quadratic 2.0), and the most
# interleaved rounds. 1.4 accepts up to 1.74 times measurement noise.
_MAX_REDACTION_GROWTH_EXPONENT = 1.4
_REDACTION_GROWTH_ROUNDS = 3
# Wall seconds after which one redaction counts as hung. The slowest correct
# shape took 6.09 s of wall time on the most loaded CI pod on record.
_REDACTION_HANG_GUARD_SECONDS = 120.0


def _redaction_cpu_seconds(text: str) -> float:
    """This thread's CPU seconds for redact_text plus redact on `text`."""

    def _hang(signum, frame):  # noqa: ARG001 - signal handler signature
        raise TimeoutError(
            f"redaction ran longer than {_REDACTION_HANG_GUARD_SECONDS:.0f} s "
            f"on {len(text)} chars"
        )

    gc.collect()
    previous = signal.signal(signal.SIGALRM, _hang)
    signal.setitimer(signal.ITIMER_REAL, _REDACTION_HANG_GUARD_SECONDS)
    try:
        started = time.thread_time()
        dsn.redact_text(text)
        dsn.redact(text)
        return time.thread_time() - started
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.mark.parametrize(
    "text",
    [
        "password=" + " " * 1_000_000,
        "password=a" + " a" * 500_000,
        "password=" * 120_000,
        "a://b:" * 170_000,
        "postgresql://u:" + "x" * 1_000_000,
        "@" * 1_000_000,
        "postgresql://u:p@h " * 55_000,
        "--dsn " * 170_000,
        "PGPASSWORD=' " * 90_000,
        ("x" * 99 + "\n") * 10_000,
        # Review of PR 2309, B4: quadratic before the `covered` skip.
        "PGPASSWORD=:" * 90_000,
        "--dsn=:" * 150_000,
        "OKG_PGPASSWORD=" * 70_000,
        "postgresql://u:2024 " * 55_000,
        "?password=a " * 90_000,
        "'?password=a'" * 80_000,
        "a&password=" * 90_000,
        # Review of PR 2426, B1: the rules that reach across a line break.
        "password=a\nb dbname=x\n" * 50_000,
        "x postgresql://u:a\nb@h\n" * 50_000,
        "password='a\nb' \n" * 70_000,
        ("password='\"'\"'a\nb'\"'\"' \n") * 50_000,
        "password='\"'\"'ab " * 80_000,
        "postgresql://u:12/a b " * 50_000 + "c@h/db",
    ],
    ids=[
        "blank-run",
        "many-words",
        "repeated-keys",
        "repeated-schemes",
        "long-userinfo",
        "at-signs",
        "many-urls",
        "many-flags",
        "unterminated-quotes",
        "many-lines",
        "repeated-env-assignments",
        "repeated-flags-no-blank",
        "repeated-prefixed-env",
        "digits-first-userinfo",
        "repeated-query-passwords",
        "repeated-quoted-query-passwords",
        "repeated-query-passwords-no-blank",
        "wrapped-keyword-passwords",
        "wrapped-url-passwords",
        "quoted-passwords-across-lines",
        "shell-splices-across-lines",
        "unclosed-shell-splices",
        "port-runs-then-authority",
    ],
)
def test_redact_text_is_linear_on_1mb_inputs(text: str) -> None:
    """Fast enough on 1 MB, or else provably linear on this runner.

    Every shape here once made a redaction rule quadratic, which costs tens
    of CPU seconds or more at 1 MB. A 5 s wall-clock bound also failed
    correct code on loaded CI pods (6.09 s on train 7 run 36537497429, where
    the laptop measures 0.9 s), because wall time counts the time the pod
    spent waiting for a CPU.

    So the budget is CPU time. Past 30 CPU s the test fails outright. Between
    5 and 30 s it is not yet a verdict: the test times the same shape at a
    quarter of the size and fails if quadrupling the input costs more than
    4**1.4 (about 7) times as much. A linear pass costs about 4 times as much
    and a quadratic one about 16. A slow runner therefore passes a linear
    rule, and a quadratic or n^1.5 rule fails on any runner (PACT
    timing-tests-robust carries the mutation run). A call that runs past the
    wall-clock hang guard fails outright.

    The trade-off: a rule that is linear but up to 6 times slower than today
    (5 to 30 CPU s) now passes, where the old wall-clock bound failed it.
    """
    seconds = _redaction_cpu_seconds(text)
    if seconds < _LINEAR_REDACTION_CPU_BUDGET_SECONDS:
        return
    assert seconds < _REDACTION_CPU_CEILING_SECONDS, (
        f"redaction took {seconds:.2f} s CPU on {len(text)} chars; the ceiling is "
        f"{_REDACTION_CPU_CEILING_SECONDS} s whatever the growth"
    )
    # A quarter-size input that keeps the shape's head and tail, where several
    # shapes put the one token that triggers the rule.
    eighth = len(text) // 8
    quarter = text[:eighth] + text[-eighth:]
    t_quarter, t_full = math.inf, seconds
    for rounds in range(1, _REDACTION_GROWTH_ROUNDS + 1):
        t_quarter = min(t_quarter, _redaction_cpu_seconds(quarter))
        if rounds > 1:
            t_full = min(t_full, _redaction_cpu_seconds(text))
        exponent = math.log(t_full / t_quarter, 4)
        if exponent <= _MAX_REDACTION_GROWTH_EXPONENT:
            return
    pytest.fail(
        f"redaction took {seconds:.2f} s CPU on {len(text)} chars (budget "
        f"{_LINEAR_REDACTION_CPU_BUDGET_SECONDS} s) and grows as n^{exponent:.2f}: "
        f"best of {rounds} rounds {t_quarter:.2f} s at {len(quarter)} chars, "
        f"{t_full:.2f} s at {len(text)}; linear is n^1"
    )


def test_error_codes_are_registered_classes() -> None:
    assert DsnParseError.code == "dsn_parse_error"
    assert DsnFormError.code == "dsn_keyword_form"
    assert DsnRenderError.code == "dsn_render_refused"
    assert DsnPlaceholderUnset.code == "dsn_placeholder_unset"
    assert DsnPlaceholderEmpty.code == "dsn_placeholder_empty"
    assert DsnPlaceholderUnsupported.code == "dsn_placeholder_unsupported"
