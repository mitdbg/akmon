"""Property tests for `okg.substrate.db.dsn`: the maskers never leak, and
parsing a rendered DSN gives the same DSN back.

The table tests in `test_dsn.py` pin named cases. These generate passwords
from an alphabet that includes every character the old maskers stumbled on
(`@ / : ? # ' " \\ % & = $ { }`, blanks, non-ASCII) and check the one
property that matters: the password does not appear in the masked output, in
either DSN form, raw or percent-encoded, with noise lines around it -- and the
noise lines survive.
"""

from __future__ import annotations

import inspect
import json
import re
import shlex
from urllib.parse import quote

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from okg.substrate.db import dsn
from okg.substrate.db.dsn import Dsn

_SPECIALS = "@/:?#'\"\\%&=${}[]!*+,;~ \t"
_ALPHABET = st.characters(
    blacklist_categories=("Cs", "Cc"),
    blacklist_characters="\r\n\x0b\x0c\x1c\x1d\x1e\x85  ",
) | st.sampled_from(_SPECIALS)

# A password long enough to be unmistakable in the output.
PASSWORDS = st.text(_ALPHABET, min_size=8, max_size=40)
NO_BLANK_PASSWORDS = PASSWORDS.filter(lambda p: not any(c.isspace() for c in p))
# The shapes the review of PR 2309 found leaking (B3): a first word that is
# all digits (read as a port), a raw `/` after digits, several raw `@`, and
# raw blanks between the pieces.
_WORD = st.text(_ALPHABET, min_size=1, max_size=12)
RAW_PASSWORDS = st.one_of(
    PASSWORDS,
    st.builds(
        lambda digits, sep, rest: digits + sep + rest,
        st.from_regex(r"[0-9]{1,6}", fullmatch=True),
        st.sampled_from([" ", "/", "/ ", "\t", ""]),
        PASSWORDS,
    ),
    # Escaped quotes as json.dumps and repr write them (re-review N2).
    st.builds(
        lambda words, seps: "".join(w + s for w, s in zip(words, seps, strict=False)),
        st.lists(_WORD, min_size=2, max_size=4),
        st.lists(
            st.sampled_from(['\\"', "\\'", '\\\\"', "\\\\\\'", '"', "'"]),
            min_size=4,
            max_size=4,
        ),
    ).filter(lambda p: len(p) >= 8),
    st.builds(
        lambda words, seps: "".join(w + s for w, s in zip(words, seps, strict=False)),
        st.lists(_WORD, min_size=3, max_size=5),
        st.lists(st.sampled_from(["@", " ", "@ ", " @", "/"]), min_size=5, max_size=5),
    ).filter(lambda p: len(p) >= 8),
)
NOISE = st.lists(
    st.text(st.characters(whitelist_categories=("Lu", "Ll", "Nd", "Zs")), max_size=30),
    max_size=3,
)

SETTINGS = settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)


def _keyword_quoted(password: str) -> str:
    return "'" + password.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _templates(password: str) -> list[str]:
    """Every place the password can sit in DSN text."""
    encoded = quote(password, safe="")
    lines = [
        f"postgresql://okg:{encoded}@db.example:5432/okg?sslmode=require",
        f"postgres://:{encoded}@db.example/okg",
        f"postgresql://okg@db.example/okg?password={encoded}&sslmode=require",
        f"postgresql://okg@db.example/okg?sslpassword={encoded}",
        f"host=db.example port=5432 password={_keyword_quoted(password)} dbname=okg",
        f"host=db.example sslpassword={_keyword_quoted(password)} dbname=okg",
        f"connection to 'host=db password={_keyword_quoted(password)} dbname=okg' failed",
        f"export PGPASSWORD={_keyword_quoted(password)}",
        f"PGPASSWORD={password}",
        f"OKG_PGPASSWORD={password}",
        f"DB_PASSWORD: {password}",
    ]
    if not _is_disclosed_port_limit(password):
        # Raw, unencoded: what a failed-connection paste holds.
        lines.append(f"postgresql://okg:{password}@db.example:5432/okg")
        lines.append(
            f"okg install --postgres-dsn postgresql://okg:{password}@db/okg --yes"
        )
    # Raw, with any `&`: the mask runs to the end of the line or the quote.
    lines.append(f"postgresql://okg@db.example/okg?password={password}")
    lines.append(f"postgresql://okg@db/okg?sslmode=require&sslpassword={password}")
    if "'" not in password:
        lines.append(
            f"connection to 'postgresql://okg@db/okg?password={password}' failed"
        )
    if not any(c.isspace() for c in password):
        lines.extend(
            [
                f"https://okg:{password}@example.invalid/x",
                f"PGPASSWORD={password} pg_dump",
            ]
        )
        if password[0] not in "'\"":
            lines.append(f"host=db.example password={password} dbname=okg")
    return lines


def _pieces(password: str) -> list[str]:
    """The password's words of six or more characters.

    The review of PR 2309 found partial leaks (`PGPASSWORD=*** secret pw`),
    which a whole-password check cannot see.
    """
    return [piece for piece in re.split(r"[\s@/]+", password) if len(piece) >= 6]


_PORT_LIMIT = re.compile(r"\d{1,5}[/,?#][^\s@]*\s")


def _is_disclosed_port_limit(password: str) -> bool:
    """The disclosed limit: `u:12/secret x@h` reads as host `u`, port `12`.

    One to five digits, then `/ , ? #`, then a raw blank before any `@`: the
    masker keeps it as a port, as libpq reads it. Excluded from the raw-URL
    properties; `test_known_limit_digits_slash_blank_password_reads_as_a_port`
    pins it.
    """
    return _PORT_LIMIT.match(password) is not None


def _leaks(password: str, output: str) -> bool:
    if password in output or quote(password, safe="") in output:
        return True
    return any(piece in output for piece in _pieces(password))


def _templates_without(password: str) -> str:
    """The template text with the password taken out, to rule out collisions."""
    encoded = quote(password, safe="")
    quoted = _keyword_quoted(password)
    return "\n".join(
        line.replace(quoted, "").replace(encoded, "").replace(password, "")
        for line in _templates(password)
    )


@SETTINGS
@given(password=RAW_PASSWORDS, before=NOISE, after=NOISE)
def test_redact_text_never_leaks_a_password_and_keeps_the_other_lines(
    password: str, before: list[str], after: list[str]
) -> None:
    # Only judge passwords that do not also appear in the text around them.
    assume(not any(_leaks(password, noise) for noise in (*before, *after)))
    assume(not _leaks(password, _templates_without(password)))
    for line in _templates(password):
        masked_line = dsn.redact_text(line)
        text = "\n".join([*before, line, *after])
        output = dsn.redact_text(text)
        assert not _leaks(password, output), (line, output)
        assert output.split("\n") == [*before, masked_line, *after]


@SETTINGS
@given(password=RAW_PASSWORDS)
def test_redact_one_dsn_never_leaks(password: str) -> None:
    assume(not _leaks(password, _templates_without(password)))
    for line in _templates(password):
        assert not _leaks(password, dsn.redact(line)), line


_ARGV_SKELETON = (
    "okg --dsn --postgres-dsn= <redacted-dsn> psql env pg_dump "
    "postgresql://okg:@db/okg host=db password= dbname=okg PGPASSWORD="
)


@SETTINGS
@given(password=NO_BLANK_PASSWORDS)
def test_redact_argv_never_leaks(password: str) -> None:
    # A password word that is part of the command itself is not a leak
    # (`0@stgres` has the word `stgres`, which `--postgres-dsn` contains).
    assume(not any(piece in _ARGV_SKELETON for piece in _pieces(password)))
    url = f"postgresql://okg:{password}@db/okg"
    keyword = f"host=db password={_keyword_quoted(password)} dbname=okg"
    for argv in (
        ["okg", "--dsn", url],
        ["okg", f"--postgres-dsn={keyword}"],
        ["psql", keyword],
        ["psql", f"postgresql://okg:{quote(password, safe='')}@db/okg"],
        ["env", f"PGPASSWORD={password}", "pg_dump"],
    ):
        assert not any(_leaks(password, token) for token in dsn.redact_argv(argv)), argv


@SETTINGS
@given(text=st.text(max_size=400))
def test_maskers_are_total_on_any_text(text: str) -> None:
    """Never raise, and never touch text without a secret-bearing marker."""
    for masker in (dsn.redact, dsn.redact_text):
        out = masker(text)
        assert isinstance(out, str)
    dsn.redact_argv(text.split(" "))
    dsn.placeholders(text)
    dsn.expand(text, {})
    dsn.form_of(text)


@SETTINGS
@given(
    password=PASSWORDS,
    user=st.text(_ALPHABET, min_size=1, max_size=12),
    dbname=st.text(_ALPHABET, min_size=1, max_size=12),
    host=st.sampled_from(
        ["db.example", "10.0.0.5", "::1", "/tmp", "/var/run/postgresql"]
    ),
    port=st.one_of(st.none(), st.integers(min_value=1, max_value=65535)),
    options=st.one_of(
        st.none(), st.sampled_from(["-c search_path=okg", "-c a=b -c c=d"])
    ),
)
def test_parse_render_round_trip(
    password: str,
    user: str,
    dbname: str,
    host: str,
    port: int | None,
    options: str | None,
) -> None:
    assume(password not in f"{user} {dbname} {host} {port} {options} prefer postgresql")
    built = Dsn.build(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        options=options,
        sslmode="prefer",
    )
    url = built.render()
    assert dsn.parse(url) == built
    assert dsn.parse(url).to_url() == url
    assert password not in repr(built)
    keyword = Dsn(form="keyword", scheme=None, params=dict(built.params))
    assert dsn.parse(keyword.render()) == keyword
    assert dict(dsn.parse(keyword.to_url()).params) == dict(built.params)


@SETTINGS
@given(
    password=PASSWORDS,
    name=st.from_regex(r"[A-Z_][A-Z0-9_]{0,15}", fullmatch=True),
)
def test_expand_substitutes_exactly_and_never_reports_the_value(
    password: str, name: str
) -> None:
    template = f"postgresql://okg:${{{name}}}@db/okg"
    result = dsn.expand(template, {name: password})
    assert result.value == f"postgresql://okg:{password}@db/okg"
    assert password not in repr(result)
    missing = dsn.expand(template, {})
    assert missing.unset == (name,)
    assert missing.value is None


def _shown_pieces(shown: str) -> list[str]:
    return [shown, *(p for p in re.split(r"[\s@/]+", shown) if len(p) >= 6)]


@SETTINGS
@given(password=RAW_PASSWORDS)
def test_query_password_inside_json_or_repr_never_leaks(password: str) -> None:
    """Re-review of PR 2309, N2: an escaped quote inside a quoted query
    password is not its closing quote. Checks the password as it is written
    inside the JSON or repr text, through all four maskers."""
    prefix = "postgresql://okg@db.example/okg?password="
    url = prefix + password
    rendered = repr(url)
    as_json = json.dumps({"dsn": url, "x": 1}, ensure_ascii=False)
    cases = [
        (as_json, json.dumps(password, ensure_ascii=False)[1:-1]),
        (rendered, rendered[len(rendered[0] + prefix) : -1]),
    ]
    for line, shown in cases:
        pieces = _shown_pieces(shown)
        fixed = line.replace(shown, "")
        assume(not any(piece in fixed for piece in pieces))
        outputs = [
            dsn.redact_text(line),
            dsn.redact(line),
            repr(dsn.redact_value({"k": [line]})),
            " ".join(dsn.redact_argv([line])),
        ]
        for output in outputs:
            assert not any(piece in output for piece in pieces), (line, output)


def _malformed_templates(password: str) -> list[str]:
    """DSN-shaped strings holding the password that may or may not parse."""
    encoded = quote(password, safe="")
    return [
        f"postgresql//okg:{encoded}@db.internal/okg",
        f"postgresql:/okg:{password}@db.internal/okg",
        f"okg:{password}@db.internal/okg",
        f"mysql://okg:{password}@db.internal/okg",
        f" postgresql://okg:{encoded}@db.internal/okg",
        f"postgresql://okg:{password}@db.internal/okg",
        f"postgresql://okg:{encoded}@db.internal/okg?bogus=1&password={password}",
        f"host=db.internal password={password} dbname=okg",
        f"host=db.internal password={_keyword_quoted(password)} bogus=1",
        f"host=db.internal password='{password}",
        # Review of PR 2425 (final), S3: a double quote is literal to libpq,
        # and a URL with a raw `?` and no `@` starts its query early.
        f'host=db.internal password="{password} two=2" dbname=okg',
        f"postgresql://okg:{password}?{password}=",
    ]


_MODULE_TEXT = inspect.getsource(dsn)


@SETTINGS
@given(password=RAW_PASSWORDS)
def test_dsn_errors_never_contain_the_password(password: str) -> None:
    """Review of PR 2425: for any password, no DsnError message or data holds it."""
    # A password word that the module's own message text contains is not a leak.
    assume(not any(piece in _MODULE_TEXT for piece in _pieces(password)))
    assume(password not in _MODULE_TEXT)
    for value in _malformed_templates(password):
        assume(
            not any(piece in value.replace(password, "") for piece in _pieces(password))
        )
        for call in (dsn.parse, lambda v: dsn.require_url_form(v, source="OKG_DSN")):
            try:
                call(value)
            except dsn.DsnError as error:
                shown = " ".join(
                    [
                        str(error),
                        repr(error),
                        json.dumps(error.data, default=str),
                        str(error.fix_hint),
                    ]
                )
                assert not _leaks(password, shown), (value, shown)


# Review of PR 2426, B1: shapes the line-by-line masker used to show. No `=`
# in these words: a wrapped piece shaped like `word=` is a libpq key, which
# ends the password for libpq and for the old masker alike.
_PLAIN = st.text(
    st.sampled_from(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._~!$%^*+-"
    ),
    min_size=6,
    max_size=14,
)
_WRAP_SKELETON = (
    "conn=postgresql://u: @h/db failed password= dbname=x host=h ' psql -c select 1 "
    "postgresql://u:12/ @h/db"
)


def _all_maskers(text: str) -> list[str]:
    return [
        dsn.redact_text(text),
        dsn.redact(text),
        dsn.redact_value({"k": [text]})["k"][0],
    ]


@SETTINGS
@given(
    password=st.one_of(
        RAW_PASSWORDS,
        st.builds(lambda a, b: a + "\n" + b, PASSWORDS, PASSWORDS),
    )
)
def test_shlex_joined_keyword_password_never_leaks(password: str) -> None:
    """A logged command built with `shlex.join`, with blanks, `/`, quotes and
    line breaks in the quoted password."""
    for argv in (
        [
            "psql",
            f"host=h password={_keyword_quoted(password)} dbname=x",
            "-c",
            "select 1",
        ],
        ["pg_dump", "-d", f"host=h dbname=x password={_keyword_quoted(password)}"],
    ):
        joined = shlex.join(argv)
        assume(
            not any(
                piece in joined.replace(password, "") for piece in _pieces(password)
            )
        )
        for masked in _all_maskers(joined):
            assert not _leaks(password, masked), masked


@SETTINGS
@given(first=_PLAIN, second=_PLAIN, digits=st.from_regex(r"[0-9]{1,5}", fullmatch=True))
def test_a_password_split_by_a_line_break_or_a_blank_never_leaks(
    first: str, second: str, digits: str
) -> None:
    assume(first not in _WRAP_SKELETON and second not in _WRAP_SKELETON)
    templates = [
        f"host=h password={_keyword_quoted(first + chr(10) + second)} dbname=x",
        f"conn=postgresql://u:{first}\n{second}@h/db failed",
        f"password={first}\n{second} dbname=x",
        f"postgresql://u:{digits}/{first} {second}@h/db",
    ]
    for text in templates:
        for masked in _all_maskers(text):
            assert first not in masked, (text, masked)
            assert second not in masked, (text, masked)


# Lines after a wrapped password. Apostrophes are allowed, so a later
# `don't` cannot end the mask of an unclosed quote (review of PR 2425, S1).
_LATER_LINES = st.lists(
    st.text(
        st.characters(
            whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters=" .,:()'"
        ),
        max_size=40,
    ),
    min_size=1,
    max_size=4,
)


@SETTINGS
@given(
    first=_PLAIN,
    second=_PLAIN,
    tail=st.sampled_from(["", " dbname=okg", " dbname=okg user=u", " and more words"]),
    later=_LATER_LINES,
    opener=st.sampled_from(["", "'", "'", "'\"'\"'"]),
    closer=st.sampled_from(["", "'"]),
)
def test_a_wrapped_password_hides_every_fragment_and_keeps_the_later_lines(
    first: str, second: str, tail: str, later: list[str], opener: str, closer: str
) -> None:
    """A wrapped password, unquoted or in a quote that is not closed on its
    own line, masks at most the next line; every line after that one is
    kept byte for byte (#2285 item 1), even when it holds an apostrophe."""
    assume(first not in _WRAP_SKELETON and second not in _WRAP_SKELETON)
    assume(not any(first in line or second in line for line in later))
    closing = closer if opener == "'" else ""
    head = [f"host=h password={opener}{first}", f"{second}{closing}{tail}"]
    for masked in (
        dsn.redact_text("\n".join(head + later)),
        dsn.redact_value(["\n".join(head + later)])[0],
    ):
        assert first not in masked and second not in masked, masked
        assert masked.split("\n")[2:] == later, masked
