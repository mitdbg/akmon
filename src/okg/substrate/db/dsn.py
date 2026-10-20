"""One place to parse, expand, rewrite and redact PostgreSQL connection strings.

A PostgreSQL connection string (a DSN) comes in two spellings that libpq
treats as the same thing:

* the URL form, ``postgresql://user:password@host:port/dbname?key=value``;
* the keyword form, ``host=... port=... dbname=... password=...``.

This system refuses the keyword form wherever an operator sets a DSN, because
Chronos picks its storage backend by URL scheme (issue #1971). Before this
module, several pieces of code parsed DSNs, expanded ``${VAR}`` placeholders,
masked passwords and rewrote parameters, and they disagreed (issues #1980,
#2230, #2040 and #2285). This module is the single implementation. The guard
``tests/substrate/test_no_private_dsn_handling.py`` refuses any new private
copy under ``src/okg``.

Import cost: at load time this module imports only the standard library and
:mod:`okg.substrate.errors`. :func:`parse` and the keyword-form renderer
import ``psycopg`` lazily, so the maskers and the placeholder helpers stay
cheap enough for an installer's ``--help`` path.

The rules, one sentence each:

* libpq is the ground truth for what a DSN means. :func:`parse` wraps
  ``conninfo_to_dict`` and never second-guesses it.
* A rewrite keeps the input's form. :meth:`Dsn.to_url` is the only
  conversion, and it is explicit.
* ``${NAME}`` expands anywhere; a value that is exactly ``$NAME`` expands as a
  whole; a bare ``$word`` inside a longer string is literal; shell operators
  such as ``${NAME:-default}`` are refused by name.
* The maskers over-mask rather than under-mask: never show a password
  shape the old maskers hid, even at the cost of unrelated text. They keep
  the lines after a secret unless the password may continue onto them
  (:func:`redact` treats its whole input as one DSN), never raise on a
  string, and run in linear time.
* No error raised here repeats a password, and no ``repr`` shows one.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import quote

from okg.substrate.errors import SubstrateError

Form = Literal["url", "keyword"]
Lookup = Mapping[str, str] | Callable[[str], str | None]

#: The marker that replaces a secret inside a DSN. Operator-facing refusals
#: say "The *** stands for your real password", so it is part of the contract.
MASK = "***"

#: The marker that replaces the whole value of a ``--dsn``-style flag.
REDACTED_DSN = "<redacted-dsn>"

#: libpq keys whose value is a secret or points at one. ``passfile`` is a
#: path, masked by the same rule for simplicity; ``oauth_client_secret`` and
#: the ``scram_*_key`` keys exist from libpq 18.
SECRET_KEYS: frozenset[str] = frozenset(
    {
        "password",
        "sslpassword",
        "passfile",
        "oauth_client_secret",
        "scram_client_key",
        "scram_server_key",
    }
)

#: Flags whose whole value :func:`redact_argv` and :func:`redact_text` replace.
DEFAULT_DSN_FLAGS: tuple[str, ...] = ("--dsn", "--postgres-dsn")

#: libpq keyword/value connection parameters. A scheme-less string carrying
#: any of them is a DSN in keyword form. The same set as
#: ``okg.substrate.dsn_form.LIBPQ_KEYWORD_PARAM``, which becomes a re-export
#: of this one when its callers move here.
LIBPQ_KEYWORD_PARAM = re.compile(
    r"(?:^|\s)(?:host|hostaddr|port|dbname|user|password|sslmode|service"
    r"|passfile|connect_timeout|application_name|options)\s*=",
)

_URL_SCHEMES = ("postgresql://", "postgres://")
_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
_NAME_RE = re.compile(rf"^{_NAME}$")

# libpq connection parameter -> the environment variable a libpq child
# (pg_dump, psql, pg_restore) reads instead. A parameter without an entry has
# no environment spelling; to_libpq_env refuses it rather than drop it.
_LIBPQ_ENV: Mapping[str, str] = MappingProxyType(
    {
        "host": "PGHOST",
        "hostaddr": "PGHOSTADDR",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "passfile": "PGPASSFILE",
        "dbname": "PGDATABASE",
        "options": "PGOPTIONS",
        "application_name": "PGAPPNAME",
        "connect_timeout": "PGCONNECT_TIMEOUT",
        "client_encoding": "PGCLIENTENCODING",
        "service": "PGSERVICE",
        "target_session_attrs": "PGTARGETSESSIONATTRS",
        "load_balance_hosts": "PGLOADBALANCEHOSTS",
        "sslmode": "PGSSLMODE",
        "sslnegotiation": "PGSSLNEGOTIATION",
        "sslcompression": "PGSSLCOMPRESSION",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
        "sslcertmode": "PGSSLCERTMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcrl": "PGSSLCRL",
        "sslcrldir": "PGSSLCRLDIR",
        "sslsni": "PGSSLSNI",
        "requirepeer": "PGREQUIREPEER",
        "require_auth": "PGREQUIREAUTH",
        "channel_binding": "PGCHANNELBINDING",
        "ssl_min_protocol_version": "PGSSLMINPROTOCOLVERSION",
        "ssl_max_protocol_version": "PGSSLMAXPROTOCOLVERSION",
        "gssencmode": "PGGSSENCMODE",
        "krbsrvname": "PGKRBSRVNAME",
        "gsslib": "PGGSSLIB",
        "gssdelegation": "PGGSSDELEGATION",
        # libpq 18.
        "min_protocol_version": "PGMINPROTOCOLVERSION",
        "max_protocol_version": "PGMAXPROTOCOLVERSION",
    }
)

# URL parts that live in the authority or the path, not the query string.
_URL_AUTHORITY_KEYS = frozenset({"user", "password", "host", "port", "dbname"})
_URL_FIX_HINT = "write the DSN as postgresql://user:password@host:port/dbname"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class DsnError(SubstrateError):
    """Base class for this module's errors. None of them carries a secret."""


class DsnParseError(DsnError):
    """libpq could not read the string, or it is not a PostgreSQL DSN."""

    code = "dsn_parse_error"


class DsnFormError(DsnError):
    """A keyword-form DSN reached a place that requires the URL form."""

    code = "dsn_keyword_form"


class DsnRenderError(DsnError):
    """A parsed DSN cannot be written in the requested shape faithfully."""

    code = "dsn_render_refused"


class DsnPlaceholderError(DsnError):
    """A placeholder in a DSN template could not be expanded."""


class DsnPlaceholderUnset(DsnPlaceholderError):
    """A ``${NAME}`` refers to a variable that is not set."""

    code = "dsn_placeholder_unset"


class DsnPlaceholderEmpty(DsnPlaceholderError):
    """A ``${NAME}`` refers to a variable that is set to the empty string."""

    code = "dsn_placeholder_empty"


class DsnPlaceholderUnsupported(DsnPlaceholderError):
    """Shell syntax such as ``${NAME:-default}`` or ``$(cmd)``, refused by name."""

    code = "dsn_placeholder_unsupported"


# ---------------------------------------------------------------------------
# Form detection
# ---------------------------------------------------------------------------


def form_of(dsn: str) -> Form | None:
    """``"url"``, ``"keyword"``, or ``None`` when ``dsn`` is neither.

    Cheap and import-free: no libpq call. It follows libpq's own test: the
    URL form starts with exactly ``postgresql://`` or ``postgres://`` --
    lowercase, no leading blank, or libpq reads the string as keyword pairs.
    Any other string carrying a libpq keyword parameter is the keyword form,
    even when a value holds ``://`` (``application_name=http://x``, or a
    quoted password). Anything else -- another URL scheme, a file path, a
    placeholder, an empty string -- is ``None``.
    """
    if dsn.startswith(_URL_SCHEMES):
        return "url"
    if LIBPQ_KEYWORD_PARAM.search(dsn):
        return "keyword"
    return None


# A URL scheme: what `urlsplit` would accept, bounded. Shown in a refusal
# because it names the form problem and holds no credential.
_SCHEME_PREFIX = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]{0,31})://")


def _unrecognized_form(dsn: str) -> str:
    """Why ``dsn`` is neither form, without repeating it.

    A string that is neither a ``postgresql://`` URL nor libpq keyword pairs
    is a string no parser tokenized, so the maskers cannot locate its secrets
    (``postgresql//u:secret@h/db`` has no ``://`` for the userinfo rule). Show
    at most its scheme.
    """
    if not dsn.strip():
        return "the value is empty"
    match = _SCHEME_PREFIX.match(dsn)
    if match is not None:
        return (
            f"the value starts with the scheme {match.group(1)}://, "
            "not postgresql:// or postgres://"
        )
    return "the value is not shown, because it may hold a password"


def _keyword_form_refusal(source: str, dsn: str) -> str:
    """The keyword-form refusal text with every secret masked.

    Same wording as ``okg.substrate.dsn_form.keyword_form_refusal`` so the
    operator meets one message wherever the refusal fires. The URL offered is
    checked by parsing it back before it is shown.
    """
    suggestion: str | None = None
    parsed = try_parse(dsn)
    # A string libpq cannot read is not shown at all: the masker stops an
    # unquoted password at the next ` key=` token, and in a string libpq
    # refuses, that "key" can be the second word of the password.
    shown = (
        f"({redact(dsn)!r})"
        if parsed is not None
        else "(not shown: libpq cannot parse it, so its secrets cannot be located)"
    )
    if parsed is not None and parsed.dbname:
        try:
            suggestion = redact(parsed.to_url())
        except DsnError:
            suggestion = None
    instead = (
        f"Set it to {suggestion} instead. The *** stands for your real password."
        if suggestion
        else "Set it in URL form: postgresql://user:password@host:port/dbname."
    )
    return (
        f"{source} is a libpq keyword/value connection string "
        f"{shown}, "
        "which Chronos routes to SQLite because it has no URL scheme -- the "
        "publish path would write a FILE named after this string instead of "
        f"using the database. {instead}"
    )


def require_url_form(dsn: str, *, source: str) -> str:
    """Return ``dsn`` unchanged when it is a ``postgresql://`` URL; refuse otherwise.

    ``source`` names what the operator edited (an environment variable, a
    manifest key, a flag). A keyword-form DSN raises :class:`DsnFormError`
    (``dsn_keyword_form``); anything else that is not a PostgreSQL URL raises
    :class:`DsnParseError`. Expand placeholders first (:func:`require_expanded`).
    """
    kind = form_of(dsn)
    if kind == "url":
        return dsn
    if kind == "keyword":
        raise DsnFormError(
            _keyword_form_refusal(source, dsn),
            data={"source": source},
            fix_hint=_URL_FIX_HINT,
        )
    raise DsnParseError(
        f"{source} is not a PostgreSQL connection string "
        f"({_unrecognized_form(dsn)}); "
        "expected postgresql://user:password@host:port/dbname.",
        data={"source": source},
        fix_hint=_URL_FIX_HINT,
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_EQUALS_WITH_BLANKS = re.compile(r"[ \t]*=[ \t]*")
# libpq connection keywords, through libpq 18, plus a few older ones. A parse
# error names an unknown option only when it is one of these.
_KNOWN_LIBPQ_KEYWORDS: frozenset[str] = frozenset(
    {
        *_LIBPQ_ENV,
        *SECRET_KEYS,
        "fallback_application_name",
        "keepalives",
        "keepalives_count",
        "keepalives_idle",
        "keepalives_interval",
        "oauth_client_id",
        "oauth_issuer",
        "oauth_scope",
        "replication",
        "requiressl",
        "sslkeylogfile",
        "tcp_user_timeout",
        "tty",
    }
)
# Quoted tokens libpq writes itself, never taken from the input.
_LIBPQ_FIXED_TOKENS = (
    'missing "=" after ',
    'separator "=" in ',
    'matching "]" in ',
)


def _sanitized_libpq_message(
    message: str, dsn: str, form: Form
) -> tuple[str, str | None]:
    """libpq's parse message with every fragment that could be secret replaced.

    libpq quotes the token it choked on without escaping it, and for
    ``password=my secret`` that token is part of the password (``missing "="
    after "secret"``). A password holding ``"`` makes quote pairs unreliable,
    so after setting aside the quoted tokens libpq writes itself (``"="``,
    ``"]"``), everything from the FIRST remaining ``"`` to the LAST one is one
    region. It is kept only when it is an unknown key that provably sits in a
    key position; otherwise it becomes ``"<fragment>"``. Returns the message
    and the unknown key when it is safe to report.
    """
    text = " ".join(message.split())
    for index, token in enumerate(_LIBPQ_FIXED_TOKENS):
        text = text.replace(token, f"\x00{index}\x00")
    reported: str | None = None
    first = text.find('"')
    last = text.rfind('"')
    if first != -1:
        prefix = text[:first]
        region = text[first + 1 : last] if last > first else text[first + 1 :]
        suffix = text[last + 1 :] if last > first else ""
        lowered = prefix.rstrip().lower()
        if lowered.endswith(
            ("invalid connection option", "invalid uri query parameter:")
        ):
            # An unknown option is named only when it is a libpq keyword the
            # installed libpq lacks (an older client); any other name may be
            # a word of a password (`password="hunter two=2"`).
            if region in _KNOWN_LIBPQ_KEYWORDS and _is_safe_key_position(
                region, dsn, form
            ):
                shown = f'"{region}"'
                reported = region
            else:
                shown = "(name not shown)"
        else:
            shown = '"<fragment>"'
        text = prefix + shown + suffix
    for index, token in enumerate(_LIBPQ_FIXED_TOKENS):
        text = text.replace(f"\x00{index}\x00", token)
    return text, reported


def _is_safe_key_position(key: str, dsn: str, form: Form) -> bool:
    """True when ``key`` appears in ``dsn`` as a key, not inside a secret value.

    ``password=my secret=x`` makes libpq read ``secret`` as a key, but it is
    the second word of the operator's password, so it is not safe to echo.
    """
    if not _NAME_RE.match(key):
        return False
    if form == "url":
        return _is_safe_query_key(key, dsn)
    match = re.search(rf"(?:^|(?<=\s)){re.escape(key)}[ \t]*=", dsn)
    if match is None:
        return False
    preceding = _EQUALS_WITH_BLANKS.sub("=", dsn[: match.start()]).rstrip()
    if not preceding:
        return True
    last = preceding.rsplit(None, 1)[-1]
    if "=" not in last:
        return False
    last_key, last_value = last.split("=", 1)
    # Only a single quote quotes for libpq; a double quote is literal.
    if last_key.lower() in SECRET_KEYS and not last_value.startswith("'"):
        return False
    return True


def _is_safe_query_key(key: str, dsn: str) -> bool:
    """True when ``key`` is a query key that cannot be part of a password.

    Unsafe: a key before the last ``@`` (a raw ``?`` inside a userinfo
    password starts libpq's query there), and a key right after a
    ``password=``-style parameter (a raw ``&`` inside a query password splits
    it, so the "key" is the password's tail).
    """
    match = re.search(rf"[?&]{re.escape(key)}=", dsn)
    if match is None:
        return False
    if match.start() < dsn.rfind("@"):
        return False
    prefix = dsn[: match.start()]
    separator = max(prefix.rfind("?"), prefix.rfind("&"))
    if separator == -1:
        return True
    previous_key = prefix[separator + 1 :].split("=", 1)[0]
    return previous_key.lower() not in SECRET_KEYS


_NON_ASCII_SPLIT_MESSAGE = (
    "a value holds a non-ASCII character libpq split while reading it; "
    "single-quote keyword values that are not plain ASCII"
)


@lru_cache(maxsize=256)
def _parse_cached(dsn: str) -> Dsn:
    kind = form_of(dsn)
    if kind is None:
        raise DsnParseError(
            "not a PostgreSQL connection string: expected a postgresql:// URL "
            f"or libpq key=value pairs ({_unrecognized_form(dsn)})",
            data={},
            fix_hint=_URL_FIX_HINT,
        )
    import psycopg
    import psycopg.conninfo

    failure: tuple[str, str | None] | None = None
    params: dict[str, Any] = {}
    try:
        params = psycopg.conninfo.conninfo_to_dict(dsn)
    except psycopg.Error as exc:
        failure = _sanitized_libpq_message(str(exc), dsn, kind)
    except UnicodeError:
        # libpq tests blanks byte by byte with C isspace, which in some
        # locales splits a UTF-8 character (U+00A0 holds 0xA0), and psycopg
        # then fails to decode libpq's message. The message would quote the
        # split token, so none of it is kept.
        failure = (_NON_ASCII_SPLIT_MESSAGE, None)
    if failure is not None:
        # Raised after the `except` block, so neither `__cause__` nor
        # `__context__` holds libpq's error: its text quotes password
        # fragments, and a reporter that walks the chain would show them.
        message, key = failure
        data: dict[str, Any] = {"form": kind}
        if key is not None:
            data["key"] = key
        raise DsnParseError(
            f"libpq could not parse the connection string: {message}",
            data=data,
            fix_hint=(
                "single-quote keyword values that contain spaces, and "
                "percent-encode special characters in a URL password"
            ),
        ) from None
    scheme = dsn.split("://", 1)[0] if kind == "url" else None
    return Dsn(
        form=kind,
        scheme=scheme,
        params={str(k): str(v) for k, v in params.items() if v is not None},
    )


def parse(dsn: str) -> Dsn:
    """Parse ``dsn`` by libpq's own rules.

    Raises :class:`DsnParseError` (``dsn_parse_error``) when libpq refuses the
    string or it is not a PostgreSQL DSN. The message keeps libpq's reason
    but not a quoted fragment that could be part of a password, and the error
    is raised ``from None`` so no chained traceback repeats it.

    ``params["password"]`` is the decoded password. A URL's ``?password=``
    overrides its userinfo password, a repeated key keeps its last value, and
    a double-quoted keyword value keeps its quotes -- all as libpq does. The
    accepted keys are the installed libpq's (``require_auth`` needs 16+).
    Results are cached by the raw string.
    """
    return _parse_cached(dsn)


def parse_refusal(dsn: str) -> str | None:
    """Why libpq cannot parse ``dsn``, with no fragment of it; None when it can.

    For a caller about to hand ``dsn`` to ``psycopg.connect``, whose parse
    error quotes the token libpq choked on -- often part of the password
    (#2285 item 5). Unlike :func:`parse`, this asks libpq about any string,
    not only one :func:`form_of` recognizes, so it refuses exactly what
    libpq refuses and nothing it accepts. Never raises on a ``str``.
    """
    import psycopg
    import psycopg.conninfo

    failure: tuple[str, str | None] | None = None
    try:
        psycopg.conninfo.conninfo_to_dict(dsn)
    except psycopg.Error as exc:
        failure = _sanitized_libpq_message(str(exc), dsn, form_of(dsn) or "keyword")
    except UnicodeError:
        failure = (_NON_ASCII_SPLIT_MESSAGE, None)
    if failure is None:
        return None
    return f"libpq could not parse the connection string: {failure[0]}"


def try_parse(dsn: str) -> Dsn | None:
    """:func:`parse`, returning ``None`` instead of raising. For diagnostics."""
    try:
        return parse(dsn)
    except DsnError:
        return None


# ---------------------------------------------------------------------------
# The parsed DSN
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, repr=False, eq=False)
class Dsn:
    """A parsed DSN. ``repr`` and ``str`` are redacted; the password never prints."""

    form: Form
    scheme: str | None
    params: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dsn):
            return NotImplemented
        return (
            self.form == other.form
            and self.scheme == other.scheme
            and dict(self.params) == dict(other.params)
        )

    def __hash__(self) -> int:
        return hash((self.form, self.scheme, frozenset(self.params.items())))

    def __repr__(self) -> str:
        return f"Dsn({self._safe_text()!r})"

    def __str__(self) -> str:
        return self._safe_text()

    def _safe_text(self) -> str:
        if self.form == "url":
            return redact(_render_url(self.scheme or "postgresql", self.params))
        shown = {
            key: (MASK if key in SECRET_KEYS else value)
            for key, value in self.params.items()
        }
        return redact(_render_keyword(shown))

    # -- fields ---------------------------------------------------------

    @property
    def host(self) -> str | None:
        return self.params.get("host")

    @property
    def port(self) -> str | None:
        return self.params.get("port")

    @property
    def user(self) -> str | None:
        return self.params.get("user")

    @property
    def dbname(self) -> str | None:
        return self.params.get("dbname")

    @property
    def password(self) -> str | None:
        return self.params.get("password")

    @property
    def options(self) -> str | None:
        return self.params.get("options")

    @property
    def application_name(self) -> str | None:
        return self.params.get("application_name")

    def target(self) -> dict[str, str | None]:
        """Host, port, user and database name. Never the password."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "dbname": self.dbname,
        }

    # -- rendering ------------------------------------------------------

    def render(self) -> str:
        """The DSN in its own form: URL in, URL out; keyword in, keyword out."""
        if self.form == "url":
            return _render_url(self.scheme or "postgresql", self.params)
        return _render_keyword(self.params)

    def to_url(self) -> str:
        """This DSN as a ``postgresql://`` URL, checked by parsing it back.

        The only form conversion in this module. Raises
        :class:`DsnRenderError` if the URL would not mean the same connection.
        """
        scheme = self.scheme if self.form == "url" and self.scheme else "postgresql"
        url = _render_url(scheme, self.params)
        reparsed = parse(url)
        if dict(reparsed.params) != dict(self.params):
            differing = sorted(
                key
                for key in set(reparsed.params) | set(self.params)
                if reparsed.params.get(key) != self.params.get(key)
            )
            raise DsnRenderError(
                "the URL spelling of this DSN would not mean the same connection "
                f"(parameters that change: {', '.join(differing)})",
                data={"keys": differing},
            )
        return url

    # -- rewrites -------------------------------------------------------

    def with_params(
        self,
        *,
        if_absent: Iterable[str] = frozenset(),
        **changes: str | int | None,
    ) -> Dsn:
        """A copy with ``changes`` applied. The form is kept.

        ``None`` removes a key; any other value sets it, as ``str``. A key named
        in ``if_absent`` is set only when this DSN has no non-empty value for
        it: the store's ``application_name`` rule, where a DSN that names
        itself keeps its name. Without ``if_absent`` the change wins, which is
        ``connection.connect(application_name=...)``'s rule.
        """
        only_if_absent = frozenset(if_absent)
        stray = sorted(only_if_absent - set(changes))
        if stray:
            raise ValueError(
                f"if_absent names keys that are not being changed: {stray}"
            )
        params = dict(self.params)
        for key, value in changes.items():
            if value is None:
                params.pop(key, None)
            elif key in only_if_absent and params.get(key):
                continue
            else:
                params[key] = str(value)
        return Dsn(form=self.form, scheme=self.scheme, params=params)

    def with_dbname(self, name: str) -> Dsn:
        """A copy pointing at database ``name`` on the same server."""
        return self.with_params(dbname=name)

    def with_options(self, *fragments: str) -> Dsn:
        """A copy whose ``options`` has ``fragments`` appended, never replaced.

        The DSN's own ``options`` (a ``search_path`` pin, timeouts) come first,
        blank fragments are dropped, and the parts are joined by one space.
        """
        parts = [self.params.get("options", ""), *fragments]
        joined = " ".join(part.strip() for part in parts if part and part.strip())
        return self.with_params(options=joined or None)

    def without_password(self) -> Dsn:
        """A copy with no password."""
        return self.with_params(password=None)

    def to_libpq_env(self) -> dict[str, str]:
        """Environment variables that point a libpq child at the same database.

        Covers libpq's environment table, including ``options`` as
        ``PGOPTIONS`` and ``passfile`` as ``PGPASSFILE``. A parameter libpq
        cannot read from the environment (``sslpassword``, the TCP keepalive
        settings) raises :class:`DsnRenderError` naming it: dropping it would
        connect a ``pg_dump`` child differently from the parent.
        """
        missing = sorted(key for key in self.params if key not in _LIBPQ_ENV)
        if missing:
            raise DsnRenderError(
                "these connection parameters have no libpq environment variable, "
                f"so a child process would connect differently: {', '.join(missing)}",
                data={"keys": missing},
                fix_hint="drop them from the DSN, or pass the DSN to the child as an argument",
            )
        return {_LIBPQ_ENV[key]: value for key, value in self.params.items()}

    # -- construction ---------------------------------------------------

    @classmethod
    def from_connection_info(cls, info: Any, *, form: Form = "url") -> Dsn:
        """Rebuild a DSN from a live ``psycopg`` ``ConnectionInfo``.

        Uses ``info.get_parameters()`` (the non-default parameters, never the
        password) plus ``info.password``, so the result INCLUDES the password.
        Call :meth:`without_password` when it must not.
        """
        params = {
            str(key): str(value)
            for key, value in dict(info.get_parameters()).items()
            if value is not None
        }
        password = getattr(info, "password", None)
        if password:
            params["password"] = str(password)
        return cls(
            form=form, scheme="postgresql" if form == "url" else None, params=params
        )

    @classmethod
    def build(
        cls,
        *,
        host: str | None,
        dbname: str,
        port: int | str | None = None,
        user: str | None = None,
        password: str | None = None,
        scheme: str = "postgresql",
        **params: str | int | None,
    ) -> Dsn:
        """A URL-form DSN from parts. Every part is percent-encoded on render."""
        values: dict[str, str] = {}
        pairs: list[tuple[str, str | int | None]] = [
            ("user", user),
            ("password", password),
            ("host", host),
            ("port", port),
            ("dbname", dbname),
        ]
        pairs.extend(params.items())
        for key, value in pairs:
            if value is not None:
                values[key] = str(value)
        return cls(form="url", scheme=scheme, params=values)


def _is_socket_host(host: str) -> bool:
    return host.startswith(("/", "@"))


def _url_host(host: str) -> str:
    if ":" in host:
        return f"[{host}]"
    return quote(host, safe="")


def _render_url(scheme: str, params: Mapping[str, str]) -> str:
    """A URL for ``params``. Hosts go in the authority when a URL can say them.

    A unix-socket directory, or a multi-host list whose port list is neither
    empty nor one-per-host, goes in the query string (``?host=...&port=...``),
    which libpq reads back to the same values.
    """
    user = params.get("user")
    password = params.get("password")
    host = params.get("host")
    port = params.get("port")
    dbname = params.get("dbname")
    query: list[tuple[str, str]] = []

    hosts = host.split(",") if host is not None else []
    ports = port.split(",") if port is not None else []
    authority_hosts = ""
    simple = len(hosts) <= 1 and len(ports) <= 1
    zipped = len(hosts) > 1 and len(ports) in (0, len(hosts))
    if any(_is_socket_host(name) for name in hosts) or not (simple or zipped):
        if host is not None:
            query.append(("host", host))
        if port is not None:
            query.append(("port", port))
    elif zipped:
        entries = []
        for index, name in enumerate(hosts):
            entry = _url_host(name)
            if ports and ports[index]:
                entry += f":{ports[index]}"
            entries.append(entry)
        authority_hosts = ",".join(entries)
    else:
        if host is not None:
            authority_hosts = _url_host(host)
        if port is not None:
            authority_hosts += f":{port}"

    userinfo = ""
    if user is not None or password is not None:
        userinfo = quote(user or "", safe="")
        if password is not None:
            userinfo += ":" + quote(password, safe="")
        userinfo += "@"

    path = "/" + quote(dbname, safe="") if dbname is not None else ""
    query.extend(
        (key, value) for key, value in params.items() if key not in _URL_AUTHORITY_KEYS
    )
    # quote, never quote_plus: libpq does not read `+` as a space.
    query_text = "&".join(f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in query)
    url = f"{scheme}://{userinfo}{authority_hosts}{path}"
    if query_text:
        url += "?" + query_text
    return url


_KEYWORD_PLAIN_VALUE = re.compile(r"[A-Za-z0-9_.,:/@+%-]+")


def _quote_keyword(value: str) -> str:
    """A libpq keyword value, single-quoted unless it is plain ASCII.

    Not ``make_conninfo``: psycopg decides whether to quote with Python's idea
    of a blank, while libpq tests each byte with C ``isspace``, which in some
    locales matches a byte inside a UTF-8 character (U+2800 contains 0xA0).
    An unquoted value psycopg thought safe then fails to parse. Quoting
    everything that is not plain ASCII avoids the disagreement.
    """
    if _KEYWORD_PLAIN_VALUE.fullmatch(value):
        return value
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _render_keyword(params: Mapping[str, str]) -> str:
    return " ".join(f"{key}={_quote_keyword(value)}" for key, value in params.items())


# ---------------------------------------------------------------------------
# Placeholders (#1980)
# ---------------------------------------------------------------------------

# Every `${...}` (terminated or not) and `$(...)`; _scan classifies each.
_BRACE_OR_SUBSHELL = re.compile(r"\$\{[^}\r\n]*\}?|\$\([^)\r\n]*\)?")
_WHOLE_VALUE_NAME = re.compile(rf"^\s*\$({_NAME})\s*$")
_BRACED_NAME = re.compile(rf"^\$\{{({_NAME})\}}$")
# `${NAME:-word}` and the other shell operators; only NAME and the operator
# are ever reported, never the word after the operator.
_BRACED_OPERATOR = re.compile(rf"^\$\{{({_NAME})(:?[-=?+])")


def _describe_unsupported(text: str) -> str:
    """A safe name for unsupported placeholder syntax: its shape and variable.

    Never the text itself: in ``${PW:-hunter2}`` the default is a password,
    and an unterminated ``${`` inside a password runs to the end of the
    line. Only a variable name that sits right after ``${`` and before a
    shell operator is kept.
    """
    if text.startswith("$("):
        return "$(...)"
    if not text.endswith("}"):
        return "${... (unterminated)"
    operator = _BRACED_OPERATOR.match(text)
    if operator:
        return f"${{{operator.group(1)}{operator.group(2)}...}}"
    return "${...}"


@dataclass(frozen=True)
class Expansion:
    """The result of :func:`expand`.

    ``value`` is the expanded string only when every placeholder resolved to a
    non-empty value and nothing unsupported was found; otherwise ``None``.
    ``value`` is left out of ``repr`` because it usually holds a password.
    ``unsupported`` holds safe descriptions (``${PW:-...}``, ``$(...)``), never
    the template text, which can hold a password or a default.
    """

    value: str | None = field(repr=False)
    resolved: tuple[str, ...] = ()
    unset: tuple[str, ...] = ()
    empty: tuple[str, ...] = ()
    unsupported: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.value is not None


def _scan(template: str) -> tuple[list[tuple[int, int, str]], list[str]]:
    """Placeholder spans ``(start, end, name)`` and unsupported texts, in order."""
    whole = _WHOLE_VALUE_NAME.match(template)
    if whole:
        return [(whole.start(1) - 1, whole.end(1), whole.group(1))], []
    spans: list[tuple[int, int, str]] = []
    unsupported: list[str] = []
    for match in _BRACE_OR_SUBSHELL.finditer(template):
        braced = _BRACED_NAME.match(match.group(0))
        if braced:
            spans.append((match.start(), match.end(), braced.group(1)))
        else:
            unsupported.append(_describe_unsupported(match.group(0)))
    return spans, unsupported


def placeholders(template: str) -> list[str]:
    """The variable names ``template`` refers to, sorted and unique.

    ``${NAME}`` anywhere, or a value that is exactly ``$NAME``. Unsupported
    syntax names no variable and is reported by :func:`expand` instead.
    """
    spans, _ = _scan(template)
    return sorted({name for _, _, name in spans})


def expand(template: str, lookup: Lookup | None = None) -> Expansion:
    """Expand ``template``'s placeholders from ``lookup`` (default ``os.environ``).

    ``lookup`` is a mapping or a callable returning ``None`` for unset. It
    owns precedence: a caller with several sources (process environment,
    ``.okg/local.env``) merges them and passes the result. A variable set to
    ``""`` is reported in ``empty``, apart from ``unset``. ``${X:-d}``,
    ``${X-d}``, ``${X:=d}``, ``${X:?m}``, ``${ X }``, ``${1X}``, an
    unterminated ``${`` and ``$(cmd)`` are reported in ``unsupported`` by
    shape and variable name only (``${X:-...}``, ``${...}``, ``${... (unterminated)``,
    ``$(...)``): a default would silently pick a different database, and the
    text after an operator can be a password. A bare
    ``$word`` inside a longer string is literal. Values are not rescanned.
    """
    source: Lookup = os.environ if lookup is None else lookup
    get = source.get if isinstance(source, Mapping) else source
    spans, unsupported = _scan(template)
    resolved: list[str] = []
    unset: list[str] = []
    empty: list[str] = []
    values: dict[str, str] = {}
    for name in dict.fromkeys(name for _, _, name in spans):
        value = get(name)
        if value is None:
            unset.append(name)
        elif value == "":
            empty.append(name)
        else:
            resolved.append(name)
            values[name] = value
    expanded: str | None = None
    if not unset and not empty and not unsupported:
        pieces: list[str] = []
        cursor = 0
        for start, end, name in spans:
            pieces.append(template[cursor:start])
            pieces.append(values[name])
            cursor = end
        pieces.append(template[cursor:])
        expanded = "".join(pieces)
        if _WHOLE_VALUE_NAME.match(template):
            expanded = expanded.strip()
    return Expansion(
        value=expanded,
        resolved=tuple(resolved),
        unset=tuple(unset),
        empty=tuple(empty),
        unsupported=tuple(dict.fromkeys(unsupported)),
    )


def require_expanded(
    template: str, *, source: str, lookup: Lookup | None = None
) -> str:
    """:func:`expand`, raising a named error unless every placeholder resolved.

    Unsupported syntax is reported first (:class:`DsnPlaceholderUnsupported`),
    then unset variables (:class:`DsnPlaceholderUnset`), then variables set to
    the empty string (:class:`DsnPlaceholderEmpty`). The error names
    ``source``, the variables and the shape of unsupported syntax, and never
    a value, a default or any other template text.
    """
    result = expand(template, lookup)
    if result.value is not None:
        return result.value
    shown_unsupported = list(result.unsupported)
    data: dict[str, Any] = {
        "source": source,
        "unset": list(result.unset),
        "empty": list(result.empty),
        "unsupported": shown_unsupported,
    }
    literal_hint = (
        " A literal ${WORD} inside a password is read as a placeholder;"
        " percent-encode it as %24%7BWORD%7D to keep it."
    )
    if result.unsupported:
        raise DsnPlaceholderUnsupported(
            f"{source} uses placeholder syntax this system does not expand: "
            f"{', '.join(shown_unsupported)}. Only ${{NAME}}, or a value that is "
            "exactly $NAME, expands. Defaults and commands are refused because "
            "they would silently choose a different database." + literal_hint,
            data=data,
            fix_hint="write ${NAME} and export NAME, or write the DSN literally",
        )
    if result.unset:
        raise DsnPlaceholderUnset(
            f"{source} refers to {', '.join(result.unset)}, which is not set."
            + literal_hint,
            data=data,
            fix_hint=f"export {result.unset[0]}=postgresql://user:password@host:port/dbname",
        )
    raise DsnPlaceholderEmpty(
        f"{source} refers to {', '.join(result.empty)}, which is set but empty.",
        data=data,
        fix_hint=f"give {result.empty[0]} a non-empty value",
    )


# ---------------------------------------------------------------------------
# Redaction (#2230, #2285)
# ---------------------------------------------------------------------------

_SECRET_ALTERNATION = "|".join(sorted(SECRET_KEYS, key=len, reverse=True))
# `password = value` as a libpq keyword; also `--password=` and `db_password=`
# (over-masking is the rule). Not after a letter, digit, `?` or `&`: the query
# rule owns `?password=` and the environment rule owns `PGPASSWORD=`.
_KEYWORD_SECRET = re.compile(
    rf"(?<![A-Za-z0-9?&])(?:{_SECRET_ALTERNATION})[ \t]*+=[ \t]*+", re.IGNORECASE
)
# A query password runs to the end of the line, or to the closing quote when
# the DSN token opened with a quote: a raw blank, `#` or `&` inside it
# (libpq splits or refuses on each, so they turn up in error pastes) must not
# end the mask. Over-masks the parameters after it.
_QUERY_SECRET = re.compile(rf"[?&](?:{_SECRET_ALTERNATION})=", re.IGNORECASE | re.ASCII)
_QUOTE = re.compile(r"['\"]")
# An environment-style assignment: `PGPASSWORD=`, `PGSSLPASSWORD=` (any
# case), or an upper-case variable name ending in PASSWORD or PASSWD
# (`OKG_PGPASSWORD=`, `DBPASSWORD=`, `POSTGRES_PASSWORD:`). Masked to the end
# of the line: `env` and `printenv` never quote, so a password with a blank
# would otherwise show its second word.
_ENV_SECRET = re.compile(
    r"(?<![A-Za-z0-9_])"
    r"(?:(?i:PG(?:SSL)?PASSWORD)|[A-Z0-9_]*+(?:(?<=PASSWORD)|(?<=PASSWD)))"
    r"[\"']?[ \t]*+[=:][ \t]*+"
)
# The start of the next ` key=` token after an unquoted keyword value. The
# lookbehind starts each attempt at the beginning of a blank run and the
# possessive quantifiers never backtrack, so one search is linear.
_NEXT_KEY = re.compile(r"(?<![ \t])[ \t]++[A-Za-z_][A-Za-z0-9_]*+[ \t]*+=")
# ASCII blanks only: a Unicode space is not a boundary to a shell or to libpq,
# so treating it as one would show the rest of a password.
_BLANK = re.compile(r"\s", re.ASCII)
# Any scheme, or none: the scheme is not read, only the `://` that ends it,
# so `${X:-postgresql://u:pw@h}` and `-https://u:t@h` are masked as well.
_URL_USER = re.compile(r"://[^:/?#@\[\]\s]*+:", re.ASCII)
# A user holding a blank, a bracket, `?` or `#` (`my user:pw@h`, `[u:pw@h`),
# which _URL_USER does not read as one; the class is dev's old user class.
# Only used when an `@` follows (on the line, or on the next line for a
# wrapped password), and never for an IPv6 host (_IPV6_HOST).
_URL_LOOSE_USER = re.compile(r"://[^:/@\r\n]*+:")
# A bracketed IPv6 literal (hex, colons and dots, an optional zone), which is
# a host, not a user: `postgresql://[::1]:5432/db`. Bounded in length (an
# address is at most 45 characters) so each check is constant time; the
# two-colon test is in _loose_users.
_IPV6_HOST = re.compile(
    r"\[([0-9A-Fa-f:.]{2,45}+)(?:%[\w.~-]{1,64}+)?\]"
    # A host ends the authority: then a port, a path, a query or the end.
    # Anything else (`[::1]:pw@h`) makes the bracket part of a user.
    r"(?=(?::\d{1,5}+)?(?:[/?#,\s]|$))",
    re.ASCII,
)
# A port: one to five digits ended by `/ , ? #` or the end of the line. A
# blank does not end a port, so `u:2024 secret@h` stays a password.
_PORT_AFTER_COLON = re.compile(r"\d{1,5}+(?:[/,?#]|$)")
_LINE_BREAK = re.compile(r"\r\n|\r|\n")
_LINE_END = re.compile(r"[\r\n]")
_NON_BLANK = re.compile(r"\S", re.ASCII)

Span = tuple[int, int, str]


def _flag_pattern(flags: Sequence[str]) -> re.Pattern[str] | None:
    names = sorted((flag for flag in flags if flag), key=len, reverse=True)
    if not names:
        return None
    alternation = "|".join(re.escape(flag) for flag in names)
    return re.compile(rf"(?<![A-Za-z0-9_\-])(?:{alternation})[= \t]++")


_DEFAULT_FLAG_RE = _flag_pattern(DEFAULT_DSN_FLAGS)


# How a shell writes a single quote inside a single-quoted word.
_SHELL_QUOTE_SPLICES = ("'\"'\"'", "'\\''")


def _value_end(text: str, start: int, *, stop_at_next_key: bool) -> int:
    """Where a secret value that starts at ``start`` ends.

    A quoted value (single or double quote, backslash escapes) runs to its
    closing quote and then through any non-blank characters glued to it, so
    ``'ab'cd`` masks ``cd`` too. An unterminated quote runs to the end. An
    unquoted value runs to the next `` key=`` token (``stop_at_next_key``) or
    to the next blank, else to the end.
    """
    end = len(text)
    if start >= end:
        return start
    for splice in _SHELL_QUOTE_SPLICES:
        if text.startswith(splice, start):
            # `shlex.join` writes a single quote inside a single-quoted word
            # as '"'"' (a POSIX shell writes '\''), so the libpq-level quoted
            # value runs to the next splice, not to the next bare quote.
            closing = _closing_quote(text, splice, start + len(splice))
            if closing == -1:
                return end
            blank = _BLANK.search(text, closing + len(splice))
            return blank.start() if blank else end
    opener = text[start]
    if opener in "'\"":
        index = start + 1
        while index < end:
            char = text[index]
            if char == "\\":
                index += 2
                continue
            if char == opener:
                blank = _BLANK.search(text, index + 1)
                return blank.start() if blank else end
            index += 1
        return end
    if stop_at_next_key:
        found = _NEXT_KEY.search(text, start)
        return found.start() if found else end
    blank = _BLANK.search(text, start)
    return blank.start() if blank else end


def _line_end(text: str, start: int) -> int:
    found = _LINE_END.search(text, start)
    return found.start() if found else len(text)


def _userinfo_spans(line: str) -> list[Span]:
    """URL passwords of any scheme, from the user's ``:`` to the last ``@``.

    The password runs to the LAST ``@`` on the line, so a raw ``@``, blank,
    ``/``, ``:``, ``?``, ``#`` or quote inside it is masked, not shown, and a
    password whose first word is all digits (``u:2024 secret@h``) is never
    read as a port. A port-shaped value (up to five digits ended by ``/ , ?
    #`` or the end of the line) is a port and is kept only when no ``@``
    follows anywhere on the line, so ``redis://h:6379/0`` is untouched but
    ``redis://h:6379/0 mail a@b`` is masked up to ``a@``. Otherwise
    this over-masks a line that holds an ``@`` after the URL
    (``postgresql://u:pw@h/db as a@b`` masks up to ``a@``), which is the
    rule: over-mask, never under-mask. A password that starts with digits
    and ``/`` and holds blanks (``u:12/a b c@h``) is therefore always masked,
    at the cost of prose after a port that names an e-mail address.
    With no ``@`` after the colon, anything not port-shaped is masked to the
    next blank (a truncated URL).
    """
    if "://" not in line:
        return []
    spans: list[Span] = []
    last_at = line.rfind("@")
    position = 0
    # The blank-free run around the last colon checked, and the first `@` in
    # it at or after that colon; reused while colons stay in the same run, so
    # the scan stays linear.
    run_end = -1
    run_at = -1
    while True:
        found = _URL_USER.search(line, position)
        if found is None:
            break
        colon = found.end()
        if _PORT_AFTER_COLON.match(line, colon):
            if colon >= run_end:
                blank = _BLANK.search(line, colon)
                run_end = blank.start() if blank else len(line)
                run_at = line.find("@", colon, run_end)
            elif run_at != -1 and run_at < colon:
                run_at = line.find("@", colon, run_end)
            # A port-shaped run is a port only when no `@` follows anywhere on
            # the line; otherwise it may be a password with a blank in it
            # (`u:12/a b c@h`), so it is masked to the last `@`.
            if run_at == -1 and last_at < run_end:
                position = colon
                continue
        if colon <= last_at:
            # Everything up to the last `@` is covered; no later URL on this
            # line can have a userinfo password that is not masked already
            # or that ends at an `@`.
            spans.append((colon, last_at, MASK))
            position = last_at + 1
            continue
        if _PORT_AFTER_COLON.match(line, colon):
            position = colon
            continue
        blank = _BLANK.search(line, colon)
        end = blank.start() if blank else len(line)
        spans.append((colon, end, MASK))
        position = max(end, colon)
    for found in _loose_users(line):
        if found.end() <= last_at:
            spans.append((found.end(), last_at, MASK))
    return spans


def _loose_users(line: str) -> list[re.Match[str]]:
    """:data:`_URL_LOOSE_USER` matches that are not an IPv6 host."""
    users = []
    for found in _URL_LOOSE_USER.finditer(line):
        host = _IPV6_HOST.match(line, found.start() + 3)
        if host is None or host.group(1).count(":") < 2:
            users.append(found)
    return users


def _secret_spans(text: str, flag_re: re.Pattern[str] | None) -> list[Span]:
    """Every span of ``text`` to mask, with its marker."""
    # Each loop skips a match that starts inside the span it already masked,
    # so no stretch of text is scanned twice and the whole pass stays linear
    # (a line of `PGPASSWORD=:` or `--dsn=:` repeated has a match every few
    # characters, each of which would otherwise scan to the end).
    spans: list[Span] = []
    if flag_re is not None:
        covered = -1
        for match in flag_re.finditer(text):
            if match.start() < covered:
                continue
            end = _value_end(text, match.end(), stop_at_next_key=False)
            if end > match.end():
                spans.append((match.end(), end, REDACTED_DSN))
            covered = end
    covered = -1
    for match in _ENV_SECRET.finditer(text):
        if match.start() < covered:
            continue
        end = _line_end(text, match.end())
        spans.append((match.end(), end, MASK))
        covered = end
    spans.extend(_query_spans(text))
    covered = -1
    for match in _KEYWORD_SECRET.finditer(text):
        if match.start() < covered:
            continue
        value = match.end()
        first = _NON_BLANK.search(text, value)
        if first is not None and first.start() > value:
            # A line break after `key=` (one DSN read across lines): libpq
            # skips it, so the value is the next word, even one shaped like
            # `x=y`; then on to a ` key=` token as usual.
            word = _value_end(text, first.start(), stop_at_next_key=False)
            end = _value_end(text, word, stop_at_next_key=True) if word < len(text) else word
        else:
            end = _value_end(text, value, stop_at_next_key=True)
        spans.append((value, end, MASK))
        covered = end
    spans.extend(_userinfo_spans(text))
    return spans


def _query_spans(text: str) -> list[Span]:
    """``?password=`` values: to the closing quote of a quoted DSN token, else
    to the end of the line.

    The token is the blank-free run the parameter sits in; its opener is the
    first quote in the run before the parameter. The backward search for the
    run start and the forward search for its quote both stop at the previous
    match, and a match inside a masked span is skipped, so the pass stays
    linear.
    """
    spans: list[Span] = []
    covered = -1
    previous = 0
    quote_from = 0
    run_quote: int | None = None
    for match in _QUERY_SECRET.finditer(text):
        if match.start() < covered:
            continue
        blank = max(text.rfind(ch, previous, match.start()) for ch in _RUN_BREAKS)
        if blank != -1:
            quote_from = blank + 1
            run_quote = None
        if run_quote is None:
            found = _QUOTE.search(text, quote_from, match.start())
            run_quote = found.start() if found else None
            quote_from = match.start()
        previous = match.start()
        value = match.end()
        end = -1
        if run_quote is not None and run_quote < match.start():
            # Search to the closing quote first and check that no line
            # break comes before it: each search is bounded by the span it
            # masks, not by the rest of the text.
            end = _closing_quote(text, text[run_quote], value)
            if end != -1 and _LINE_END.search(text, value, end):
                end = -1
        if end == -1:
            end = _line_end(text, value)
        spans.append((value, end, MASK))
        covered = end
    return spans


_RUN_BREAKS = (" ", "\t", "\r", "\n")


def _closing_quote(text: str, quote: str, start: int, limit: int | None = None) -> int:
    """The next ``quote`` at or after ``start`` that no backslash escapes.

    A quote after an odd number of backslashes is part of the value (JSON
    ``\\"``, a Python repr's ``\\'``), as in :func:`_value_end`. Linear in the
    distance searched: each backslash run is counted once, back to ``start``.
    With ``limit``, only a quote that ends by ``limit`` counts.
    """
    index = start
    end = len(text) if limit is None else limit
    while True:
        found = text.find(quote, index, end)
        if found == -1:
            return -1
        slashes = 0
        back = found - 1
        while back >= start and text[back] == "\\":
            slashes += 1
            back -= 1
        if slashes % 2 == 0:
            return found
        index = found + 1


def _apply_spans(text: str, spans: list[Span]) -> str:
    if not spans:
        return text
    spans.sort(key=lambda span: (span[0], -span[1]))
    merged: list[list[Any]] = []
    for start, end, marker in spans:
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
            if marker == REDACTED_DSN:
                merged[-1][2] = REDACTED_DSN
            continue
        merged.append([start, end, marker])
    pieces: list[str] = []
    cursor = 0
    for start, end, marker in merged:
        pieces.append(text[cursor:start])
        pieces.append(marker)
        cursor = end
    pieces.append(text[cursor:])
    return "".join(pieces)


def redact(dsn: str) -> str:
    """``dsn`` with every secret replaced by ``***``, read as ONE DSN.

    Unlike :func:`redact_text`, a line break does not end a secret here: an
    unquoted keyword password runs to the next `` key=`` token or the end of
    the string. Never raises on a ``str``; any other value is returned as is.
    """
    if not isinstance(dsn, str):
        return dsn
    return _apply_spans(dsn, _secret_spans(dsn, None))


def redact_text(text: str, *, dsn_flags: Sequence[str] = DEFAULT_DSN_FLAGS) -> str:
    """``text`` with the secrets of every DSN in it masked, line by line.

    Masks URL userinfo passwords of any scheme, ``?password=``-style query
    values, keyword ``password=``/``sslpassword=``/``passfile=`` values,
    ``PGPASSWORD``/``PGSSLPASSWORD`` assignments, and the whole value of a
    ``--dsn``/``--postgres-dsn`` flag on the same line. The lines after a
    secret survive, except where a password may continue past its line: a
    single-quoted value runs to its closing quote (libpq accepts a line break
    inside one), a URL password a log wrapped is masked on the next line up
    to its ``@``, and an unquoted keyword password that reaches the end of
    its line masks at most the next line, up to a `` key=`` token on it
    (:func:`_continuation_spans`). Never raises on a ``str``; any other
    value is returned as is.
    """
    if not isinstance(text, str):
        return text
    flags = tuple(dsn_flags)
    flag_re = _DEFAULT_FLAG_RE if flags == DEFAULT_DSN_FLAGS else _flag_pattern(flags)
    bounds: list[tuple[int, int]] = []
    cursor = 0
    for brk in _LINE_BREAK.finditer(text):
        bounds.append((cursor, brk.start()))
        cursor = brk.end()
    bounds.append((cursor, len(text)))
    spans: list[Span] = []
    for start, end in bounds:
        line = text[start:end]
        spans.extend(
            (first + start, last + start, marker)
            for first, last, marker in _secret_spans(line, flag_re)
        )
    if len(bounds) > 1:
        spans.extend(_quoted_spans_across_lines(text))
        spans.extend(_continuation_spans(text, bounds))
    return _apply_spans(text, spans)


def _quoted_spans_across_lines(text: str) -> list[Span]:
    """Single-quoted keyword secrets not closed on their own line.

    libpq accepts a line break inside a quoted value, so the mask goes on
    past the line, but by at most ONE following line, as for a wrapped
    unquoted password: up to a closing quote on that line (and anything glued
    to it), else up to a `` key=`` token on it, else the whole line. A
    ``key=`` at the start of that line ends the mask at the line break. Every
    later line is kept, so an apostrophe further on (``don't``) cannot hide
    the lines in between. A quote closed on its own line is left to the
    one-line rule.
    """
    spans: list[Span] = []
    covered = -1
    for match in _KEYWORD_SECRET.finditer(text):
        value = match.end()
        if value < covered or value >= len(text) or text[value] != "'":
            continue
        quote = next(
            (
                splice
                for splice in _SHELL_QUOTE_SPLICES
                if text.startswith(splice, value)
            ),
            "'",
        )
        line_end = _line_end(text, value)
        closing = _closing_quote(text, quote, value + len(quote), line_end)
        if closing != -1:
            covered = closing
            continue
        brk = _LINE_BREAK.match(text, line_end)
        if brk is None:
            covered = line_end
            continue
        following_start = brk.end()
        following_end = _line_end(text, following_start)
        closing = _closing_quote(text, quote, following_start, following_end)
        # The rest of this line is masked by the one-line rule; the line
        # break itself is kept, as for a wrapped unquoted password.
        if closing != -1:
            blank = _BLANK.search(text, closing + len(quote), following_end)
            end = blank.start() if blank else following_end
        elif _LINE_START_KEY.match(text, following_start, following_end):
            end = following_start
        else:
            key = _NEXT_KEY.search(text, following_start, following_end)
            end = key.start() if key is not None else following_end
        spans.append((value, line_end, MASK))
        if end > following_start:
            spans.append((following_start, end, MASK))
        covered = max(end, line_end)
    return spans


# A `key=` token at the very start of a line.
_LINE_START_KEY = re.compile(r"[ \t]*+[A-Za-z_][A-Za-z0-9_]*+[ \t]*+=")


def _continuation_spans(text: str, bounds: list[tuple[int, int]]) -> list[Span]:
    """The part of a password that a log wrapped past the end of its line.

    * A URL whose userinfo password runs to the end of its line with no ``@``
      (``postgresql://u:ab`` then ``cd@h/db``): the next line is masked up to
      its last ``@``.
    * An unquoted keyword secret that runs to the end of its line
      (``password=ab`` then ``cd dbname=x``): the next line is masked up to
      its first `` key=`` token, or whole when it has none (nothing marks
      where the password ends). A ``key=`` at the start of the next line ends
      the password there. The lines after the next one are always kept
      (#2285 item 1): a wrapped remainder can only land on the next line.
    """
    spans: list[Span] = []
    covered = -1
    for index in range(len(bounds) - 1):
        start, end = bounds[index]
        if start < covered:
            continue
        line = text[start:end]
        following_start, following_end = bounds[index + 1]
        if following_start >= len(text):
            continue
        if _ends_in_open_userinfo(line):
            at = text.rfind("@", following_start, following_end)
            if at > following_start:
                spans.append((following_start, at, MASK))
                covered = at
                continue
        loose = _open_loose_userinfo(line)
        if loose is not None:
            # A loose user's password is masked on its own line only when an
            # `@` follows there, so a wrapped one is masked on both lines.
            at = text.rfind("@", following_start, following_end)
            if at >= following_start:
                spans.append((start + loose, end, MASK))
                spans.append((following_start, at, MASK))
                covered = at
                continue
        if _ends_in_empty_keyword_secret(line):
            span = _empty_value_span(text, end)
            if span is not None:
                spans.append(span)
                covered = span[1]
            continue
        if _ends_in_open_keyword_secret(line):
            key = _LINE_START_KEY.match(text, following_start, following_end)
            if key is not None:
                continue
            key = _NEXT_KEY.search(text, following_start, following_end)
            stop = key.start() if key is not None else following_end
            if stop > following_start:
                spans.append((following_start, stop, MASK))
                covered = stop
    return spans


def _ends_in_open_userinfo(line: str) -> bool:
    if "://" not in line:
        return False
    last = None
    for last in _URL_USER.finditer(line):
        pass
    if last is None:
        return False
    rest = line[last.end() :]
    return (
        bool(rest)
        and "@" not in rest
        and _BLANK.search(rest) is None
        and (_PORT_AFTER_COLON.match(rest) is None)
    )


def _open_loose_userinfo(line: str) -> int | None:
    """Where the password of a loose URL user starts, when it runs to the end
    of ``line`` with no ``@`` after it; else ``None``."""
    if "://" not in line:
        return None
    users = _loose_users(line)
    if not users:
        return None
    colon = users[-1].end()
    if "@" in line[colon:]:
        return None
    return colon


def _ends_in_open_keyword_secret(line: str) -> bool:
    last = None
    for last in _KEYWORD_SECRET.finditer(line):
        pass
    if last is None:
        return False
    value = last.end()
    if value >= len(line) or line[value] in "'\"":
        return False
    return _NEXT_KEY.search(line, value) is None


def _ends_in_empty_keyword_secret(line: str) -> bool:
    last = None
    for last in _KEYWORD_SECRET.finditer(line):
        pass
    return last is not None and last.end() >= len(line)


def _empty_value_span(text: str, line_end: int) -> Span | None:
    """The value of a keyword secret written as ``key=`` at a line end.

    libpq skips blanks and line breaks after ``=``, so the value is the
    first word after them, however many lines on, even one shaped like
    ``key=``. That word is masked, and the rest of its line up to a
    `` key=`` token, as for a wrapped password. A quoted word is masked to
    its closing quote, but by at most one line past its own.
    """
    first = _NON_BLANK.search(text, line_end)
    if first is None:
        return None
    start = first.start()
    own_end = _LINE_END.search(text, start)
    own_line_end = own_end.start() if own_end else len(text)
    stop = _value_end(text, start, stop_at_next_key=False)
    if text[start] in "'\"":
        after = _LINE_END.search(text, own_line_end + 1)
        stop = min(stop, after.start() if after else len(text))
    else:
        key = _NEXT_KEY.search(text, stop, own_line_end)
        stop = max(stop, key.start() if key else own_line_end)
    return (start, stop, MASK)


def redact_argv(
    argv: Sequence[str], *, dsn_flags: Sequence[str] = DEFAULT_DSN_FLAGS
) -> list[str]:
    """``argv`` with DSN flag values replaced whole and every token redacted.

    The token after a flag in ``dsn_flags``, and the value of
    ``--flag=value``, become ``<redacted-dsn>``. Every other token goes
    through :func:`redact_text`, which also catches keyword-form DSN tokens.
    """
    flags = tuple(dsn_flags)
    out: list[str] = []
    mask_next = False
    for raw in argv:
        token = str(raw)
        if mask_next:
            out.append(REDACTED_DSN)
            mask_next = False
            continue
        if token in flags:
            out.append(token)
            mask_next = True
            continue
        flag = next((f for f in flags if token.startswith(f + "=")), None)
        if flag is not None:
            out.append(f"{flag}={REDACTED_DSN}")
            continue
        out.append(redact_text(token, dsn_flags=flags))
    return out


def redact_value(value: Any) -> Any:
    """:func:`redact_text` over every string inside dicts, lists and tuples.

    Dictionary keys are kept; values are redacted. Other types pass through.
    """
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    return value


#: What :func:`redact_target` writes in place of any user info.
REDACTED_USERINFO = "<redacted>"

#: Schemes whose URL path names a database, so :func:`redact_target` may
#: show it. Any other scheme's path may be a secret itself (a webhook URL's
#: path is its token), so it is withheld.
DATABASE_SCHEMES: frozenset[str] = frozenset({
    "postgres", "postgresql", "mysql", "mariadb", "redshift", "cockroachdb",
    "redis", "rediss", "mongodb", "mongodb+srv",
})


def redact_target(value: str) -> str:
    """``value`` shown as where it points: ``scheme://<redacted>@host:port/dbname``.

    For diagnostics that name a URL's target and never who connects or with
    what: the user info is withheld whole, user name included, for any
    scheme, so a token in the user slot (``https://TOKEN@host``) is hidden
    too. The path is shown only for a :data:`DATABASE_SCHEMES` scheme, where
    it names the database; for any other scheme a path becomes
    ``/<redacted>``, since it may be the secret (a Slack webhook's path). The
    query and fragment are never shown. Returns :data:`REDACTED_DSN` when the
    target cannot be read without
    guessing: no scheme or host, a port that is not a number in range, or an
    ``@`` after the authority. A raw ``/``, ``?`` or ``#`` in a password ends
    the authority early, so what reads as the port, path or query would be
    the password's tail (#2285 item 4). Never raises on a ``str``; any other
    value becomes :data:`REDACTED_DSN`.
    """
    from urllib.parse import urlsplit

    if not isinstance(value, str):
        return REDACTED_DSN
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError:
        return REDACTED_DSN
    host = parts.hostname
    if not parts.scheme or not host:
        return REDACTED_DSN
    _, _, rest = value.partition("://")
    if "@" in rest[len(parts.netloc):]:
        return REDACTED_DSN
    shown_host = f"[{host}]" if ":" in host else host
    shown_port = f":{port}" if port is not None else ""
    path = parts.path.lstrip("/")
    if not path:
        suffix = ""
    elif parts.scheme.lower() in DATABASE_SCHEMES:
        suffix = f"/{path}"
    else:
        suffix = f"/{REDACTED_USERINFO}"
    return f"{parts.scheme}://{REDACTED_USERINFO}@{shown_host}{shown_port}{suffix}"


__all__ = [
    "DEFAULT_DSN_FLAGS",
    "LIBPQ_KEYWORD_PARAM",
    "MASK",
    "REDACTED_DSN",
    "SECRET_KEYS",
    "Dsn",
    "DsnError",
    "DsnFormError",
    "DsnParseError",
    "DsnPlaceholderEmpty",
    "DsnPlaceholderError",
    "DsnPlaceholderUnset",
    "DsnPlaceholderUnsupported",
    "DsnRenderError",
    "Expansion",
    "Form",
    "expand",
    "form_of",
    "parse",
    "parse_refusal",
    "placeholders",
    "redact",
    "redact_argv",
    "redact_target",
    "redact_text",
    "redact_value",
    "require_expanded",
    "require_url_form",
    "try_parse",
]
