"""What a console read may say about the server it runs on (issue #2130).

Identity and provenance fields a console read composes never serve a
filesystem path, a connection string, or an exception's text. A path names the
machine, the account and often the person; a connection string can carry a
password; an exception's text can quote the host, the address, the role, the
failing SQL and the database objects it touched. None of that helps an operator
act, and all of it leaves the server the moment a browser reads the payload.

Two helpers, one rule:

* :func:`mask_served_text` walks a composed payload and rewrites what it finds.
  A path under a root the read knows is kept relative to a label naming that
  root (``<deployment directory>/deployment.yaml``); any other absolute path is
  replaced by ``<path withheld>``; a connection string by
  ``<connection string withheld>``; a string shaped like an exception or a
  traceback by one plain sentence. It returns the dotted trail of every field
  it rewrote, so the payload can say which values are not as recorded.
* :func:`console_failure_detail` is the one failure body every console route
  serves: a reason code, the exception's TYPE, a fixed sentence and
  ``error_text_unavailable_reason``. The exception itself goes to the server
  log, and only there.

Record CONTENT and excerpts are not composed by the console and are not passed
through here: they are served as stored (#2179). The rule's own blind spot is
stated in :data:`SERVED_TEXT_RULE`, the same way ``masked_paths_rule`` states
the record reads' blind spot.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable, Mapping

#: Labels for the roots a read knows. A path under one keeps its tail.
DEPLOYMENT_DIRECTORY = "<deployment directory>"
SYNC_ROOT = "<sync root>"
WORKING_DIRECTORY = "<working directory>"

#: What replaces a path under no known root.
PATH_WITHHELD = "<path withheld>"
#: What replaces a connection string, whole.
CONNECTION_WITHHELD = "<connection string withheld>"
#: What replaces the string value of a credential-named key, whole.
CREDENTIAL_WITHHELD = "<credential withheld>"
#: What replaces a string shaped like an exception or a traceback, whole.
EXCEPTION_TEXT_WITHHELD = (
    "The server recorded an error here. Its text is not served; the server "
    "log holds it."
)

#: The reason code a field carries when its value is not served at all.
SERVER_PATH_NOT_SERVED = "server_path_not_served"
EXCEPTION_TEXT_NOT_SERVED = "exception_text_not_served"

#: The rule, including what it cannot see. Served beside every rewritten trail.
SERVED_TEXT_RULE = (
    "a path under a root this read knows is shown relative to a label naming "
    "that root; any other absolute path (a string or a word starting with '/', "
    "'~/', a drive letter or '\\\\') is replaced by <path withheld>; a "
    "connection string by <connection string withheld>; the text value of a "
    "key named like a credential (password, secret, token, dsn, api key, "
    "private key) by <credential withheld>; a value shaped like an exception "
    "or a traceback by a fixed sentence. A relative path, a "
    "hostname, an address or a database object name written in plain text "
    "outside a connection string or an exception is NOT detected and is "
    "served as recorded"
)

#: One fixed sentence for every console read failure.
READ_FAILED_SENTENCE = (
    "This read failed inside the server. The exception's text is not served; "
    "the server log holds it."
)

_LOG = logging.getLogger("okg.viz.console_failures")

# A traceback, a `File "...", line N` frame, `SomeError:` / `SomeException:`
# at a word boundary, a PostgreSQL severity prefix, a SQLSTATE, or psycopg
# naming itself. Any of these makes the whole string exception text.
_EXCEPTION_SHAPED = re.compile(
    r"Traceback \(most recent call last\)"
    r"|\bFile \"[^\"]*\", line \d+"
    r"|\b[A-Z][A-Za-z0-9_]*(?:Error|Exception)\b\s*:"
    r"|\b(?:FATAL|PANIC):"
    r"|\bSQLSTATE\b"
    r"|\bpsycopg2?\b"
)

# scheme://user[:password]@host..., or any postgres URL with or without
# credentials. A libpq keyword string (`host=... password=...`) is caught
# separately.
_URL_WITH_CREDENTIALS = re.compile(
    r"\b[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>@/]*@[^\s'\"<>]*"
)
_POSTGRES_URL = re.compile(r"\bpostgres(?:ql)?(?:\+\w+)?://[^\s'\"<>]*", re.I)
_KEYWORD_DSN = re.compile(r"\b(?:password|passfile|sslpassword)\s*=", re.I)
_KEYWORD_DSN_PAIR = re.compile(r"\bhost\s*=\S+.*\bdbname\s*=", re.I | re.S)

# An absolute path written as a word: `/`, `~/`, `C:\` or `C:/`, or `\\host`,
# not preceded by anything that makes it the tail of a longer word (`a/b`,
# `http://x`, `<label>/tail`).
_PATH_WORD = re.compile(
    r"(?<![\w.:/~>\\-])(?:~/|/|[A-Za-z]:[\\/]|\\\\)[^\s'\"(),;<>\[\]`]+"
)


# A key whose text value is a credential by its NAME, whatever the value
# looks like: `password: hunter2` has no shape a pattern could see.
_CREDENTIAL_KEY = re.compile(
    r"(?:^|[_.-])(?:pass(?:word|wd)?|secret|token|dsn|api[_-]?key|"
    r"private[_-]?key|credentials?)$",
    re.I,
)


def _roots(roots: Iterable[tuple[str | None, str]]) -> list[tuple[str, str]]:
    """Known roots, longest first, each without a trailing separator."""
    out: list[tuple[str, str]] = []
    for root, label in roots:
        if not root:
            continue
        text = str(root).rstrip("/\\")
        if len(text) < 2:
            # `/` is not a root worth naming; it would relabel every path.
            continue
        out.append((text, label))
    return sorted(out, key=lambda item: len(item[0]), reverse=True)


def _rewrite(text: str, roots: list[tuple[str, str]], *, whole: bool) -> str:
    """``text`` with its paths and connection strings rewritten.

    ``whole`` is True for a value (which may be exception text) and False for
    an object key (which is never replaced by a sentence).
    """
    if whole and _EXCEPTION_SHAPED.search(text):
        return EXCEPTION_TEXT_WITHHELD
    if _KEYWORD_DSN.search(text) or _KEYWORD_DSN_PAIR.search(text):
        return CONNECTION_WITHHELD
    out = _POSTGRES_URL.sub(CONNECTION_WITHHELD, text)
    out = _URL_WITH_CREDENTIALS.sub(CONNECTION_WITHHELD, out)
    for root, label in roots:
        # A root is replaced only where it ends at a separator or the end of
        # a word, so `/srv/okg` does not relabel `/srv/okg-old`.
        out = re.sub(
            re.escape(root) + r"(?=[/\\]|$|[\s'\"(),;<>\]`])", label, out,
        )
    return _PATH_WORD.sub(PATH_WITHHELD, out)


def mask_served_text(
    value: Any,
    *,
    roots: Iterable[tuple[str | None, str]] = (),
    at: str = "",
) -> tuple[Any, list[str]]:
    """``value`` with server paths, connection strings and exception text rewritten.

    Walks objects, arrays and strings, keys included. Returns the rewritten
    value and the dotted trails of every field that changed, under the name
    each field was emitted as. Numbers, booleans and null pass through.
    """
    return _walk(value, _roots(roots), at)


def _walk(value: Any, roots: list[tuple[str, str]], at: str) -> tuple[Any, list[str]]:
    if isinstance(value, str):
        rewritten = _rewrite(value, roots, whole=True)
        return rewritten, ([at or "."] if rewritten != value else [])
    if isinstance(value, Mapping):
        masked: dict[Any, Any] = {}
        changed: list[str] = []
        for key, item in value.items():
            emitted = key
            if isinstance(key, str):
                emitted = _rewrite(key, roots, whole=False)
                suffix = 2
                base = emitted
                while emitted in masked:
                    emitted = f"{base}~{suffix}"
                    suffix += 1
            trail = f"{at}.{emitted}" if at else str(emitted)
            if (
                isinstance(key, str) and isinstance(item, str) and item
                and _CREDENTIAL_KEY.search(key)
            ):
                masked[emitted] = CREDENTIAL_WITHHELD
                changed.append(trail)
                continue
            masked[emitted], inner = _walk(item, roots, trail)
            if emitted != key:
                changed.append(trail)
            changed.extend(inner)
        return masked, changed
    if isinstance(value, (list, tuple)):
        out: list[Any] = []
        changed = []
        for index, item in enumerate(value):
            item_masked, inner = _walk(item, roots, f"{at}[{index}]")
            out.append(item_masked)
            changed.extend(inner)
        return out, changed
    return value, []


def log_console_failure(reason: str, exc: BaseException) -> None:
    """Record a failed console read, with its full text and traceback, in the
    server log -- the one place the exception's text goes."""
    _LOG.error("console read failed: %s", reason, exc_info=exc)


def console_failure_detail(
    reason: str,
    exc: BaseException,
    **context: Any,
) -> dict[str, Any]:
    """The one body a console read serves when it fails.

    The reason code names what failed; ``error_type`` names the exception's
    class; ``error`` is a fixed sentence the console prints; nothing carries
    the exception's text, which the server log records here with its
    traceback. ``context`` is what the caller already knows and the client
    sent or can see (a generation id, a requested name); it goes through
    :func:`mask_served_text` all the same.
    """
    log_console_failure(reason, exc)
    served_context, _ = mask_served_text(context)
    return {
        "reason": reason,
        **served_context,
        "error": READ_FAILED_SENTENCE,
        "error_type": type(exc).__name__,
        "error_text_unavailable_reason": EXCEPTION_TEXT_NOT_SERVED,
    }


__all__ = [
    "CONNECTION_WITHHELD",
    "CREDENTIAL_WITHHELD",
    "DEPLOYMENT_DIRECTORY",
    "EXCEPTION_TEXT_NOT_SERVED",
    "EXCEPTION_TEXT_WITHHELD",
    "PATH_WITHHELD",
    "READ_FAILED_SENTENCE",
    "SERVED_TEXT_RULE",
    "SERVER_PATH_NOT_SERVED",
    "SYNC_ROOT",
    "WORKING_DIRECTORY",
    "console_failure_detail",
    "log_console_failure",
    "mask_served_text",
]
