"""Shared redaction helpers for source preflight/run status."""
from __future__ import annotations

import functools
import re
from urllib.parse import urlsplit, urlunsplit

from okg.substrate.db import dsn as _dsn

# DSN secrets are masked by the shared module's line-aware text masker, which
# `redact_text` below runs first. It keeps every line of a multi-line
# diagnostic (the old masker dropped the lines after an unquoted
# `password=`), masks a password containing a raw `@`, and masks
# `sslpassword=` and `PGPASSWORD=`. The rules below are the generic token
# layer: headers, `*TOKEN*=`-style assignments and token-shaped strings.
#
# No whole-value flag rule (`dsn_flags=()`): diagnostics and hints name
# `--dsn` in prose ("pass a valid --dsn or use ...") and in runnable resume
# commands (`--dsn "${VAR:?...}"`), and replacing the word after the flag
# would break both. A DSN after `--dsn` still has its password masked.
redact_dsn = functools.partial(_dsn.redact_text, dsn_flags=())


_AUTH_HEADER_RE = re.compile(
    r"(?i)\b(authorization|private-token|x-api-key)\s*[:=]\s*"
    # A value already masked (`Bearer <secret:redacted>`) is left alone, so a
    # second pass changes nothing.
    r"(?!(?:(?:bearer|basic|token|digest|negotiate)\s+)?<(?:secret:)?redacted>)"
    r"(?:(?:bearer|basic|token|digest|negotiate)\s+)?[^\s,;]+"
)
# `(?!<redacted>...)` skips a value an earlier pattern already masked, so
# `?token=<redacted>&n=2` keeps its `&n=2` tail; text glued to the mask
# (`<redacted>#rest`) is still swallowed.
_TOKEN_ASSIGNMENT_RE = re.compile(
    # The atomic lookahead `(?=(?>[A-Z0-9_]+)=)` checks once that the word ends
    # in `=`; without it a long word of `token` repeats backtracks quadratically.
    # `PASS` is a whole word, optionally `PASSWD`/`PASSPHRASE`/`PASSCODE` or
    # numbered (`DB_PASS1`), so `passed` and `bypass` stay readable. A value
    # that is empty (`password=,`), a whole `<placeholder>`, or a whole
    # true/false/null/none carries no secret, and neither does a plain number
    # for a `*tokens` count (`max_tokens=4000`). "Whole" means the value ends
    # there: `true-X` and `none.X` are still masked.
    # The name is matched as one atomic word, and the keyword is looked for
    # in a lookahead, so a failure after `=` (an empty value) cannot
    # backtrack into the name: that was quadratic on `tokentoken...=`.
    r"(?i)\b(?=(?>[A-Z0-9_]+)=)"
    r"(?=[A-Z0-9_]*?(?:TOKEN|SECRET|PASSWORD|"
    r"(?<![A-Z0-9])PASS(?:WD|PHRASE|CODE)?\d*(?![A-Z0-9])|API_KEY))"
    r"((?>[A-Z0-9_]+))="
    r"(?![\"']?<(?:secret:)?redacted>[\"']?(?:[&\s,;)\]}]|$))"
    r"(?![,;)\]}])"
    r"(?!<(?!(?:secret:)?redacted>)[A-Za-z_-]+>(?=[\s,;)\]}&\"']|$))"
    r"(?!(?:true|false|null|none)(?=[\s,;)\]}&\"']|$))"
    r"(?!(?<=tokens=)\d+(?=[\s,;)\]}&\"']|$))"
    r"([^\s]+)"
)
# A content digest is NOT a secret. `_PAT_LIKE_RE` below matches any run of
# 24+ word characters, so every `sha256:<64 hex>` in an operator-visible
# diagnostic printed as the literal `sha256:<redacted>` -- which is exactly
# the value an operator needs when a publish refuses on a fingerprint
# mismatch (#2213). Digests are matched FIRST and returned unchanged.
_DIGEST_RE_SRC = r"\b(?:sha256|sha512|sha1|md5|blake2b):[0-9a-f]{32,128}\b"
_DIGEST_RE = re.compile(_DIGEST_RE_SRC)
# Signed-URL query values (`?sig=...`, `&X-Amz-Signature=...`). Only the value
# is masked; the URL path and host stay readable.
_SIGNED_QUERY_RE = re.compile(
    r"(?i)([?&](?:sig|signature|x-amz-signature|x-amz-credential"
    r"|x-amz-security-token|token|access_token|key))=([^&\s#\"'<>]+)"
)
# A quoted value (dict or JSON text) with backslash escapes, e.g. "a\\"b".
_QUOTED_SRC = (
    r"(?:\"(?:[^\"\\\r\n]|\\.)*\"|'(?:[^'\\\r\n]|\\.)*')"
)
# `Cookie:` / `Set-Cookie:` header values run to the end of the line: they hold
# `;`-separated pairs and may hold spaces. A quoted key (`{'Cookie': '...'}`)
# takes one quoted value, which stops at its closing quote.
_COOKIE_HEADER_RE = re.compile(
    r"(?i)\b(set-cookie|cookie)(?:[\"']\s*:\s*" + _QUOTED_SRC + r"|\s*:[^\r\n]*)"
)
# `auth=` and prefixed forms (`basic_auth=`, `http_auth=`); `author=`,
# `oauth=`, `unauth=` and `authority=` are different words and stay readable.
# An unterminated quote (`auth="X`) still masks the value.
_AUTH_ASSIGNMENT_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9])((?:[A-Za-z0-9]+[_-])?auth)\s*=\s*"
    r"(?!(?:true|false|null|none)(?=[\s,;)\]}&\"']|$))(?:"
    + _QUOTED_SRC
    + r"|[\"']?[^\s&,;\"'<>]+)"
)
_PAT_LIKE_RE = re.compile(
    _DIGEST_RE_SRC
    + r"|\b(?:glpat-[A-Za-z0-9_\-]+|[A-Za-z0-9_\-]{24,})\b"
)


_SECRET_FORM_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("signed_url_value", _SIGNED_QUERY_RE),
    ("cookie_header", _COOKIE_HEADER_RE),
    ("auth_assignment", _AUTH_ASSIGNMENT_RE),
    ("authorization_header", _AUTH_HEADER_RE),
    ("token_assignment", _TOKEN_ASSIGNMENT_RE),
)


def redact_secret_forms(text: str) -> tuple[str, tuple[str, ...]]:
    """Mask signed-URL values, Cookie/Authorization headers, `auth=` and
    `*TOKEN=`-style assignments; return the text and the forms that fired.

    This is the rule set `redact_text` runs after the DSN masker, without its
    long-word-run layer. The bug-report redactor and its leak check share it,
    so a form masked here is also refused there. Masking is idempotent: text
    already masked comes back unchanged.
    """
    fired: list[str] = []
    for name, pattern in _SECRET_FORM_RULES:
        changed = False

        def _mask(m: re.Match[str]) -> str:
            nonlocal changed
            masked = f"{m.group(1)}=<redacted>"
            changed = changed or masked != m.group(0)
            return masked

        text = pattern.sub(_mask, text)
        if changed:
            fired.append(name)
    return text, tuple(fired)


def redact_endpoint(endpoint: str | None) -> str | None:
    """Drop query/fragment/userinfo from an endpoint string."""
    if not endpoint:
        return endpoint
    parts = urlsplit(endpoint)
    if not parts.scheme or not parts.netloc:
        return redact_text(endpoint)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def redact_text(value: object) -> str:
    """Best-effort secret redaction for diagnostics."""
    text = str(value)
    text = redact_dsn(text)
    text, _forms = redact_secret_forms(text)
    return _PAT_LIKE_RE.sub(
        lambda m: (
            m.group(0)
            if _DIGEST_RE.fullmatch(m.group(0))
            or (m.group(0).upper() == m.group(0) and "_" in m.group(0))
            else "<redacted>"
        ),
        text,
    )


def _key_names_secret(key: str) -> bool:
    """True if a mapping key names a secret-bearing value.

    Derives from the SHARED secret-key detector (`looks_secret_key` /
    SECRET_KEY_MARKERS) so redaction is ALWAYS at least as wide as the doctor's
    detection — previously the redaction regex was narrower than the detector, so
    keys the value-free scan flags (private_key, credential, access_key, apikey,
    …) were echoed in plaintext through MCP envelopes (adversarial-review
    remediation). The extra `pass|authorization` covers redaction-only forms
    (HTTP header keys, passcode/passwd) that aren't catalog markers. Deferred
    import avoids any module-load cycle with nomos.core."""
    from okg.substrate.nomos.core import looks_secret_key

    return looks_secret_key(key) or bool(re.search(r"(?i)(pass|authorization)", key))


def redact_mapping(mapping: dict) -> dict:
    """Redact a flat/JSON-like mapping recursively."""
    out = {}
    for key, value in mapping.items():
        k = str(key)
        if _key_names_secret(k):
            out[k] = "<redacted>"
        elif isinstance(value, dict):
            out[k] = redact_mapping(value)
        elif isinstance(value, list):
            out[k] = [
                redact_mapping(v) if isinstance(v, dict)
                else redact_text(v)
                for v in value
            ]
        else:
            out[k] = redact_text(value)
    return out
