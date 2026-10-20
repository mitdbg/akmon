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
    r"(bearer\s+)?[^\s,;]+"
)
_TOKEN_ASSIGNMENT_RE = re.compile(
    r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASS|API_KEY)"
    r"[A-Z0-9_]*)=([^\s]+)"
)
# A content digest is NOT a secret. `_PAT_LIKE_RE` below matches any run of
# 24+ word characters, so every `sha256:<64 hex>` in an operator-visible
# diagnostic printed as the literal `sha256:<redacted>` -- which is exactly
# the value an operator needs when a publish refuses on a fingerprint
# mismatch (#2213). Digests are matched FIRST and returned unchanged.
_DIGEST_RE_SRC = r"\b(?:sha256|sha512|sha1|md5|blake2b):[0-9a-f]{32,128}\b"
_DIGEST_RE = re.compile(_DIGEST_RE_SRC)
_PAT_LIKE_RE = re.compile(
    _DIGEST_RE_SRC
    + r"|\b(?:glpat-[A-Za-z0-9_\-]+|[A-Za-z0-9_\-]{24,})\b"
)


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
    text = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}=<redacted>", text)
    text = _TOKEN_ASSIGNMENT_RE.sub(lambda m: f"{m.group(1)}=<redacted>", text)
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
