"""Find an unmasked credential in an evidence artifact.

Evidence artifacts are committed, and the repository is public. Agents have
captured command transcripts that carried a live Postgres password -- a
``--postgres-dsn postgresql://postgres:<password>@127.0.0.1:...`` line -- into
a receipt. Some writers already mask the password as ``******``; this scan
makes the rest impossible to record.

The scan is deliberately small: a handful of line patterns, no dependency, and a
short list of conventional placeholder passwords that fixtures, docs and
merged receipts on ``dev`` use (``postgres:okg@`` is the documented local
default, and test fixtures say ``u:p@``, ``user:password@`` and the like). A
match reports the line number and the KIND of credential, never the value, so
the refusal itself cannot leak what it found.

A file that is not UTF-8 text (a NUL byte, or a failed decode) is skipped:
the patterns are text patterns, and a binary artifact is out of scope.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

MASK_HINT = "mask the value as ****** (for example postgresql://postgres:******@host/db) and capture the artifact again"

# Values that are conventional placeholders, not secrets, compared
# case-insensitively. Measured on dev before this list was written: `okg` is
# the documented local password (2,400+ merged receipt lines say
# `postgres:okg@`), and the rest are what fixtures and docs spell a password
# that protects nothing.
PLACEHOLDERS = frozenset({
    "okg", "postgres", "password", "pass", "passwd", "pw", "pwd", "p",
    "secret", "test", "fixture", "example", "changeme", "redacted", "masked",
    "hunter2", "x", "xxx", "wrong", "none", "null", "str", "bytes",
})

# A value that labels itself as fake is a fixture: tests spell their sentinels
# `fake-sentinel-pw`, `Pw-synthetic-...`, `hunter2-not-real`.
FAKE_MARKERS = ("fake", "synthetic", "sentinel", "dummy", "not-real", "notreal", "placeholder")

# A JUnit test id is test SOURCE, already in the repository: a DSN-masking
# test is parametrized on fake passwords on purpose, and pytest writes the id
# into `name="..."` and `classname="..."`. In a JUnit file -- a `.xml` file
# that contains `<testcase` -- those attribute values are blanked before the
# scan; failure text and captured output are still scanned, which is where a
# real transcript would leak. No other file type is blanked.
_TEST_ID = re.compile(r"\b((?:class)?name)=\"[^\"]*\"")

KIND_URI = "uri_password"
KIND_PASSWORD = "password_assignment"
KIND_PASSWORD_KEY = "password_key"
KIND_BEARER = "authorization_bearer"
KIND_TOKEN = "access_token"
KIND_PRIVATE_KEY = "private_key"
BEARER_MIN = 8
TOKEN_MIN = 16

# scheme://user:password@ for the database and broker URIs agents paste into
# transcripts. The user may be empty (`redis://:pw@host`); the password stops
# at the first `@`, `/`, whitespace, quote, backslash or `]`. An empty
# password (`user:@`) carries nothing.
_URI = re.compile(
    r"(?:postgres(?:ql)?|mysql|mariadb|rediss?|amqps?|mongodb(?:\+srv)?)(?:\+[a-z0-9_]+)?"
    r"://[^:@/\s\"'`]*:([^@/\s\"'`\\\]]+)@",
    re.IGNORECASE,
)
# password=VALUE and PGPASSWORD=VALUE (also POSTGRES_PASSWORD=VALUE), as a
# libpq keyword, an env assignment or a shell export. The value must follow the
# `=` directly (an optional quote aside), so `password= host=x` reads as an
# empty value rather than as `host=x`; it stops at whitespace, a quote, a
# separator, a backslash (an escaped newline in a repr) or `]` (the end of a
# pytest id).
_ASSIGNMENT = re.compile(r"(?<![A-Za-z0-9])(?:PG)?PASSWORD=[\"']?([^\s\"'`&;,)\\\]]+)", re.IGNORECASE)
# A YAML or JSON key ending in `password`: `POSTGRES_PASSWORD: x`,
# `password: x`, `"password": "x"`. A list or object value (`[`, `{`), a type
# annotation's closing `)` and an all-digit value (a count under a key such as
# `"uri_password": 23`) are not a password.
_PASSWORD_KEY = re.compile(r"[A-Za-z0-9_]*PASSWORD[\"']?\s*:\s*[\"']?([^\s\"'`,;{})\[\\\]]+)", re.IGNORECASE)
# `Authorization: Bearer X` as a header, or `"Authorization": "Bearer X"` in
# JSON, quotes optional.
_BEARER = re.compile(r"Authorization[\"']?\s*:\s*[\"']?Bearer\s+([^\s\"'`,;\\\]]+)", re.IGNORECASE)
# Fixed-prefix tokens: GitHub (`ghp_`, `gho_`, `ghs_`, `github_pat_`) and
# Anthropic (`sk-ant-`). The value is the part after the prefix.
_TOKEN = re.compile(r"(?<![A-Za-z0-9_])(?:ghp_|gho_|ghs_|github_pat_|sk-ant-)([A-Za-z0-9_-]+)")
_PRIVATE_KEY = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")

_PATTERNS = (
    (KIND_URI, _URI),
    (KIND_PASSWORD, _ASSIGNMENT),
    (KIND_PASSWORD_KEY, _PASSWORD_KEY),
    (KIND_BEARER, _BEARER),
    (KIND_TOKEN, _TOKEN),
)


@dataclass(frozen=True)
class Finding:
    line: int
    kind: str


def _placeholder(value: str) -> bool:
    """Whether ``value`` is masked, a template, or a known placeholder."""
    if not value.strip("*"):
        return True
    # A template or a variable reference names where a secret comes from,
    # not the secret: ${PGPASSWORD}, $PW, {password}, <password>, %(pw)s.
    # A value that starts with `+` or carries a `(` is code that builds the
    # secret (`"..." + password`, `password=uuid.uuid4().hex`), not a value.
    if value[0] in "${<%+" or value.startswith("&lt") or "(" in value:
        return True
    lowered = value.lower()
    return lowered in PLACEHOLDERS or any(marker in lowered for marker in FAKE_MARKERS)


def _synthetic(value: str) -> bool:
    """A synthetic token: the body repeats a unit of at most four characters
    (`AAAA...`, `Ab1Ab1Ab1...`, `x1x1...`) or carries an alphabet or digit run
    (`abcdefgh`, `12345678`). A real token is random. Measured on dev: every
    `ghp_` / `github_pat_` value of 16+ characters under pact/ outside a JUnit
    test id is one of these repeated fixtures."""
    lowered = value.lower()
    if "abcdefgh" in lowered or "12345678" in lowered:
        return True
    for size in range(1, 5):
        unit = value[:size]
        if (unit * (len(value) // size + 1))[: len(value)] == value:
            return True
    return False


def _counts(kind: str, value: str) -> bool:
    # A bearer token is long; `Authorization: Bearer and ...` is prose.
    if kind == KIND_BEARER and len(value) < BEARER_MIN:
        return False
    if kind == KIND_TOKEN and (len(value) < TOKEN_MIN or _synthetic(value)):
        return False
    if kind == KIND_PASSWORD_KEY and value.isdigit():
        return False
    return not _placeholder(value)


def scan_text(text: str, *, junit: bool = False) -> list[Finding]:
    findings: list[Finding] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = _TEST_ID.sub(r'\1=""', raw) if junit else raw
        for kind, pattern in _PATTERNS:
            if any(_counts(kind, match.group(1)) for match in pattern.finditer(line)):
                findings.append(Finding(number, kind))
        if _PRIVATE_KEY.search(line):
            findings.append(Finding(number, KIND_PRIVATE_KEY))
    return findings


def is_junit(name: str | None, text: str) -> bool:
    return name is not None and name.lower().endswith(".xml") and "<testcase" in text


def scan_bytes(data: bytes, name: str | None = None) -> list[Finding]:
    """Findings in a file's bytes; empty for a binary or non-UTF-8 file.
    ``name`` (the file's path) decides whether JUnit test ids are blanked."""
    if b"\x00" in data:
        return []
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return []
    return scan_text(text, junit=is_junit(name, text))


def describe(finding: Finding) -> str:
    return f"line {finding.line} holds an unmasked {finding.kind.replace('_', ' ')}"
