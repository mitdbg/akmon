"""Hardening for outbound HTTP, shared and mechanism-only.

Issue #2100. This module knows nothing about live relations, OpenSearch or
the graph. It answers six questions, each of which was previously answered
twice in this repository and is now answered once:

1. Is this URL safe to dial at all -- scheme, address, port, and whether a
   non-global address is allowed?
2. What does this URL look like once its credentials are removed?
3. How much time is left in a total budget?
4. How do I read a response body without trusting its size?
5. How do I stop reading when the budget runs out?
6. How do I parse JSON from an untrusted source without accepting things
   that are not JSON?

`_opensearch_client.py` (#2014) solves 1-4 today with one defect this module
deliberately does NOT carry across: its `_body` falls back to
`response.json()` when the response object has no `iter_content`, which
silently removes the byte cap for a test double or any future session type
without streaming. :func:`read_bounded_body` RAISES instead. #2014 switches
to this module and deletes its copies.

**Refusals are `HttpGuardRefusal`, a plain `ValueError`, not a
`SubstrateError`.** A mechanism module has no business owning a wire error
code: the caller knows whether a bad address is a deployment-load refusal or
a runtime adapter failure, and names it accordingly. `reason` is a stable
token for the caller to map; `detail` is safe to show and never contains a
credential.
"""
from __future__ import annotations

import ipaddress
import json
import re
import time
from itertools import accumulate
from typing import Any, Iterable, Optional, Sequence, Union
from urllib.parse import urlsplit, urlunsplit

__all__ = [
    "ALLOWED_SCHEMES",
    "DEFAULT_PORTS",
    "HostPort",
    "HttpGuardRefusal",
    "IpAddress",
    "MAX_JSON_DEPTH",
    "assert_host_allowed",
    "canonical_address",
    "json_nesting_depth",
    "is_non_global",
    "parse_host_port",
    "parse_json_strict",
    "read_bounded_body",
    "redact_url",
    "redact_userinfo",
    "remaining_budget",
    "split_base_url",
]

IpAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

#: A canonical address and the ONE port it is allowed on.
HostPort = tuple[IpAddress, int]

#: The only two schemes anything in this repository dials.
ALLOWED_SCHEMES = ("http", "https")

DEFAULT_PORTS = {"http": 80, "https": 443}


class HttpGuardRefusal(ValueError):
    """One guard said no.

    `reason` is a stable token, so a caller maps it to its own named error
    without matching on prose. The full set:

    `scheme`, `userinfo`, `url_has_path`, `host_not_literal`,
    `ipv6_unbracketed`, `bad_port`, `host_not_allowed`, `non_global`,
    `streaming_unavailable`, `response_over_cap`, `deadline_exceeded`,
    `body_not_utf8`, `body_not_json`, `body_too_deep`.
    """

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


# ---------------------------------------------------------------- redaction

#: `scheme://user:password@host` anywhere in a string. Text-level, for the
#: case where a URL is embedded in a larger message rather than standing
#: alone -- an exception's `str()`, or an excerpt of an upstream error page.
_USERINFO_RE = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s@]*@")


def redact_userinfo(text: str) -> str:
    """Strip `user:password@` from every URL in a string."""
    return _USERINFO_RE.sub(lambda m: m.group("scheme") + "<redacted>@", text)


def redact_url(url: str) -> str:
    """Drop userinfo, query and fragment before a URL reaches a log or payload.

    Error text is the one place a credential leaks without anyone deciding to
    write it down. The query goes too: this repository sends no arguments in
    a query string, so a query on a URL being reported is either a mistake or
    somebody else's data, and neither belongs in a log.

    An unparsable URL returns a fixed marker rather than itself. Returning
    the input on a parse failure is how a malformed credential-bearing string
    reaches the log it was being redacted for.
    """
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return "<unparsable url>"
    if not parts.hostname:
        return "<unparsable url>"
    netloc = parts.hostname
    if ":" in netloc:  # a bare IPv6 literal must go back in its brackets
        netloc = f"[{netloc}]"
    try:
        port = parts.port
    except ValueError:
        port = None
    if port:
        netloc = f"{netloc}:{port}"
    if parts.username or parts.password:
        netloc = f"<redacted>@{netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


# ------------------------------------------------------------------ address

def canonical_address(host: str) -> IpAddress:
    """Parse an IP LITERAL strictly, or refuse.

    Strictness is the whole point, and `ipaddress.ip_address` on a string is
    already strict in the ways that matter. Each of these is a bypass someone
    has used against a host allowlist, and each RAISES here rather than being
    quietly normalised into something that compares equal to a different
    address than the socket will reach:

    - `0177.0.0.1`  -- octal; leading zeros are rejected outright
    - `2130706433`  -- decimal; a bare integer STRING is not an address
    - `127.0.0.1.`  -- trailing dot
    - `localhost`   -- a name, not a literal, and names are resolved by DNS
                       at connect time, which is the race this module exists
                       to avoid
    - `fe80::1%eth0` -- an IPv6 zone id; `ipaddress` accepts and KEEPS it,
                       but a zone selects an interface rather than naming an
                       address, so it would make one address compare as two

    Only one form is normalised rather than refused: an IPv4-mapped IPv6
    address (`::ffff:127.0.0.1`) is unmapped to its IPv4 address and judged
    AS that address, because it is that address -- the socket reaches
    127.0.0.1 -- and refusing it would leave the caller comparing two
    spellings of one host while `is_global` answered for the wrong one.
    """
    raw = str(host).strip()
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    if not raw:
        raise HttpGuardRefusal("host_not_literal", "the host is empty")
    if "%" in raw:
        # An IPv6 ZONE ID (`fe80::1%eth0`). `ipaddress` accepts it and keeps
        # it, so it would flow into the allowlist comparison as part of the
        # address -- and the zone is not part of the address, it is a choice
        # of INTERFACE. Two consequences, both bad: an allowlist entry
        # without a zone stops matching a base_url with one, and an entry
        # WITH a zone authorises reaching a link-local address out of a
        # specific NIC, which is a routing decision no host allowlist should
        # be able to express by accident.
        raise HttpGuardRefusal(
            "host_not_literal",
            "the host carries an IPv6 zone id (the `%` suffix). A zone "
            "selects a network INTERFACE rather than naming an address, so "
            "it is refused here: write the address alone",
        )
    try:
        addr: IpAddress = ipaddress.ip_address(raw)
    except ValueError:
        raise HttpGuardRefusal(
            "host_not_literal",
            f"{raw!r} is not an IP literal in canonical form. Hostnames, "
            f"octal and decimal spellings and trailing dots are all refused: "
            f"a name is resolved by DNS at connect time, which can answer "
            f"differently from the check",
        ) from None
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        return addr.ipv4_mapped
    return addr


def is_non_global(addr: IpAddress) -> bool:
    """True for an address that must not be dialled without an explicit opt-in.

    Loopback, private, link-local -- **including 169.254.169.254**, the cloud
    instance-metadata address, which is the single most valuable SSRF target
    there is -- multicast, reserved and unspecified.

    **`is_global` is NOT sufficient, and believing it was is how this nearly
    shipped wrong.** `ipaddress` reports `is_global is True` for every
    multicast address, v4 and v6: `224.0.0.1`, `239.255.255.250` and
    `ff02::1` all pass a `not is_global` check. It also reports `is_global`
    for the NAT64 well-known prefix `64:ff9b::/96`, whose whole purpose is to
    carry an embedded IPv4 destination -- `64:ff9b::7f00:1` reaches 127.0.0.1
    through a NAT64 gateway, which is loopback wearing a global-looking
    address. So every category is named explicitly and the predicate is a
    union, not a single flag. A test enumerates all of them.
    """
    return (
        not addr.is_global
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_private
    )


def parse_host_port(entry: str, *, default_port: int) -> HostPort:
    """Parse an allowlist entry into a canonical `(address, port)` pair.

    **Port semantics, which are exact and not permissive.** An entry is
    either `ip` or `ip:port`:

    - `127.0.0.1` means that address **on the scheme's default port only**.
      A bare address does NOT mean "any port". An allowlist whose entries
      quietly covered all 65,535 ports would authorise the admin interface
      next door to the one the operator meant.
    - `127.0.0.1:9200` means exactly that address on exactly that port.

    **An IPv6 address MUST be bracketed** -- `[::1]` or `[::1]:9200` -- and
    an unbracketed one is refused whether or not it appears to carry a port.
    The ambiguity is not resolvable by parsing: `::1:9200` is a valid IPv6
    literal meaning the address `::1:9200`, and it is also exactly what an
    operator writes when they mean `::1` on port 9200. Either guess hands
    somebody a host they did not ask for, silently, so neither is made.
    """
    raw = str(entry).strip()
    if not raw:
        raise HttpGuardRefusal("host_not_literal", "the entry is empty")
    port_text: Optional[str] = None
    if raw.startswith("["):
        host, sep, rest = raw.partition("]")
        host = host[1:]
        if not sep:
            raise HttpGuardRefusal(
                "host_not_literal", f"{raw!r} opens a bracket it never closes",
            )
        if rest:
            if not rest.startswith(":"):
                raise HttpGuardRefusal(
                    "bad_port", f"{raw!r} has trailing text after the address",
                )
            port_text = rest[1:]
    elif raw.count(":") > 1:
        # Unbracketed, more than one colon: an IPv6 address. It is REFUSED
        # whether or not a port looks present, because the ambiguity cannot
        # be resolved by parsing. `::1:9200` is a perfectly valid IPv6
        # literal meaning the address `::1:9200`, AND it is what an operator
        # writes when they mean `::1` on port 9200. Guessing either way gives
        # one of them a host they never meant, with no error. So brackets are
        # mandatory for IPv6 and the refusal says which spelling to use.
        raise HttpGuardRefusal(
            "ipv6_unbracketed",
            f"{raw!r} is an unbracketed IPv6 address. Brackets are required "
            f"so an address and a port cannot be confused: write "
            f"'[{raw}]' for the default port, or '[{raw}]:9200' for a port",
        )
    elif raw.count(":") == 1:
        host, _, port_text = raw.partition(":")
    else:
        host = raw
    addr = canonical_address(host)
    if port_text is None:
        return addr, default_port
    if not port_text.isdigit():
        raise HttpGuardRefusal("bad_port", f"{port_text!r} is not a port")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise HttpGuardRefusal("bad_port", f"port {port} is out of range")
    return addr, port


def _is_ipv6_literal(text: str) -> bool:
    try:
        ipaddress.IPv6Address(text)
    except ValueError:
        return False
    return True


def split_base_url(url: str) -> tuple[str, IpAddress, int]:
    """Validate a base URL and return `(scheme, address, port)`.

    A base URL contributes scheme, host and port and NOTHING else. A path,
    query or fragment is refused rather than dropped, so a base URL and the
    separately declared path can never disagree about where the request goes.

    An IPv6 host must be bracketed here too, which `urlsplit` already
    requires -- `http://::1:9200/` does not parse as a host with a port, and
    the refusal says so by name rather than reporting a mysterious address.
    """
    text = str(url)
    try:
        parts = urlsplit(text)
    except ValueError:
        raise HttpGuardRefusal(
            "host_not_literal", "the URL could not be parsed",
        ) from None
    scheme = (parts.scheme or "").lower()
    if scheme not in ALLOWED_SCHEMES:
        raise HttpGuardRefusal(
            "scheme",
            f"scheme {scheme or '(none)'!r} is not one of "
            f"{list(ALLOWED_SCHEMES)}",
        )
    if parts.username or parts.password:
        raise HttpGuardRefusal(
            "userinfo",
            "the URL embeds a credential before the '@'. Credentials come "
            "from a named environment variable, never from a URL, where they "
            "reach every log that ever prints it",
        )
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise HttpGuardRefusal(
            "url_has_path",
            f"{redact_url(text)} carries a path, query or fragment. A base "
            f"URL is scheme, host and port only",
        )
    if not parts.hostname:
        raise HttpGuardRefusal("host_not_literal", "the URL names no host")
    host = parts.hostname
    if ":" in host and not _is_ipv6_literal(host):
        raise HttpGuardRefusal(
            "ipv6_unbracketed",
            "the URL's host has colons but is not a valid IPv6 literal. An "
            "IPv6 host must be bracketed, as in 'http://[::1]:9200'",
        )
    addr = canonical_address(host)
    try:
        port = parts.port
    except ValueError:
        raise HttpGuardRefusal(
            "bad_port", "the URL's port is not a number in range",
        ) from None
    return scheme, addr, port or DEFAULT_PORTS[scheme]


def assert_host_allowed(
    addr: IpAddress,
    port: int,
    allowed: Sequence[HostPort],
    *,
    allow_non_global: bool,
) -> None:
    """Refuse unless `(addr, port)` is in the allowlist and may be dialled.

    Matching is on the CANONICAL pair, so every spelling of an address has
    already collapsed to one value before it gets here and the comparison
    cannot be fooled by notation. The port is part of the match: an operator
    who allowlisted `127.0.0.1:9200` did not allowlist `127.0.0.1:5432`.

    The non-global check comes SECOND and applies to the address actually
    being dialled, so a deployment cannot reach a private address merely by
    listing it -- it must also say `allow_non_global`, which is one visible
    line in the manifest a reviewer can find.
    """
    if not allowed:
        raise HttpGuardRefusal(
            "host_not_allowed", "the allowlist is empty, so nothing may be dialled",
        )
    if (addr, port) not in tuple(allowed):
        raise HttpGuardRefusal(
            "host_not_allowed",
            f"{_show(addr, port)} is not in the allowlist "
            f"({', '.join(_show(a, p) for a, p in allowed)})",
        )
    if is_non_global(addr) and not allow_non_global:
        raise HttpGuardRefusal(
            "non_global",
            f"{_show(addr, port)} is a loopback, private, link-local, "
            f"multicast, reserved or unspecified address. Set "
            f"`allow_non_global: true` on the binding to permit it, so the "
            f"permission is visible in the manifest",
        )


def _show(addr: IpAddress, port: int) -> str:
    text = str(addr)
    return f"[{text}]:{port}" if ":" in text else f"{text}:{port}"


# ------------------------------------------------------------------ budget

def remaining_budget(deadline: float, *, now: Optional[float] = None) -> float:
    """Seconds left before `deadline`, or refuse.

    `deadline` is a `time.monotonic()` value. Monotonic because a wall-clock
    budget can be lengthened or shortened by an NTP step in the middle of a
    read, and a deadline that moves is not a deadline.
    """
    left = float(deadline) - (time.monotonic() if now is None else now)
    if left <= 0:
        raise HttpGuardRefusal(
            "deadline_exceeded", "the time budget is spent",
        )
    return left


# -------------------------------------------------------------- bounded read

def read_bounded_body(
    response: Any,
    *,
    max_bytes: int,
    deadline: float,
    chunk_size: int = 65536,
) -> bytes:
    """Read a response body under a byte cap AND a total deadline.

    Two bounds, because either alone leaves a hole.

    **The byte cap counts DECODED bytes.** `iter_content` yields the body
    after `urllib3` has undone any `Content-Encoding`, so the running total
    is the decoded quantity -- which is the right one, because the cap exists
    to bound memory. Counting wire bytes would admit a compression bomb: a
    few hundred kilobytes on the wire is gigabytes in this process.

    **The deadline is checked between chunks, and closes the socket.** This
    is the slow-drip case, and it is not decoration. `requests`' read timeout
    is a per-read quiet period, not a total: a server sending one byte per
    second resets it forever. Meanwhile the caller's own deadline mechanism
    may ABANDON the thread this runs on, and an abandoned thread keeps its
    concurrency slot until its I/O returns -- so a drip that is never cut
    parks a thread and a socket permanently. Closing the response closes the
    socket, the generator stops, and the thread exits.

    **The bound is honest about what it is.** A between-chunk check only
    fires when a chunk ARRIVES. A server that sends headers and then nothing
    blocks inside the read until the per-read socket timeout expires, and
    nothing here can shorten that. The caller must therefore ALSO derive its
    per-read socket timeout from the same budget. The two compose to
    `min(remaining budget, per-read timeout)`; neither alone is a bound.

    **A response with no `iter_content` RAISES.** The module this is
    extracted from fell back to `response.json()`, which silently removed the
    byte cap for a test double or any session type without streaming. A cap
    that disappears when the object shape changes is not a cap.
    """
    iter_content = getattr(response, "iter_content", None)
    if not callable(iter_content):
        raise HttpGuardRefusal(
            "streaming_unavailable",
            "the response object cannot be streamed, so the byte cap cannot "
            "be enforced while reading. Refusing rather than reading an "
            "unbounded body",
        )
    chunks: list[bytes] = []
    total = 0
    stream: Iterable[bytes] = iter_content(chunk_size)
    try:
        for chunk in stream:
            # Deadline FIRST. A chunk that arrives after the budget is spent
            # is not admitted: checking the cap first would let a fast final
            # chunk decide the outcome of a call that had already timed out.
            if time.monotonic() >= deadline:
                raise HttpGuardRefusal(
                    "deadline_exceeded",
                    "the body did not arrive within the time budget",
                )
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                raise HttpGuardRefusal(
                    "response_over_cap",
                    f"the response exceeded {max_bytes} decoded bytes. This "
                    f"is a refusal, not a truncation: a half-read body is "
                    f"not a smaller answer",
                )
            chunks.append(chunk)
    finally:
        # Unconditional. Every exit from this function -- success, refusal,
        # or an exception from the socket -- closes the connection, because
        # the one path that matters is the one where we stopped reading
        # early and the server is still sending.
        close = getattr(response, "close", None)
        if callable(close):
            close()
    return b"".join(chunks)


# -------------------------------------------------------------- strict JSON

def _refuse_constant(name: str) -> Any:
    raise HttpGuardRefusal(
        "body_not_json",
        f"the body contains the JavaScript literal {name!r}, which is not "
        f"JSON. Python accepts it and re-emits it, so it would reach the "
        f"envelope and the stored sample as text no JSON reader can parse",
    )


def _refuse_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            raise HttpGuardRefusal(
                "body_not_json",
                f"the body repeats the object key {key!r}. Which value wins "
                f"is a parser choice, not a fact about the document, and a "
                f"reader that disagreed with this one would see different "
                f"data",
            )
        seen.add(key)
    return dict(pairs)


#: The deepest nesting of arrays and objects :func:`parse_json_strict` will
#: hand to `json.loads`. The byte cap bounds MEMORY and nothing else: twenty
#: thousand `[` is about 40 KB, far inside any byte cap, and `json.loads`
#: recurses once per level and raises `RecursionError` -- a `RuntimeError`,
#: which no `except ValueError` catches, so it used to escape the adapter as
#: an unexplained crash (issue #2170).
#:
#: 256 because it sits far below the interpreter's recursion limit (1,000 by
#: default, less whatever the calling thread's stack already holds) and far
#: above anything a real API returns. It bounds the WHOLE document; the
#: dispatcher's tighter depth cap (24) still applies to the rows returned.
MAX_JSON_DEPTH = 256

#: One JSON string literal, escapes included -- or, for an UNCLOSED string,
#: everything from its opening quote to the end of the body.
#:
#: LINEAR BY CONSTRUCTION, and the construction is the point. The first
#: version required a closing quote, so on a body of repeated `\\"` every
#: opening quote started a match that ran to the end of the body and failed,
#: and the next quote started another: quadratic, 21 s for 80 KB, and it held
#: the GIL, freezing the whole server. That body gzips to a few KB. Two rules
#: make that impossible now:
#:
#: - the match can never FAIL once it has seen a quote: an unclosed string
#:   simply runs to the end (`\\Z`), which is also exactly how far
#:   `json.loads` would read it before refusing it as unterminated, so no
#:   position is ever scanned twice;
#: - every repetition is ATOMIC (`(?>...)`, `*+`), so the engine cannot
#:   backtrack into a run it has already consumed.
#:
#: Each character is therefore examined a bounded number of times. A test
#: pins the timing on 8 MB of the adversarial body.
_JSON_STRING_RE = re.compile(
    r'"(?>[^"\\]*+)(?>\\.[^"\\]*+)*+(?:"|\\?\Z)', re.DOTALL,
)

#: Everything that is not a bracket, once the strings are gone.
_NOT_BRACKET_RE = re.compile(r"[^\[\]{}]+")

_BRACKET_STEP = {"[": 1, "{": 1, "]": -1, "}": -1}


def json_nesting_depth(text: str) -> int:
    """The deepest array/object nesting in `text`, measured WITHOUT parsing.

    Strings are removed first, so a `[` inside a string value does not
    count. The measurement is linear and non-recursive: the whole point is
    to answer before anything recursive sees the document. On a body that is
    not valid JSON the answer may be wrong in either direction, which is
    harmless -- `json.loads` then refuses it as `body_not_json`, and a
    `RecursionError` that slipped past is still caught and named below.
    """
    brackets = _NOT_BRACKET_RE.sub("", _JSON_STRING_RE.sub('""', text))
    if not brackets:
        return 0
    return max(0, max(accumulate(map(_BRACKET_STEP.__getitem__, brackets))))


def _too_deep(depth_text: str) -> HttpGuardRefusal:
    return HttpGuardRefusal(
        "body_too_deep",
        f"the body nests arrays or objects {depth_text} levels deep, over the "
        f"{MAX_JSON_DEPTH}-level limit. This is a refusal, not a truncation: "
        f"a document that deep is not parsed at all",
    )


def parse_json_strict(raw: bytes) -> Any:
    """Parse JSON from an untrusted source, accepting only real JSON.

    `json.loads` is permissive in four ways that matter when the document
    comes from a system this graph does not control and the result is put in
    an envelope an agent reads and a row a database stores:

    - **`NaN`, `Infinity`, `-Infinity` are accepted and RE-EMITTED.**
      `json.dumps` writes them back out verbatim, so the envelope and the
      persisted sample would carry text that no conforming JSON reader can
      parse. Refused by name.
    - **Duplicate object keys are silently collapsed, last one winning.**
      Which value survives is a parser choice; a different reader would see
      different data. Refused rather than documented, because "last wins" is
      a rule nobody checks.
    - **A very long integer literal raises `ValueError`** from the digit
      limit added in Python 3.11, which is a denial-of-service guard doing
      its job and must read as a refusal, not a crash.
    - **A non-UTF-8 body, or one carrying a byte-order mark**, is not JSON
      either. UTF-8 is decoded strictly; a BOM is named rather than stripped,
      because a stripped BOM is a body that parsed differently from the bytes
      that arrived.
    - **Nesting deeper than :data:`MAX_JSON_DEPTH`** is refused as
      `body_too_deep` BEFORE parsing, because `json.loads` recurses once per
      level and a nesting bomb small enough to pass the byte cap raises
      `RecursionError`, which is not a `ValueError` and so escaped every
      clause below as an unexplained crash (#2170).
    """
    if raw.startswith(b"\xef\xbb\xbf"):
        raise HttpGuardRefusal(
            "body_not_json",
            "the body begins with a UTF-8 byte-order mark. JSON has no BOM; "
            "stripping it would mean parsing something other than the bytes "
            "that arrived",
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HttpGuardRefusal(
            "body_not_utf8",
            "the body is not valid UTF-8, so it is not JSON",
        ) from None
    depth = json_nesting_depth(text)
    if depth > MAX_JSON_DEPTH:
        raise _too_deep(str(depth))
    try:
        return json.loads(
            text,
            parse_constant=_refuse_constant,
            object_pairs_hook=_refuse_duplicate_keys,
        )
    except HttpGuardRefusal:
        raise
    except RecursionError:
        # The pre-scan above should make this unreachable. It is caught by
        # name anyway, and named as the same refusal, because "should be
        # unreachable" is a belief and the alternative is an unexplained
        # crash. The calling thread's own stack depth is the one thing the
        # pre-scan cannot see.
        raise _too_deep("too many") from None
    except json.JSONDecodeError as exc:
        raise HttpGuardRefusal(
            "body_not_json", f"the body is not JSON: {exc.msg} at line "
            f"{exc.lineno} column {exc.colno}",
        ) from None
    except ValueError as exc:
        # The integer digit limit (Python 3.11+) raises a bare ValueError.
        # Naming the type only: the message can embed the offending literal.
        raise HttpGuardRefusal(
            "body_not_json",
            f"the body could not be parsed ({type(exc).__name__}); a number "
            f"longer than the interpreter's digit limit is the usual cause",
        ) from None
