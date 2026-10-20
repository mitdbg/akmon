"""The shared outbound-HTTP guard, in isolation.

Issue #2100. Every row here is a refusal that has a name, because the thing
this module exists to prevent is a request that LOOKS checked and is not:
an address spelled so that the check and the socket disagree, a byte cap
that disappears when the response object has a different shape, a deadline
that only fires if the server keeps sending, or a body that parses here and
is unreadable everywhere else.

Hermetic: no sockets, no database, no fixtures beyond the stubs below.
"""
from __future__ import annotations

import ipaddress
import json
import time

import pytest

from okg.substrate.external_backed._http_guard import (
    MAX_JSON_DEPTH,
    HttpGuardRefusal,
    assert_host_allowed,
    canonical_address,
    is_non_global,
    json_nesting_depth,
    parse_host_port,
    parse_json_strict,
    read_bounded_body,
    redact_url,
    redact_userinfo,
    remaining_budget,
    split_base_url,
)


class _Response:
    """The shape `requests` gives back, reduced to what the guard touches."""

    def __init__(self, chunks, *, delays=None):
        self._chunks = list(chunks)
        self._delays = list(delays or [])
        self.closed = False
        self.consumed = 0

    def iter_content(self, chunk_size=65536):
        for index, chunk in enumerate(self._chunks):
            if index < len(self._delays):
                time.sleep(self._delays[index])
            self.consumed += 1
            yield chunk

    def close(self):
        self.closed = True


class _UnstreamableResponse:
    """A response object with no `iter_content` -- a stub, or a future session.

    This is the exact shape that made `_opensearch_client._body` fall back to
    `.json()` and read an unbounded body.
    """

    def __init__(self, body):
        self._body = body
        self.closed = False

    def json(self):  # pragma: no cover - must never be reached
        raise AssertionError("the guard called .json() instead of refusing")

    def close(self):
        self.closed = True


# --------------------------------------------------------------- addresses

@pytest.mark.parametrize(
    "spelling",
    [
        "0177.0.0.1",     # octal
        "2130706433",     # decimal
        "127.0.0.1.",     # trailing dot
        "127.0.0.01",     # leading zero in a part
        "localhost",      # a name: resolved by DNS at connect time
        "example.com",
        "127.0.0.1:9200", # a host:port pair is not an address
        "",
        "   ",
    ],
)
def test_canonical_address_refuses_every_non_literal_spelling(spelling):
    """Each of these is a real allowlist bypass, and each must RAISE.

    Not "be normalised": a spelling that quietly became some address is how
    a check and a socket end up disagreeing about where a request went.
    """
    with pytest.raises(HttpGuardRefusal) as caught:
        canonical_address(spelling)
    assert caught.value.reason == "host_not_literal"


@pytest.mark.parametrize(
    ("spelling", "expected"),
    [
        ("127.0.0.1", "127.0.0.1"),
        ("::ffff:127.0.0.1", "127.0.0.1"),   # IPv4-mapped is unmapped
        ("[::ffff:127.0.0.1]", "127.0.0.1"),
        ("::1", "::1"),
        ("[::1]", "::1"),
        ("10.0.0.1", "10.0.0.1"),
        ("8.8.8.8", "8.8.8.8"),
    ],
)
def test_canonical_address_collapses_the_spellings_that_are_one_address(
    spelling, expected,
):
    assert canonical_address(spelling) == ipaddress.ip_address(expected)


def test_ipv4_mapped_is_judged_as_the_ipv4_address_it_is():
    """`::ffff:10.0.0.1` is private, and `is_global` on the IPv6 form lies.

    Unmapping first is what makes the non-global check answer about the
    address the socket will actually reach.
    """
    assert is_non_global(canonical_address("::ffff:10.0.0.1")) is True
    assert is_non_global(canonical_address("::ffff:8.8.8.8")) is False


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",          # loopback
        "::1",
        "10.0.0.1",           # private
        "192.168.1.1",
        "172.16.0.1",
        "169.254.169.254",    # cloud instance metadata
        "fe80::1",            # link-local
        "224.0.0.1",          # multicast -- is_global says True
        "239.255.255.250",    # SSDP multicast
        "ff02::1",            # IPv6 all-nodes multicast
        "0.0.0.0",            # unspecified
        "::",
        "fc00::1",            # unique local
        "240.0.0.1",          # reserved
        "255.255.255.255",    # broadcast
        "100.64.0.1",         # carrier-grade NAT
        "64:ff9b::7f00:1",    # NAT64 -- reaches 127.0.0.1 through a gateway
    ],
)
def test_is_non_global_catches_every_address_that_needs_an_opt_in(address):
    assert is_non_global(canonical_address(address)) is True


@pytest.mark.parametrize(
    "address", ["224.0.0.1", "239.255.255.250", "ff02::1", "64:ff9b::7f00:1"],
)
def test_is_global_alone_would_have_let_these_through(address):
    """The premise of the row above, pinned as a fact about the library.

    `ipaddress` reports `is_global is True` for every multicast address and
    for the NAT64 well-known prefix. A guard written as `not addr.is_global`
    -- which is how this module was first drafted, and how the design
    document described it -- would have dialled all four without the opt-in.
    If a future Python fixes this, this row fails and the comment above can
    be simplified; it must not be deleted quietly.
    """
    assert ipaddress.ip_address(address).is_global is True


def test_instance_metadata_address_is_non_global():
    """Named on its own because it is the SSRF target that pays.

    169.254.169.254 hands out cloud credentials to anything that can make an
    unauthenticated GET from inside the network.
    """
    assert is_non_global(canonical_address("169.254.169.254")) is True


# ------------------------------------------------------------ port semantics

def test_bare_entry_means_the_default_port_not_any_port():
    """A bare address does NOT authorise all 65,535 ports.

    An allowlist that silently covered every port would authorise the admin
    interface next to the one the operator meant.
    """
    assert parse_host_port("127.0.0.1", default_port=80) == (
        ipaddress.ip_address("127.0.0.1"), 80,
    )
    assert parse_host_port("127.0.0.1", default_port=443) == (
        ipaddress.ip_address("127.0.0.1"), 443,
    )


def test_entry_with_a_port_means_exactly_that_port():
    assert parse_host_port("127.0.0.1:9200", default_port=80) == (
        ipaddress.ip_address("127.0.0.1"), 9200,
    )


def test_ipv6_with_a_port_must_be_bracketed():
    assert parse_host_port("[::1]:9200", default_port=80) == (
        ipaddress.ip_address("::1"), 9200,
    )
    assert parse_host_port("[::1]", default_port=9200) == (
        ipaddress.ip_address("::1"), 9200,
    )


@pytest.mark.parametrize("entry", ["::1", "::1:9200", "fe80::1",
                                   "2001:db8::1:443"])
def test_unbracketed_ipv6_is_refused_by_name(entry):
    """The ambiguity cannot be resolved by parsing, so neither guess is made.

    `::1:9200` is a valid IPv6 literal meaning the address `::1:9200`, and it
    is also what an operator writes meaning `::1` on port 9200. Reading it
    either way hands somebody a host they never asked for, with no error, so
    brackets are mandatory and the refusal names the spelling to use.
    """
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_host_port(entry, default_port=80)
    assert caught.value.reason == "ipv6_unbracketed"
    assert "[" in caught.value.detail


def test_the_ambiguous_ipv6_spelling_really_is_a_valid_address():
    """The premise of the row above: this is why guessing is not an option."""
    assert ipaddress.ip_address("::1:9200") == ipaddress.IPv6Address("::1:9200")
    assert ipaddress.ip_address("::1:9200") != ipaddress.IPv6Address("::1")


@pytest.mark.parametrize("entry", ["127.0.0.1:0", "127.0.0.1:70000",
                                   "127.0.0.1:http", "[::1]9200"])
def test_bad_ports_are_refused(entry):
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_host_port(entry, default_port=80)
    assert caught.value.reason in ("bad_port", "host_not_literal")


def test_port_mismatch_is_not_allowed():
    """9200 allowlisted does not mean 5432 allowlisted."""
    allowed = [parse_host_port("127.0.0.1:9200", default_port=80)]
    assert_host_allowed(
        canonical_address("127.0.0.1"), 9200, allowed, allow_non_global=True,
    )
    with pytest.raises(HttpGuardRefusal) as caught:
        assert_host_allowed(
            canonical_address("127.0.0.1"), 5432, allowed,
            allow_non_global=True,
        )
    assert caught.value.reason == "host_not_allowed"


def test_empty_allowlist_allows_nothing():
    with pytest.raises(HttpGuardRefusal) as caught:
        assert_host_allowed(
            canonical_address("8.8.8.8"), 80, [], allow_non_global=False,
        )
    assert caught.value.reason == "host_not_allowed"


def test_listing_a_private_address_is_not_enough_without_the_opt_in():
    """The allowlist and the opt-in are two separate decisions.

    Listing the address says where; `allow_non_global` says that reaching
    inside the network is intended. A deployment must do both, so the
    permission is one visible line a reviewer can find.
    """
    allowed = [parse_host_port("127.0.0.1:9200", default_port=80)]
    with pytest.raises(HttpGuardRefusal) as caught:
        assert_host_allowed(
            canonical_address("127.0.0.1"), 9200, allowed,
            allow_non_global=False,
        )
    assert caught.value.reason == "non_global"
    assert_host_allowed(
        canonical_address("127.0.0.1"), 9200, allowed, allow_non_global=True,
    )


def test_a_mapped_spelling_matches_the_allowlisted_ipv4_entry():
    """Canonicalisation happens on BOTH sides, so notation cannot bypass."""
    allowed = [parse_host_port("127.0.0.1:9200", default_port=80)]
    assert_host_allowed(
        canonical_address("::ffff:127.0.0.1"), 9200, allowed,
        allow_non_global=True,
    )


# ---------------------------------------------------------------- base URLs

def test_split_base_url_returns_scheme_address_and_port():
    assert split_base_url("http://127.0.0.1:9200") == (
        "http", ipaddress.ip_address("127.0.0.1"), 9200,
    )
    assert split_base_url("https://8.8.8.8") == (
        "https", ipaddress.ip_address("8.8.8.8"), 443,
    )
    assert split_base_url("http://[::1]:9200") == (
        "http", ipaddress.ip_address("::1"), 9200,
    )


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("ftp://127.0.0.1", "scheme"),
        ("file:///etc/passwd", "scheme"),
        ("gopher://127.0.0.1:70", "scheme"),
        ("127.0.0.1:9200", "scheme"),
        ("http://user:pass@127.0.0.1:9200", "userinfo"),
        ("http://127.0.0.1:9200/logs-*/_search", "url_has_path"),
        ("http://127.0.0.1:9200/?q=1", "url_has_path"),
        ("http://127.0.0.1:9200#frag", "url_has_path"),
        ("http://localhost:9200", "host_not_literal"),
        ("http://0177.0.0.1:9200", "host_not_literal"),
    ],
)
def test_split_base_url_refuses_by_name(url, reason):
    with pytest.raises(HttpGuardRefusal) as caught:
        split_base_url(url)
    assert caught.value.reason == reason


def test_a_base_url_refusal_never_echoes_the_credential():
    """The refusal text for a path error must not carry the userinfo.

    The path check runs after the userinfo check, but the message it builds
    formats the URL -- so it goes through `redact_url` first.
    """
    with pytest.raises(HttpGuardRefusal) as caught:
        split_base_url("http://127.0.0.1:9200/some/path?token=sekrit")
    assert "sekrit" not in str(caught.value)


# --------------------------------------------------------------- redaction

@pytest.mark.parametrize(
    ("url", "absent"),
    [
        ("http://user:pass@127.0.0.1:9200/x", "pass"),
        ("https://token@example.com/y", "token"),
        ("https://u:p@[::1]:9200/z", "p@"),
    ],
)
def test_redact_url_drops_the_credential(url, absent):
    out = redact_url(url)
    assert absent not in out
    assert "<redacted>@" in out


def test_redact_url_drops_the_query_too():
    assert "sekrit" not in redact_url("http://127.0.0.1:9200/a?token=sekrit")


def test_redact_url_returns_a_marker_for_an_unparsable_url():
    """Returning the input on a parse failure leaks the thing being redacted."""
    assert redact_url("http://[oops") == "<unparsable url>"
    assert redact_url("not a url at all") == "<unparsable url>"


def test_redact_userinfo_works_on_text_around_a_url():
    text = "GET https://admin:hunter2@10.0.0.1/_search returned 500"
    out = redact_userinfo(text)
    assert "hunter2" not in out
    assert "<redacted>@10.0.0.1" in out


# ----------------------------------------------------------------- budget

def test_remaining_budget_counts_down_and_then_refuses():
    deadline = time.monotonic() + 100
    assert 99 < remaining_budget(deadline) <= 100
    with pytest.raises(HttpGuardRefusal) as caught:
        remaining_budget(time.monotonic() - 1)
    assert caught.value.reason == "deadline_exceeded"


# ------------------------------------------------------------ bounded read

def test_bounded_read_returns_the_body_and_closes():
    response = _Response([b"{", b'"a": 1', b"}"])
    out = read_bounded_body(
        response, max_bytes=1024, deadline=time.monotonic() + 100,
    )
    assert out == b'{"a": 1}'
    assert response.closed is True


def test_bounded_read_refuses_and_stops_reading_at_the_cap():
    """The point is that it stopped EARLY, not merely that it refused.

    A guard that read everything and then complained would have already spent
    the memory the cap exists to bound.
    """
    response = _Response([b"x" * 100] * 50)
    with pytest.raises(HttpGuardRefusal) as caught:
        read_bounded_body(
            response, max_bytes=250, deadline=time.monotonic() + 100,
            chunk_size=100,
        )
    assert caught.value.reason == "response_over_cap"
    assert response.consumed == 3      # not 50
    assert response.closed is True


def test_bounded_read_cuts_a_slow_drip_between_chunks_and_closes_the_socket():
    """The slow-loris row. `requests`' read timeout resets on every chunk.

    Without the between-chunk check this read never ends, and the thread it
    runs on holds a live-call concurrency slot until the process dies.
    """
    response = _Response([b"x"] * 100, delays=[0.02] * 100)
    started = time.monotonic()
    with pytest.raises(HttpGuardRefusal) as caught:
        read_bounded_body(
            response, max_bytes=10_000, deadline=time.monotonic() + 0.1,
            chunk_size=1,
        )
    assert caught.value.reason == "deadline_exceeded"
    assert time.monotonic() - started < 1.0
    assert response.consumed < 100
    assert response.closed is True


def test_bounded_read_raises_rather_than_falling_back_to_json():
    """The defect NOT carried across from `_opensearch_client._body`.

    A response object without `iter_content` used to reach `.json()`, which
    reads the whole body with no cap. The stub's `.json()` asserts if it is
    ever called, so a reintroduced fallback reds this row twice over.
    """
    response = _UnstreamableResponse({"a": 1})
    with pytest.raises(HttpGuardRefusal) as caught:
        read_bounded_body(
            response, max_bytes=1024, deadline=time.monotonic() + 100,
        )
    assert caught.value.reason == "streaming_unavailable"


def test_bounded_read_closes_even_when_the_stream_itself_raises():
    class _Exploding:
        def __init__(self):
            self.closed = False

        def iter_content(self, chunk_size=65536):
            yield b"partial"
            raise OSError("connection reset")

        def close(self):
            self.closed = True

    response = _Exploding()
    with pytest.raises(OSError):
        read_bounded_body(
            response, max_bytes=1024, deadline=time.monotonic() + 100,
        )
    assert response.closed is True


# ------------------------------------------------------------- strict JSON

def test_strict_json_parses_ordinary_json():
    assert parse_json_strict(b'{"hits": {"hits": [1, 2]}}') == {
        "hits": {"hits": [1, 2]},
    }


@pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
def test_strict_json_refuses_the_javascript_constants(literal):
    """`json.loads` accepts these and `json.dumps` WRITES THEM BACK.

    The envelope an agent reads and the sample row a database stores would
    then contain text no conforming JSON reader can parse -- produced by this
    process, from data an external system chose.
    """
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b'{"score": ' + literal + b"}")
    assert caught.value.reason == "body_not_json"


def test_the_standard_library_really_does_round_trip_nan():
    """The premise of the row above, pinned rather than asserted in prose."""
    assert json.dumps(json.loads('{"a": NaN}')) == '{"a": NaN}'


def test_strict_json_refuses_duplicate_object_keys():
    """Which value wins is a parser choice, not a fact about the document."""
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b'{"total": 1, "total": 99}')
    assert caught.value.reason == "body_not_json"
    assert "total" in caught.value.detail


def test_strict_json_refuses_a_non_utf8_body():
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b'{"a": "\xff\xfe"}')
    assert caught.value.reason == "body_not_utf8"


def test_strict_json_refuses_a_byte_order_mark():
    """Stripping it would mean parsing something other than what arrived."""
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b'\xef\xbb\xbf{"a": 1}')
    assert caught.value.reason == "body_not_json"


def test_strict_json_refuses_an_integer_over_the_digit_limit():
    """Python 3.11's digit limit raises a bare ValueError, not a JSON error.

    Unhandled, that reaches the dispatcher's backstop as an unexpected
    exception type. It is a refusal, and it must read like one.
    """
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b'{"n": ' + b"9" * 5000 + b"}")
    assert caught.value.reason == "body_not_json"


def test_strict_json_refuses_plain_garbage():
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b"<html><body>502 Bad Gateway</body></html>")
    assert caught.value.reason == "body_not_json"


def test_strict_json_refusal_does_not_echo_the_body():
    """An error path is the least-inspected path; it must not become an echo."""
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b'{"token": "sekrit-value-here" ')
    assert "sekrit-value-here" not in str(caught.value)


# ------------------------------------------------------ IPv6 zone ids (N4)

@pytest.mark.parametrize(
    "host",
    [
        pytest.param("fe80::1%eth0", id="link-local-with-zone"),
        pytest.param("2606:4700::1%eth0", id="global-with-zone"),
        pytest.param("fe80::1%25eth0", id="percent-encoded-zone"),
        pytest.param("[fe80::1%eth0]", id="bracketed-with-zone"),
    ],
)
def test_an_ipv6_zone_id_is_refused_by_name(host):
    """A zone selects an INTERFACE; it does not name an address.

    `ipaddress.ip_address` accepts a zone id and KEEPS it, so before this
    refusal the suffix flowed into the allowlist comparison as if it were
    part of the address. Two ways that goes wrong, and both are silent:
    an allowlist entry without a zone stops matching a `base_url` with one,
    and an entry WITH a zone authorises reaching a link-local address out of
    one specific NIC -- a routing decision no host allowlist should be able
    to express by accident.
    """
    with pytest.raises(HttpGuardRefusal) as caught:
        canonical_address(host)
    assert caught.value.reason == "host_not_literal"
    assert "zone id" in caught.value.detail


def test_an_ipv6_address_without_a_zone_still_parses():
    """The refusal above is about the `%` suffix, not about IPv6."""
    assert str(canonical_address("fe80::1")) == "fe80::1"
    assert str(canonical_address("[2606:4700::1]")) == "2606:4700::1"


def test_a_zone_id_cannot_smuggle_a_host_past_an_allowlist():
    """The end-to-end shape of N4, through the parser the config uses."""
    with pytest.raises(HttpGuardRefusal):
        parse_host_port("fe80::1%eth0", default_port=80)


# ------------------------------------------------- parse depth (#2170)

def _nested(levels: int, *, open_: bytes = b"[", close: bytes = b"]") -> bytes:
    return open_ * levels + close * levels


def test_strict_json_refuses_a_nesting_bomb_under_any_byte_cap_by_name():
    """#2170: 20,000 levels is about 40 KB and used to raise RecursionError.

    `RecursionError` is a `RuntimeError`, so it passed every clause in the
    parser and reached the dispatcher's backstop as an unexplained crash.
    It must be a named refusal instead.
    """
    body = _nested(20_000)
    assert len(body) == 40_000
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(body)
    assert caught.value.reason == "body_too_deep"
    assert "20000" in caught.value.detail


def test_strict_json_nesting_limit_is_exact():
    """At the limit parses; one level past it is refused. Objects count too."""
    assert parse_json_strict(_nested(MAX_JSON_DEPTH)) is not None
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(_nested(MAX_JSON_DEPTH + 1))
    assert caught.value.reason == "body_too_deep"
    deep_objects = b'{"a":' * (MAX_JSON_DEPTH + 1) + b"1" + b"}" * (
        MAX_JSON_DEPTH + 1
    )
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(deep_objects)
    assert caught.value.reason == "body_too_deep"


def test_brackets_inside_strings_do_not_count_toward_depth():
    """A string value full of `[` is shallow, escaped quotes included."""
    text = '{"q": "' + "[" * 10_000 + '\\"[[[", "r": [[1]]}'
    assert json_nesting_depth(text) == 3
    parsed = parse_json_strict(text.encode("utf-8"))
    assert parsed["r"] == [[1]]


def test_a_recursion_error_that_slips_past_the_prescan_is_still_named(
    monkeypatch,
):
    """The belt under the braces: the parser catches RecursionError by name.

    The pre-scan cannot see how deep the CALLING thread's stack already is,
    so the parse itself can still recurse out. That must read as the same
    refusal, never as an unexplained crash.
    """
    def exploding_loads(*_a, **_k):
        raise RecursionError("maximum recursion depth exceeded")

    # The guard calls `json.loads` through the `json` module, the same one
    # imported here, so patching it here patches the guard's call.
    monkeypatch.setattr(json, "loads", exploding_loads)
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(b"[1]")
    assert caught.value.reason == "body_too_deep"
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True


#: Repeated `\\"`: every quote opens a string that never closes. The first
#: depth pre-scan required a closing quote, so each opening quote started a
#: match that ran to the end of the body and failed -- quadratic: 10 KB took
#: 0.34 s, 80 KB 24.7 s, and 8 MB would take days, holding the GIL (a frozen
#: server) the whole time. Gzipped, the 8 MB body is about 8 KB on the wire.
_QUOTE_BOMB_UNIT = b'\\"'


@pytest.mark.parametrize(
    "size",
    [
        # Small enough that the QUADRATIC scan fails in about ten seconds
        # rather than days, so this row can be shown red on the old code.
        pytest.param(64 * 1024, id="64KiB"),
        # The largest body the adapter admits (the 8 MiB default cap). The
        # linear scan takes about 0.1 s here; 2 s leaves room for a slow CI
        # runner without admitting anything super-linear.
        pytest.param(8 * 1024 * 1024 - 16, id="8MiB"),
    ],
)
@pytest.mark.parametrize("gzipped", [False, True], ids=["plain", "gzip"])
def test_the_depth_prescan_is_linear_on_an_unclosed_quote_bomb(size, gzipped):
    """#2328 review: the pre-scan must never cost more than the body's size.

    The gzip variant is how the body arrives: a few KB on the wire, the full
    size after the transport decodes it -- which is what reaches the parser.
    """
    import gzip

    body = _QUOTE_BOMB_UNIT * (size // len(_QUOTE_BOMB_UNIT))
    if gzipped:
        wire = gzip.compress(body)
        assert len(wire) < size // 100, "the point is a tiny wire size"
        body = gzip.decompress(wire)
    started = time.perf_counter()
    with pytest.raises(HttpGuardRefusal) as caught:
        parse_json_strict(body)
    elapsed = time.perf_counter() - started
    assert caught.value.reason == "body_not_json"
    assert elapsed < 2.0, f"{len(body)} bytes took {elapsed:.2f}s"


@pytest.mark.parametrize(
    "text, depth",
    [
        pytest.param('["a\\"b", [[1]]]', 3, id="escaped-quote-closed"),
        # An unclosed string runs to the end, exactly as far as json.loads
        # would read it before refusing it: brackets after it do not count.
        pytest.param('[[ "abc [[[[', 2, id="unclosed-string-to-end"),
        pytest.param('[ "abc\\', 1, id="trailing-backslash"),
        pytest.param('["\\\\", [[1]]]', 3, id="escaped-backslash-then-close"),
    ],
)
def test_the_depth_prescan_reads_strings_the_way_json_does(text, depth):
    assert json_nesting_depth(text) == depth
