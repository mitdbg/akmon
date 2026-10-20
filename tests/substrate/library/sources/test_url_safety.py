"""SSRF guard tests. Per `validate-source-fetch-urls` and
`centralize-egress-url-validation`."""
from __future__ import annotations

from unittest import mock

import pytest

from okg.substrate.library.sources._url_safety import (
    EGRESS_ALLOW_HOSTS_ENV,
    UnsafeFetchURL,
    assert_public_http_url,
    assert_same_origin,
    assert_token_request_url,
    validate_redirect_target,
)


def _resolves_to(ip: str):
    """Patch getaddrinfo so the host 'maps' to `ip` without real DNS."""
    return mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        return_value=[(2, 1, 6, "", (ip, 80))],
    )


# ---- scheme rejection (no DNS needed) ----


@pytest.mark.parametrize("url", [
    "file:///etc/passwd",
    "ftp://example.com/x",
    "gopher://example.com/",
    "data:text/plain;base64,AAAA",
    "//example.com/x",        # scheme-relative -> no scheme
    "example.com/x",          # bare host -> no scheme
    "",
])
def test_rejects_non_http_schemes(url: str) -> None:
    with pytest.raises(UnsafeFetchURL):
        assert_public_http_url(url)


# ---- private / metadata / loopback IPs (SSRF) ----


@pytest.mark.parametrize("ip", [
    "169.254.169.254",   # cloud metadata
    "127.0.0.1",         # loopback
    "10.0.0.5",          # private
    "172.16.0.9",        # private
    "192.168.1.1",       # private
    "0.0.0.0",           # unspecified
    "::1",               # ipv6 loopback
    "fd00::1",           # ipv6 ULA (private)
])
def test_rejects_non_public_addresses(ip: str) -> None:
    with _resolves_to(ip):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://malicious.example/x")


def test_rejects_when_any_resolved_address_is_private() -> None:
    """A split answer with one public + one private IP is rejected."""
    with mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        return_value=[
            (2, 1, 6, "", ("93.184.216.34", 80)),   # public
            (2, 1, 6, "", ("169.254.169.254", 80)),  # metadata
        ],
    ):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://rebind.example/x")


def test_allows_public_https_host() -> None:
    with _resolves_to("93.184.216.34"):  # example.com, public
        assert_public_http_url("https://example.com/people")  # no raise


def test_unresolvable_host_is_rejected() -> None:
    import socket as _s
    with mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        side_effect=_s.gaierror("name resolution failed"),
    ):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://nonexistent.invalid/x")


# ---- redirect / pagination / delta revalidation ----


def test_redirect_target_to_metadata_is_rejected() -> None:
    with _resolves_to("169.254.169.254"):
        with pytest.raises(UnsafeFetchURL):
            validate_redirect_target(
                "http://metadata.example/latest/meta-data",
                base_url="https://api.example.com/page1",
            )


def test_relative_redirect_is_resolved_then_validated() -> None:
    with _resolves_to("93.184.216.34"):
        target = validate_redirect_target(
            "/page2", base_url="https://api.example.com/page1",
        )
    assert target == "https://api.example.com/page2"


# ---- same-origin token binding ----


def test_same_origin_rejects_cross_origin_followup() -> None:
    with pytest.raises(UnsafeFetchURL):
        assert_same_origin(
            "https://attacker.example/steal", "api.example.com",
        )


def test_same_origin_accepts_same_host() -> None:
    assert_same_origin("https://api.example.com/next", "api.example.com")


def test_same_origin_accepts_leading_dot_subdomain() -> None:
    # A provider may explicitly allow sibling hosts via a leading-dot pattern.
    assert_same_origin(
        "https://content.googleapis.com/dl", [".googleapis.com"],
    )


def test_token_request_url_blocks_private_same_host() -> None:
    # Even a same-origin host is refused if it resolves to a private address.
    with _resolves_to("10.0.0.5"):
        with pytest.raises(UnsafeFetchURL):
            assert_token_request_url(
                "https://api.example.com/x", "api.example.com",
            )


# ---- operator exception allowlist (explicit, surfaced) ----


def test_private_host_rejected_without_allowlist() -> None:
    with _resolves_to("10.0.0.5"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://internal.svc/x")


def test_private_host_permitted_only_with_allowlist_arg() -> None:
    with _resolves_to("10.0.0.5"):
        # allow_hosts argument permits the otherwise-private host.
        assert_public_http_url(
            "http://internal.svc/x", allow_hosts=["internal.svc"],
        )


def test_private_host_permitted_via_env_allowlist(monkeypatch) -> None:
    monkeypatch.setenv(EGRESS_ALLOW_HOSTS_ENV, "internal.svc, other.host")
    with _resolves_to("10.0.0.5"):
        assert_public_http_url("http://internal.svc/x")  # no raise
    # A host NOT on the allowlist is still rejected.
    with _resolves_to("10.0.0.5"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://unlisted.svc/x")


# An IPv6 literal on the allowlist must match the URL host however the operator
# wrote it: bare, bracketed, bracketed with a port, or as a full origin.
@pytest.mark.parametrize("pattern", [
    "::1",
    "[::1]",
    "[::1]:8080",
    "http://[::1]:8080",
    "0:0:0:0:0:0:0:1",
])
@pytest.mark.parametrize("url", [
    "http://[::1]/x",
    "http://[::1]:8080/x",
])
def test_ipv6_literal_permitted_via_allowlist(pattern: str, url: str) -> None:
    with _resolves_to("::1"):
        assert_public_http_url(url, allow_hosts=[pattern])


def test_ipv6_literal_permitted_via_env_allowlist(monkeypatch) -> None:
    monkeypatch.setenv(EGRESS_ALLOW_HOSTS_ENV, "[fd00::5]:9000, internal.svc")
    with _resolves_to("fd00::5"):
        assert_public_http_url("http://[fd00::5]:9000/x")  # no raise
    # A different IPv6 address is still rejected.
    with _resolves_to("fd00::6"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://[fd00::6]:9000/x")


@pytest.mark.parametrize("pattern", [
    "::", "::2", "[::2]:8080", "fd00::1",
    # Malformed brackets match nothing rather than a stray host or suffix.
    "[::1", "[]", "[::1]x", "[::1]:", "[::1]:http",
    # Unbracketed "address:port" is a different IPv6 address, not ::1.
    "::1:8080",
    # An IPv6 host never matches a leading-dot suffix.
    ".::1", ".1",
])
def test_ipv6_allowlist_does_not_overmatch(pattern: str) -> None:
    # Stripping a "port" off "::1" used to leave ":" or "::"; no IPv6 pattern
    # may match a different address.
    with _resolves_to("::1"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://[::1]/x", allow_hosts=[pattern])


@pytest.mark.parametrize("pattern", ["10.0.0.5", "10.0.0.5:8080", "http://10.0.0.5:8080"])
def test_ipv4_allowlist_with_and_without_port_unchanged(pattern: str) -> None:
    with _resolves_to("10.0.0.5"):
        assert_public_http_url("http://10.0.0.5:8080/x", allow_hosts=[pattern])
    with _resolves_to("10.0.0.6"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://10.0.0.6/x", allow_hosts=[pattern])


@pytest.mark.parametrize("pattern", ["internal.svc:8443", ".svc", "https://internal.svc:8443"])
def test_hostname_allowlist_forms_unchanged(pattern: str) -> None:
    with _resolves_to("10.0.0.5"):
        assert_public_http_url("https://internal.svc:8443/x", allow_hosts=[pattern])


@pytest.mark.parametrize("pattern", ["[.com]", "[evil.com]", "[evil.com]:443"])
def test_bracketed_non_ipv6_pattern_matches_nothing(pattern: str) -> None:
    # "[.com]" must not degrade into the suffix pattern ".com".
    with _resolves_to("10.0.0.5"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://evil.com/x", allow_hosts=[pattern])


@pytest.mark.parametrize("pattern", ["10.0.0.5", ".0.0.5"])
def test_ipv4_mapped_ipv6_host_does_not_match_ipv4_pattern(pattern: str) -> None:
    with _resolves_to("10.0.0.5"):
        with pytest.raises(UnsafeFetchURL):
            assert_public_http_url("http://[::ffff:a00:5]/x", allow_hosts=[pattern])


def test_ipv6_allowlist_is_case_insensitive() -> None:
    with _resolves_to("fd00::ab"):
        assert_public_http_url("http://[FD00::AB]/x", allow_hosts=["[fd00:0::Ab]:80"])


def test_same_origin_accepts_ipv6_origin() -> None:
    # Bare host form, as environment_readiness passes `ui.host`.
    assert_same_origin("http://[::1]:8080/next", ["::1", "example.org"])
    assert_same_origin("http://[::1]:8080/next", "http://[::1]:8080")
    with pytest.raises(UnsafeFetchURL):
        assert_same_origin("http://[::2]:8080/next", "http://[::1]:8080")


# ---- the operator-URL sources call the guard ----


def test_group_webpage_scrape_blocks_ssrf() -> None:
    from okg.substrate.library.sources.academic_researcher.group_webpage_scrape import (
        GroupWebpageScrapeSource,
    )
    src = GroupWebpageScrapeSource(group_webpage="file:///etc/passwd")
    with pytest.raises(UnsafeFetchURL):
        src.run("r1")


def test_calendar_ics_url_blocks_ssrf_but_allows_local_file(
    tmp_path,
) -> None:
    from okg.substrate.library.sources.calendar_ics import CalendarIcsSource

    # SSRF URL -> guarded (loud).
    with _resolves_to("169.254.169.254"):
        bad = CalendarIcsSource(
            calendar_ics_url="http://metadata.example/feed.ics",
        )
        with pytest.raises(UnsafeFetchURL):
            bad.run("r1")

    # Local file path -> read directly, no guard, no raise.
    ics = tmp_path / "cal.ics"
    ics.write_text("BEGIN:VCALENDAR\nEND:VCALENDAR\n")
    ok = CalendarIcsSource(calendar_ics_path=str(ics))
    run = ok.run("r2")
    assert run.completed_scope is True
