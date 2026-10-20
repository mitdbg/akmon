"""Central egress validator for outbound fetch URLs (SSRF guard + origin binding).

Per `validate-source-fetch-urls` (security umbrella `secure-academic-pi`) and
`centralize-egress-url-validation` (umbrella `harden-okg-security`).

This module is the single authority for outbound-URL policy across the substrate:
source adapters, OAuth delta/pagination walkers, mailbox/Poros delivery, and
sandbox-policy generation all route here. A source whose fetch target comes from
operator config, graph attributes, discovery, or a remote response — the group
webpage, a `.ics` feed URL, a `Link: rel=next` header, an `@odata.deltaLink`,
a `receiver_inbox_url` — must not be coaxed into fetching:

  * a non-http(s) URL: `file:///etc/passwd` (local file disclosure), `ftp://`,
    `gopher://`, `data:` …; or
  * an http(s) URL whose host resolves to a private / loopback / link-local /
    reserved address: `http://169.254.169.254/latest/meta-data/...`
    (cloud-credential theft), `http://localhost:<port>/...`, `http://10.0.0.5/...`
    (internal services).

Beyond the single-URL SSRF check, two follow-up hazards are covered:

  * **Redirect / pagination / delta hops.** A response can hand back a `Location`
    or `nextLink` pointing at a private/metadata host; `validate_redirect_target`
    re-runs the SSRF check on each subsequent hop before it is fetched.
  * **Token forwarding.** An OAuth Bearer / `x-api-key` must not be replayed to a
    response-provided URL that changes origin; `assert_same_origin` (and the
    combined `assert_token_request_url`) bind a token-bearing request to the
    provider's expected host set.

Private/internal destinations are reachable only through an explicit operator
exception (the `OKG_EGRESS_ALLOW_HOSTS` env allowlist or an `allow_hosts=`
argument); the exception is surfaced (logged at WARNING), never a silent
fallback. The guard fails LOUD: callers invoke it BEFORE their network
try/except so the rejection surfaces as an ingest failure rather than being
swallowed into an empty result.
"""
from __future__ import annotations

import ipaddress
import logging
import os
import socket
import urllib.error
import urllib.request
from collections.abc import Iterable
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

#: HTTP status codes that carry a ``Location`` redirect.
_REDIRECT_CODES = frozenset({301, 302, 303, 307, 308})

#: Request headers that must never be replayed to a different origin on a
#: redirect hop (credential/identity material).
_SENSITIVE_HEADERS = frozenset({"authorization", "cookie", "proxy-authorization"})

#: Env var holding a comma-separated allowlist of host[:port] entries that are
#: permitted even when they resolve to a private/internal address. A leading
#: dot (``.internal.example``) matches any subdomain. Empty by default.
EGRESS_ALLOW_HOSTS_ENV = "OKG_EGRESS_ALLOW_HOSTS"


class UnsafeFetchURL(ValueError):
    """An operator-supplied fetch URL is not a safe public http(s) URL."""


def _operator_allow_hosts() -> frozenset[str]:
    raw = os.environ.get(EGRESS_ALLOW_HOSTS_ENV, "")
    return frozenset(h.strip().lower() for h in raw.split(",") if h.strip())


def _canonical_ip_host(host: str) -> str:
    """Return an IP-literal host in its canonical text form (``0:0::1`` ->
    ``::1``); any other host is returned unchanged."""
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return host


def _pattern_host(pat: str) -> str:
    """Reduce an allowlist pattern to the bare host it names.

    Accepts a full origin/URL, ``host``, ``host:port``, a bare IPv6 literal
    (``::1``), or a bracketed one with or without a port (``[::1]:8080``).
    A malformed bracketed pattern (``[.com]``, ``[::1]x``) returns ``""`` and
    so matches nothing.
    """
    # Accept full origins/URLs as patterns by extracting their host.
    if "://" in pat:
        return urlparse(pat).hostname or pat
    if pat.startswith("["):  # bracketed IPv6, optionally with a port
        inner, sep, rest = pat[1:].partition("]")
        if not sep or not (rest == "" or (rest[:1] == ":" and rest[1:].isdigit())):
            return ""
        try:
            return str(ipaddress.IPv6Address(inner))
        except ValueError:
            return ""
    try:  # a bare IPv6 literal: every ':' belongs to the address
        return str(ipaddress.IPv6Address(pat))
    except ValueError:
        pass
    # Strip an explicit port from the pattern; matching is host-level.
    if ":" in pat and not pat.startswith("."):
        return pat.rsplit(":", 1)[0]
    return pat


def _host_matches(host: str, patterns: Iterable[str]) -> bool:
    """True if ``host`` exactly matches a pattern, or matches a leading-dot
    suffix pattern (``.googleapis.com`` matches ``content.googleapis.com``).
    IP-literal hosts compare in canonical form, so ``0:0:0:0:0:0:0:1`` and
    ``::1`` match. An IPv6 host matches only an exact pattern, never a
    leading-dot suffix."""
    host = _canonical_ip_host((host or "").lower())
    if not host:
        return False
    host_is_ipv6 = ":" in host
    for raw in patterns:
        pat = (raw or "").lower().strip()
        if not pat:
            continue
        pat = _canonical_ip_host(_pattern_host(pat))
        if not pat:
            continue
        if pat.startswith("."):
            if host_is_ipv6:
                continue
            if host == pat[1:] or host.endswith(pat):
                return True
        elif host == pat:
            return True
    return False


def assert_public_http_url(
    url: str,
    *,
    allow_hosts: Iterable[str] | None = None,
) -> None:
    """Raise `UnsafeFetchURL` unless `url` is http(s) and every address its host
    resolves to is publicly routable.

    Checks ALL resolved addresses (not just the first) so a round-robin /
    split-horizon answer can't sneak a private IP through. Full DNS-rebinding
    protection would also pin the connection to the validated IP; that residual
    is documented but the realistic vectors (file://, literal metadata IP,
    localhost, private hosts) are covered here.

    A host on the operator exception allowlist (``allow_hosts`` argument unioned
    with the ``OKG_EGRESS_ALLOW_HOSTS`` env var) is permitted even when it
    resolves to a private address; that permission is logged at WARNING, never
    silent.
    """
    parsed = urlparse(url or "")
    if parsed.scheme not in ("http", "https"):
        raise UnsafeFetchURL(
            "refusing to fetch non-http(s) URL "
            f"(scheme={parsed.scheme or '∅'!r}): only http/https operator "
            "URLs are allowed (blocks file://, ftp://, gopher://, data:, …)"
        )
    host = parsed.hostname
    if not host:
        raise UnsafeFetchURL(f"URL has no host: {url!r}")

    allowed = frozenset(h.lower() for h in (allow_hosts or ())) | _operator_allow_hosts()
    if allowed and _host_matches(host, allowed):
        logger.warning(
            "egress allowlist exception: permitting fetch to operator-allowed "
            "host %r (URL %r) despite the public-address requirement",
            host, url,
        )
        return

    try:
        infos = socket.getaddrinfo(
            host, parsed.port or None, proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as e:
        raise UnsafeFetchURL(
            f"could not resolve host {host!r}: {e}"
        ) from e
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise UnsafeFetchURL(
                f"refusing to fetch {url!r}: host {host!r} resolves to "
                f"non-public address {ip} (SSRF guard — blocks localhost, "
                "private ranges, and cloud-metadata 169.254.169.254)"
            )


def validate_redirect_target(
    location: str,
    *,
    base_url: str | None = None,
    allow_hosts: Iterable[str] | None = None,
) -> str:
    """Validate a redirect / pagination / delta target before the next hop.

    ``location`` may be relative; it is resolved against ``base_url`` (the URL
    that produced it) and then re-checked with `assert_public_http_url`. Returns
    the absolute, validated URL so the caller fetches exactly what was checked.
    """
    target = urljoin(base_url or "", location or "")
    assert_public_http_url(target, allow_hosts=allow_hosts)
    return target


def assert_same_origin(
    url: str,
    allowed: str | Iterable[str],
    *,
    allow_hosts: Iterable[str] | None = None,
) -> None:
    """Raise `UnsafeFetchURL` unless `url`'s host is in the `allowed` host set.

    Used to bind a token-bearing request to its provider's expected origin: a
    `nextPageToken` / `Link` / `@odata.deltaLink` that changes origin must not
    receive the OAuth/API token. ``allowed`` may be a single host/origin or an
    iterable; entries may be full origins, bare hosts, or leading-dot suffix
    patterns (``.googleapis.com``). Hosts on the operator exception allowlist
    are also accepted.
    """
    parsed = urlparse(url or "")
    host = parsed.hostname
    if not host:
        raise UnsafeFetchURL(f"URL has no host (cannot origin-bind): {url!r}")
    patterns: tuple[str, ...]
    if isinstance(allowed, str):
        patterns = (allowed,)
    else:
        patterns = tuple(allowed)
    if _host_matches(host, patterns):
        return
    extra = frozenset(h.lower() for h in (allow_hosts or ())) | _operator_allow_hosts()
    if extra and _host_matches(host, extra):
        logger.warning(
            "egress allowlist exception: permitting token-bearing request to "
            "operator-allowed host %r (URL %r) outside the provider origin set",
            host, url,
        )
        return
    raise UnsafeFetchURL(
        f"refusing to forward a token to {url!r}: host {host!r} is not in the "
        f"expected provider origin set {sorted(patterns)!r}; a response-provided "
        "URL cannot redirect a credentialed request to another origin"
    )


def assert_token_request_url(
    url: str,
    allowed: str | Iterable[str],
    *,
    allow_hosts: Iterable[str] | None = None,
) -> None:
    """Combined guard for a token-bearing request: the URL must be a public
    http(s) URL AND bound to the provider's expected origin set. Call this
    before attaching an OAuth Bearer / `x-api-key` to any request, including
    pagination/delta follow-ups."""
    assert_public_http_url(url, allow_hosts=allow_hosts)
    assert_same_origin(url, allowed, allow_hosts=allow_hosts)


class _NoFollowRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect handler that refuses to auto-follow.

    ``urllib``'s default opener follows 3xx redirects automatically AND (verified
    empirically on the runtime Python) replays the ``Authorization`` header to a
    cross-origin redirect target. That defeats the SSRF / token-origin guard,
    because the initial URL is validated but the redirect ``Location`` is not.
    Returning ``None`` from ``redirect_request`` makes the 3xx surface as an
    ``HTTPError`` (carrying the ``Location`` header) so the caller can re-validate
    the next hop BEFORE following it.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401,ANN001
        return None


def _redirected_request(
    prev: urllib.request.Request,
    target: str,
    code: int,
    *,
    strip_sensitive: bool,
) -> urllib.request.Request:
    """Build the next-hop request for a validated redirect ``target``.

    307/308 preserve method+body; 301/302/303 degrade to GET without a body
    (browser-equivalent). When ``strip_sensitive`` is set and the host changes,
    credential headers (Authorization/Cookie) are dropped as defense in depth.
    """
    headers = {k: v for k, v in prev.header_items()}
    if strip_sensitive and (urlparse(target).hostname != urlparse(prev.full_url).hostname):
        headers = {k: v for k, v in headers.items() if k.lower() not in _SENSITIVE_HEADERS}
    if code in (307, 308):
        return urllib.request.Request(
            target, data=prev.data, headers=headers, method=prev.get_method(),
        )
    return urllib.request.Request(target, headers=headers, method="GET")


def open_validated_url(
    request: urllib.request.Request | str,
    *,
    allow_hosts: Iterable[str] | None = None,
    same_origin: str | Iterable[str] | None = None,
    timeout: float = 30.0,
    max_redirects: int = 5,
):
    """SSRF-safe ``urlopen`` replacement that revalidates every redirect hop.

    The initial URL is checked with `assert_public_http_url` (and, when
    ``same_origin`` is given, bound to that origin set with `assert_same_origin`).
    Redirects are NOT auto-followed: each 3xx ``Location`` is resolved against the
    current URL, re-checked with `validate_redirect_target`, and only then
    re-issued. For a credentialed request (``same_origin`` set) a cross-origin
    redirect is REFUSED — the token never crosses origin. For an uncredentialed
    fetch, a cross-host hop still re-runs the public-address SSRF check and strips
    any credential headers. Returns the final response (an open file-like object
    the caller reads/closes), exactly like `urllib.request.urlopen`.
    """
    req = request if isinstance(request, urllib.request.Request) else urllib.request.Request(request)
    current_url = req.full_url
    # Validate the first hop BEFORE opening any connection so a refused request
    # never reaches the network (and never builds an opener).
    assert_public_http_url(current_url, allow_hosts=allow_hosts)
    if same_origin is not None:
        assert_same_origin(current_url, same_origin, allow_hosts=allow_hosts)

    opener = urllib.request.build_opener(_NoFollowRedirect)
    for _hop in range(max_redirects + 1):
        try:
            return opener.open(req, timeout=timeout)
        except urllib.error.HTTPError as exc:
            if exc.code not in _REDIRECT_CODES:
                raise
            location = exc.headers.get("Location")
            exc.close()
            if not location:
                raise
            target = validate_redirect_target(
                location, base_url=current_url, allow_hosts=allow_hosts,
            )
            if same_origin is not None:
                # A credentialed request must never be redirected off its origin.
                assert_same_origin(target, same_origin, allow_hosts=allow_hosts)
            req = _redirected_request(
                req, target, exc.code, strip_sensitive=same_origin is None,
            )
            current_url = target
    raise UnsafeFetchURL(
        f"refusing to follow more than {max_redirects} redirects starting at "
        f"{request if isinstance(request, str) else request.full_url!r} "
        "(possible redirect loop or SSRF probe)"
    )
