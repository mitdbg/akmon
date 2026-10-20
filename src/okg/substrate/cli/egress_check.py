"""Doctor-level egress-validator coverage check.

Mechanical regression guard for `centralize-egress-url-validation`: a substrate
module that opens an outbound HTTP connection (urllib / requests / httpx) must
also route through the central egress validator (`_url_safety`). If a change adds
a raw outbound sink without the guard, this check flags it loudly rather than
letting an unvalidated SSRF sink land silently.

The check SCANS THE WHOLE `src/okg/substrate` tree (not a hand-maintained file
list, which silently misses any sink not added to it — the adversarial-review
finding). It is intentionally coarse (module-level reference, not per-call
dataflow): "does this file that makes an outbound request also reference the
validator?". A file whose sink is a genuine false positive or an intentional
non-SSRF target is listed in `EGRESS_COVERAGE_EXCEPTIONS` with a written reason;
an undocumented unguarded sink is a violation. The check is wired into
`okg doctor` (egress_validator_coverage) so it actually runs.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

# Outbound-request sinks that require an egress check on their target URL.
_SINK_MARKERS = (
    "urllib.request.urlopen",
    "requests.post",
    "requests.get",
    "requests.put",
    "requests.patch",
    "requests.delete",
    "requests.request",
    "httpx.Client",
    "httpx.AsyncClient",
    "httpx.get",
    "httpx.post",
    "httpx.request",
    # The MCP SDK's streamable-HTTP client is an outbound sink like any
    # other: it dials an operator-supplied URL over httpx underneath, and
    # none of the markers above appear at the call site. Added by PACT
    # chat-source-admin, which introduced the first in-tree caller.
    "streamablehttp_client",
)

# References that prove the module routes through the central validator.
_VALIDATOR_MARKERS = (
    "_url_safety",
    "assert_public_http_url",
    "assert_token_request_url",
    "assert_same_origin",
    "validate_redirect_target",
    "open_validated_url",
)

#: Repo root, derived from this file's location
#: (src/okg/substrate/cli/egress_check.py -> repo root is parents[4]).
_REPO_ROOT = Path(__file__).resolve().parents[4]

#: Subtree scanned for outbound sinks.
_SCAN_ROOT = "src/okg/substrate"

#: Documented exceptions: a file whose sink marker is a false positive or an
#: intentional NON-SSRF target. Each MUST carry a written reason — an
#: undocumented unguarded sink is a violation. (repo-relative posix path.)
EGRESS_COVERAGE_EXCEPTIONS: dict[str, str] = {
    "src/okg/substrate/library/sources/discovery/registry.py": (
        "Intentional probe of the opt-in Zotero localhost read API at a "
        "hardcoded http://127.0.0.1:<operator-port>/api/ — a loopback target by "
        "design, not an attacker-controlled SSRF sink; the egress validator "
        "would (correctly) reject loopback, breaking the legitimate probe."
    ),
    "src/okg/substrate/chat/source_admin.py": (
        "Dials the deployment's OWN mcp-serve endpoint, supplied by the "
        "operator on the command line or by the deployment manifest — "
        "never by fetched content, so there is no attacker-controlled "
        "URL to redirect. The target is additionally constrained before "
        "any dial by `require_admin_transport`, which refuses every "
        "non-loopback host unless callers must present a bearer "
        "credential; the egress validator would (correctly) reject the "
        "loopback interim this surface is specified to support."
    ),
    "src/okg/substrate/chat/sync.py": (
        "Dials the OPERATOR'S OWN Open WebUI instance at the address they "
        "exported as OKG_CHAT_INSTANCE_URL — never a URL from fetched "
        "content, so there is no attacker-controlled target to redirect. "
        "Every instance this verb is specified to configure is reachable at "
        "loopback or on the operator's own network, which the egress "
        "validator would (correctly) reject. Error bodies are passed through "
        "`redact()` before they reach any log, so a 401 or 422 echoing the "
        "submitted payload cannot leak the admin token."
    ),
    "src/okg/substrate/chat/instance.py": (
        "Liveness probe of the instance this module JUST started, at a URL it "
        "constructed itself from the record it wrote: "
        "`f\"http://{record.host}:{record.port}\"` (instance.py:1569) against "
        "VERSION_PATH. The address never originates in fetched content, so "
        "there is no attacker-controlled target to redirect; and the probe is "
        "loopback by construction, which the egress validator would "
        "(correctly) reject. Refusing the probe would make `chat-instance up` "
        "unable to observe the one signal its contract defines as proof the "
        "instance is running."
    ),
    "src/okg/substrate/chat/setup.py": (
        "`okg chat setup` dials the Open WebUI instance it JUST started, at "
        "the loopback address it chose itself (`SetupOptions.host`, default "
        "127.0.0.1) plus the deployment's own mcp-serve endpoint from the "
        "manifest or the operator's flags -- never a URL from fetched "
        "content, so there is no attacker-controlled target to redirect. "
        "The dial refuses every 3xx outright (`_NoRedirect`), and the egress "
        "validator would (correctly) reject the loopback target this "
        "command exists to configure."
    ),
    "src/okg/substrate/chat/front_door.py": (
        "The reference front door proxies to upstreams DERIVED from the "
        "deployment's declared instance port -- `chat_upstream=f\"http://"
        "{instance_host}:{instance_port}\"` (front_door.py:366) -- never "
        "restated by hand and never taken from fetched content, so there is "
        "no attacker-controlled URL to redirect. Both hops are loopback by "
        "design, which the egress validator would (correctly) reject. This "
        "server is the single-operator reference front used to prove the "
        "routing contract; the production front is nginx, whose config this "
        "module renders rather than dials."
    ),
    "src/okg/substrate/library/sources/_http_cache.py": (
        "CachedFetcher wraps an INJECTED HTTP session (requests/httpx/curl_cffi) "
        "and calls self.session.get(url); the only sink marker is a docstring "
        "mention of httpx.Client, not a direct call. The url originates at the "
        "source that constructs it, which validates before fetching."
    ),
}

#: Retained for back-compat: the originally-registered outbound helpers. The
#: tree scan now supersedes this list (it can't miss a new sink), but the names
#: document the known SSRF-sensitive helpers.
REGISTERED_OUTBOUND_HELPERS: tuple[str, ...] = (
    "src/okg/substrate/exchange/sender_delivery.py",
    "src/okg/substrate/federation/mailbox/client.py",
    "src/okg/substrate/cli/federation.py",
    "src/okg/substrate/library/oauth/refresh.py",
    "src/okg/substrate/library/oauth/flow.py",
    "src/okg/substrate/nomos/oidc_login.py",
    "src/okg/substrate/library/sources/canvas_api.py",
    "src/okg/substrate/library/sources/calendar_ics.py",
)


_REQUESTS_VERBS = (
    "requests.get(", "requests.post(", "requests.put(",
    "requests.patch(", "requests.delete(", "requests.request(",
)
_OTHER_SINKS = (
    "httpx.Client", "httpx.AsyncClient", "httpx.get", "httpx.post", "httpx.request",
)


def egress_violation_reason(text: str) -> str | None:
    """Return a reason string if `text` makes an outbound request that can be
    redirected to an unvalidated origin, else None.

    Crucially, a first-hop-only `assert_public_http_url(...)` is NOT enough: a raw
    `urllib.request.urlopen(` follows 3xx automatically, and a `requests.*` call
    follows redirects by default — so the SSRF guard is bypassed on the redirect.
    The only accepted forms are routing through `open_validated_url` (urllib) or
    `allow_redirects=False` (requests). Other sinks (httpx, …) still require some
    validator reference. This is stricter than a bare module-level marker check —
    that coarseness let a raw-urlopen sink pass with a first-hop guard."""
    if "urllib.request.urlopen(" in text and "open_validated_url" not in text:
        return (
            "raw urllib.request.urlopen() follows redirects; route it through "
            "open_validated_url (a first-hop assert_public_http_url does not stop "
            "a 3xx redirect to a private/metadata host)"
        )
    if any(v in text for v in _REQUESTS_VERBS) and (
        "allow_redirects=False" not in text and "open_validated_url" not in text
    ):
        return (
            "requests.* call follows redirects by default; pass "
            "allow_redirects=False (and treat a 3xx as failure) or route through "
            "open_validated_url"
        )
    if any(m in text for m in _OTHER_SINKS) and not any(
        m in text for m in _VALIDATOR_MARKERS
    ):
        return "outbound HTTP sink with no egress-validator reference"
    return None


def file_has_unguarded_sink(text: str) -> bool:
    """True if `text` makes an outbound request that is not redirect-safe."""
    return egress_violation_reason(text) is not None


def scan_egress_coverage(*, repo_root: Path | None = None) -> dict:
    """Scan the whole substrate tree for outbound sinks lacking a validator.

    Returns `{ok, checked, violations, exceptions}`. `violations` are
    repo-relative files with an outbound sink, no validator reference, and no
    documented exception. This is the doctor-wired entry point."""
    root = repo_root or _REPO_ROOT
    scan_dir = root / _SCAN_ROOT
    violations: list[str] = []
    checked: list[str] = []
    for p in sorted(scan_dir.rglob("*.py")):
        text = p.read_text(encoding="utf-8", errors="replace")
        if not any(marker in text for marker in _SINK_MARKERS):
            continue
        rel = p.relative_to(root).as_posix()
        checked.append(rel)
        if egress_violation_reason(text) is None:
            continue
        if rel in EGRESS_COVERAGE_EXCEPTIONS:
            continue
        violations.append(rel)
    return {
        "ok": not violations,
        "checked": checked,
        "violations": violations,
        "exceptions": sorted(EGRESS_COVERAGE_EXCEPTIONS),
    }


def evaluate_egress_coverage(
    paths: Iterable[str | Path] | None = None,
    *,
    repo_root: Path | None = None,
) -> dict:
    """Evaluate egress-validator coverage. With explicit `paths`, check exactly
    those files (synthetic deny/allow inputs); with no paths, scan the whole
    substrate tree (the regression guard)."""
    if paths is None:
        return scan_egress_coverage(repo_root=repo_root)
    root = repo_root or _REPO_ROOT
    violations: list[str] = []
    checked: list[str] = []
    for target in paths:
        p = Path(target)
        if not p.is_absolute():
            p = root / p
        text = p.read_text(encoding="utf-8")
        checked.append(str(target))
        if file_has_unguarded_sink(text):
            violations.append(str(target))
    return {"ok": not violations, "checked": checked, "violations": violations}
