"""The single-operator front door for one deployment (PACT
``chat-instance-lifecycle``).

design.md section 5 mechanism 1 makes a reverse proxy the front door: one
hostname per deployment, chat at ``/``, the operator console at ``/console``
once its bundle can survive a path prefix. Nothing in the program produced
one — the only proxy work is ``task.chat-sso-integration.proxy-integration``,
which is todo and scoped to the MULTI-USER shape. This module produces the
single-operator shape, and is written so the two can never both be live for
one deployment.

Two artifacts from one plan
---------------------------
:func:`plan_front_door` decides the routes. Everything downstream renders
from that one :class:`FrontDoorPlan`:

* :func:`render_nginx_front_door` — the configuration an operator deploys.
* :func:`serve_front_door` — a real reverse proxy over the SAME plan, so the
  route table is provable end to end on a machine with no nginx binary. It is
  a proof surface and a local bring-up aid, not the production front; the
  header of the rendered nginx says which one you are looking at.

Where they disagree, the plan is the arbiter, and
:func:`front_door_route_table` is what both are checked against.

Why this refuses to render for a multi-user deployment
------------------------------------------------------
The PACT's ordering clause says ``chat-sso-integration`` REPLACES this
configuration rather than adding a second proxy. A deployment that declares
``chat.sso.mode: multi_user`` therefore gets a refusal here naming
``chat.sso.render_nginx_site`` (``chat/sso.py:2026``) as the renderer that
owns it. Two fronts for one hostname is not a redundancy — nginx matches the
first ``server`` block and the second is dead configuration, which is exactly
the silent-topology failure the program keeps finding.

Transport, and why an ``https`` site base is a refusal without a certificate
---------------------------------------------------------------------------
The listen port is DERIVED from ``chat_site_base``'s scheme, so an ``https``
base used to render ``listen 443;`` — a listener every operator and every
client reads as TLS-terminated, in front of a proxy that performs no
authentication, actually serving plain HTTP. This module either emits a
COMPLETE TLS server block (certificate, key, protocol floor, session settings)
from operator-supplied paths, or refuses the ``https`` base naming the two
inputs it is missing. :func:`routes_agree` carries the invariant so it binds
any renderer, and :func:`serve_front_door` refuses a TLS plan outright because
the reference front door speaks plain HTTP.

The console location: a route when the bundle is MEASURED fit, a refusal
otherwise
-----------------------------------------------------------------------
The ``/console`` prefix was measured negative at the console build the
read-handshake spike used: its assets were requested at the origin root, they
404, and the app never booted while the front answered 200 with a blank page
(``evidence/read-handshake-transcript.md:31`` and ``:350-354``). That is a
fact about a BUNDLE, not about the topology, and
:func:`okg.substrate.chat.sso.console_base_awareness` already decides it per
bundle — measured from the built ``index.html`` when one is reachable,
declared by the operator when it is not, and refused when neither.

So the console location is gated on that verdict, and the gate is closed by
default:

* **Not permitted** (no evidence, or a bundle measured asking for its assets
  at the origin root) — an explicit not-configured response, and no location
  that serves anything. Not an OMITTED location: omitted, ``/console`` falls
  through to the catch-all, reaches the chat instance, and returns Open
  WebUI's own single-page index — a 200 that looks like a console that failed
  to render. An explicit 503 naming ``operator-ui-base-aware-build`` is the
  honest answer, and the arm that proves it asserts on the BODY.
* **Permitted** — ``/console`` proxies to the console upstream the operator
  named, with the prefix left ON the forwarded request, because a base-aware
  bundle is precisely one that asks for ``/console/…`` and would 404 against
  an upstream that had the prefix stripped from under it.

The upstream is never inferred. A permitted bundle with no ``--console-upstream``
refuses naming the flag, and an upstream supplied for a bundle that is not
permitted refuses rather than being silently discarded into a 503 — the same
rule the TLS material follows, for the same reason: an operator who believes a
surface is served and finds it is not has been told nothing.
"""

from __future__ import annotations

import http.server
import json
import re
import socket
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from okg.substrate.chat.instance import ChatInstanceError, LOOPBACK_CANONICAL

__all__ = [
    "CONSOLE_OWNER_PACT",
    "CONSOLE_PREFIX",
    "console_directives",
    "FrontDoorPlan",
    "FrontDoorServer",
    "SINGLE_OPERATOR_HEADER_MARKER",
    "TlsMaterial",
    "front_door_route_table",
    "plan_front_door",
    "render_nginx_front_door",
    "serve_front_door",
]


#: The sentence the generated configuration must carry in its own header, so
#: a file read out of context can never be mistaken for the multi-user shape
#: (design.md:456-458: "until then both surfaces run single-operator local
#: auth").
SINGLE_OPERATOR_HEADER_MARKER = "single-operator local auth"

#: The path the console will occupy once its bundle is base-aware. Until then
#: it carries the refusal, never a route.
CONSOLE_PREFIX = "/console"

#: Named in the not-configured body, so the person who hits it learns which
#: piece of work unblocks them rather than that "something is wrong".
CONSOLE_OWNER_PACT = "operator-ui-base-aware-build"

CONSOLE_NOT_CONFIGURED_STATUS = 503

#: Ports a client speaks TLS to by convention. A listener on one of them
#: without ``ssl`` is the failure they are named for: the operator reads 443
#: as terminated, the browser sends a ClientHello, and nginx answers it as a
#: malformed HTTP request.
_TLS_BY_CONVENTION = frozenset({443, 8443})

#: Plain text, not HTML. A blank-looking HTML error page is the outcome this
#: arm exists to exclude; a body that reads as a sentence cannot be mistaken
#: for one.
CONSOLE_NOT_CONFIGURED_BODY = (
    f"the operator console is not configured on this front door.\n"
    f"The console is not served under {CONSOLE_PREFIX}: nothing here has "
    f"established that its bundle asks for its assets under the prefix.\n"
    f"Owner: PACT {CONSOLE_OWNER_PACT} (a base-aware console build), or "
    f"publish the console on its own hostname.\n"
)


def CONSOLE_NOT_CONFIGURED_BODY_FOR(plan: "FrontDoorPlan") -> str:  # noqa: N802
    """The not-configured body, saying which of the two refusals this is.

    The constant above used to assert a MEASUREMENT — "its assets are
    requested at the origin root, so they 404" — on every refusal, including
    the ``no_evidence`` one where no bundle was read at all. In a PACT whose
    standing rule is that an absent input must never be recorded as a
    measurement, serving that sentence for a bundle nobody looked at is the
    defect in its own vocabulary. So the body carries the plan's actual
    verdict, and the fixed sentence above no longer claims a measurement.
    """
    detail = (plan.console_detail or "").strip()
    return (
        f"{CONSOLE_NOT_CONFIGURED_BODY}"
        f"Evidence: {plan.console_evidence}.\n"
        + (f"{detail}\n" if detail else "")
    )


@dataclass(frozen=True)
class TlsMaterial:
    """Where the proxy host keeps this deployment's certificate and key.

    PATHS, never bytes. The private key stays a file the proxy reads, which is
    the same by-reference rule the app database DSN and the instance secret key
    follow: nothing here is a credential, so it is safe in a plan, a report and
    the generated configuration — which has to name them anyway.

    Existence is deliberately not checked: the configuration is frequently
    rendered somewhere other than the host that serves it, and nginx refuses to
    start, loudly and by path, on a certificate that is not there.
    """

    certificate: str
    private_key: str

    def as_dict(self) -> dict[str, Any]:
        return {"certificate": self.certificate, "private_key": self.private_key}


@dataclass(frozen=True)
class FrontDoorPlan:
    """One hostname, chat at ``/``, and an explicitly unconfigured console.

    ``chat_upstream`` is derived from the deployment's declared instance port
    rather than restated: a hand-restated upstream can disagree with the port
    the instance actually bound, and a disagreement nobody reads is not
    detectable.

    ``scheme`` and ``tls`` travel together and are checked against each other
    at :func:`plan_front_door`: an ``https`` site base with no TLS material is
    a refusal, never a plain ``listen 443;``.
    """

    deployment: str
    hostname: str
    listen_port: int
    chat_upstream: str
    chat_site_base: str
    console_permitted: bool
    console_evidence: str
    console_detail: str
    scheme: str = "http"
    tls: TlsMaterial | None = None
    #: Where a PERMITTED console is served from. ``None`` exactly when the
    #: console is not permitted: the two are checked against each other at
    #: :func:`plan_front_door`, so no plan can reach a renderer claiming a
    #: console it has no upstream for, or holding an upstream it will not use.
    console_upstream: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "hostname": self.hostname,
            "listen_port": self.listen_port,
            "chat_upstream": self.chat_upstream,
            "chat_site_base": self.chat_site_base,
            "console_permitted": self.console_permitted,
            "console_evidence": self.console_evidence,
            "console_detail": self.console_detail,
            "console_upstream": self.console_upstream,
            "scheme": self.scheme,
            "tls": self.tls.as_dict() if self.tls else None,
            "auth": SINGLE_OPERATOR_HEADER_MARKER,
            "routes": front_door_route_table(self),
        }


def front_door_route_table(plan: FrontDoorPlan) -> list[dict[str, Any]]:
    """The route table BOTH renderers are checked against.

    Two entries and no third: the console path, and chat at the root. What the
    console entry DOES is the plan's ``console_permitted`` verdict and nothing
    else — the reason this function reads the plan rather than hard-coding the
    not-configured answer is that the verdict used to be computed, published in
    the plan, and then ignored by every renderer, so a bundle measured fit was
    503'd exactly like one measured unfit.
    """
    if plan.console_permitted:
        console = {
            "path": CONSOLE_PREFIX,
            "action": "proxy",
            "upstream": plan.console_upstream,
            "evidence": plan.console_evidence,
        }
    else:
        console = {
            "path": CONSOLE_PREFIX,
            "action": "not_configured",
            "status": CONSOLE_NOT_CONFIGURED_STATUS,
            "owner": CONSOLE_OWNER_PACT,
            "evidence": plan.console_evidence,
        }
    return [
        console,
        {"path": "/", "action": "proxy", "upstream": plan.chat_upstream},
    ]


def _hostname_from_base(
    deployment: str, chat_site_base: str | None,
) -> tuple[str, int, str]:
    if not chat_site_base:
        raise ChatInstanceError(
            f"deployment {deployment!r} declares no chat.deep_links."
            f"chat_site_base, so the front door has no hostname",
            data={
                "reason": "chat_site_base_undeclared",
                "deployment": deployment,
                "manifest_key": "chat.deep_links.chat_site_base",
            },
            fix_hint=(
                "declare chat.deep_links.chat_site_base in the deployment "
                "manifest (design.md section 4). It is the public base URL of "
                "this deployment's chat site — identity, not a credential — "
                "and the front door's server_name is generated from it, never "
                "guessed"
            ),
        )
    split = urlsplit(str(chat_site_base))
    host = (split.hostname or "").strip().lower()
    if not host:
        raise ChatInstanceError(
            f"deployment {deployment!r}'s chat.deep_links.chat_site_base "
            f"({chat_site_base!r}) carries no hostname",
            data={
                "reason": "chat_site_base_hostless",
                "deployment": deployment,
                "chat_site_base": str(chat_site_base),
            },
            fix_hint="use an absolute URL, e.g. http://chat.example.org",
        )
    scheme = (split.scheme or "http").strip().lower()
    port = split.port or (443 if scheme == "https" else 80)
    return host, int(port), scheme


def plan_front_door(
    deployment: str,
    *,
    chat_block: Any,
    instance_port: int,
    instance_host: str = LOOPBACK_CANONICAL,
    console_build: Any = None,
    console_upstream: str | None = None,
    tls: TlsMaterial | None = None,
) -> FrontDoorPlan:
    """Decide the front door, or refuse.

    Seven refusals, each naming its owner: a multi-user deployment (owned by
    ``chat-sso-integration``), an undeclared ``chat_site_base`` (owned by the
    manifest), a chat block that is absent or disabled (there is no site to
    front), an ``https`` site base with no TLS material, TLS material for a
    plain-``http`` site base, a console bundle permitted with nowhere to serve
    it from, and a console upstream for a bundle that is not permitted.

    The last two exist because the listen port is DERIVED from the site base's
    scheme. Without them an ``https`` base rendered ``listen 443;`` with no
    ``ssl`` directive and no certificate — a front that reads as TLS-terminated
    at every glance, in front of a proxy that performs no authentication, and
    that in fact serves plain HTTP on the port every client will speak TLS to.
    """
    if chat_block is None or not getattr(chat_block, "enabled", False):
        raise ChatInstanceError(
            f"deployment {deployment!r} has no enabled chat: block, so there "
            f"is no chat site to put a front door in front of",
            data={"reason": "no_enabled_chat_block", "deployment": deployment},
            fix_hint="set chat.enabled: true in the deployment manifest",
        )

    sso = getattr(chat_block, "sso", None)
    if sso is not None and getattr(sso, "multi_user", False):
        raise ChatInstanceError(
            f"deployment {deployment!r} declares multi-user SSO, so its front "
            f"door is the SSO-fronted one, not this single-operator shape",
            data={
                "reason": "multi_user_front_owned_elsewhere",
                "deployment": deployment,
                "owner": "chat-sso-integration",
                "renderer": "okg.substrate.chat.sso.render_nginx_site",
            },
            fix_hint=(
                "render the production front with `okg chat sso-check "
                "--render nginx`. Two server blocks on one server_name are "
                "not two fronts: nginx matches the first and the second is "
                "dead configuration"
            ),
        )

    deep_links = getattr(chat_block, "deep_links", None)
    site_base = getattr(deep_links, "chat_site_base", None)
    hostname, listen_port, scheme = _hostname_from_base(deployment, site_base)
    if scheme == "https" and tls is None:
        raise ChatInstanceError(
            f"deployment {deployment!r}'s chat_site_base is https, so its front "
            f"door must terminate TLS, and no certificate or key was named",
            data={
                "reason": "tls_material_undeclared",
                "deployment": deployment,
                "chat_site_base": str(site_base),
                "listen_port": listen_port,
                "missing": ["--tls-certificate", "--tls-private-key"],
            },
            fix_hint=(
                "pass --tls-certificate and --tls-private-key (PATHS on the "
                "proxy host; the key is read by nginx and never by okg), or "
                "declare an http chat_site_base. This front performs no "
                "authentication, so a listener that looks TLS-terminated and "
                "is not would put the chat instance's own login on the wire"
            ),
        )
    if scheme != "https" and listen_port in _TLS_BY_CONVENTION:
        raise ChatInstanceError(
            f"deployment {deployment!r}'s chat_site_base is {scheme}, on port "
            f"{listen_port}, which clients speak TLS to",
            data={
                "reason": "plain_scheme_on_tls_port",
                "deployment": deployment,
                "chat_site_base": str(site_base),
                "scheme": scheme,
                "listen_port": listen_port,
            },
            fix_hint=(
                f"declare the site base as https (and pass --tls-certificate "
                f"and --tls-private-key), or move it off {listen_port}. A "
                f"plain listener on that port reads as TLS-terminated and "
                f"answers a ClientHello as a malformed HTTP request"
            ),
        )
    if scheme != "https" and tls is not None:
        raise ChatInstanceError(
            f"deployment {deployment!r} supplied TLS material for a "
            f"{scheme!r} chat_site_base, which is served without TLS",
            data={
                "reason": "tls_material_for_plain_site_base",
                "deployment": deployment,
                "chat_site_base": str(site_base),
                "scheme": scheme,
            },
            fix_hint=(
                "declare an https chat_site_base, or drop the TLS flags. The "
                "material is refused rather than ignored: a certificate the "
                "operator believes is in use and is not is the same defect as "
                "a listener that claims TLS and has none"
            ),
        )

    # Consumed, not reimplemented: the base-awareness verdict already exists
    # and already carries the measured/declared/absent distinction.
    from okg.substrate.chat.instance import instance_base_url
    from okg.substrate.chat.sso import console_base_awareness

    verdict = console_base_awareness(
        CONSOLE_PREFIX, console_build, deployment=deployment,
    )
    upstream = _resolve_console_upstream(
        deployment, verdict=verdict, console_upstream=console_upstream,
    )
    return FrontDoorPlan(
        deployment=deployment,
        hostname=hostname,
        listen_port=listen_port,
        # An IPv6 instance host is bracketed, or the upstream does not parse.
        chat_upstream=instance_base_url(instance_host, instance_port),
        chat_site_base=str(getattr(deep_links, "chat_site_base", "")),
        console_permitted=bool(verdict.permitted),
        console_evidence=str(verdict.evidence),
        console_detail=str(verdict.detail),
        console_upstream=upstream,
        scheme=scheme,
        tls=tls,
    )


def _resolve_console_upstream(
    deployment: str, *, verdict: Any, console_upstream: str | None,
) -> str | None:
    """Pair the base-awareness verdict with somewhere to serve the console from.

    Both directions are refusals rather than quiet resolutions, and both are
    the same defect seen from opposite sides — a surface the operator believes
    is served and is not:

    * a bundle this front is willing to serve, with no upstream, would have to
      be answered by guessing an origin. ``chat.deep_links.operator_ui_base``
      is the console's PUBLIC base — where a person's browser is sent — and on
      a fronted deployment that is this very hostname, so treating it as an
      upstream points the front door at itself;
    * an upstream for a bundle that is not permitted would be dropped on the
      floor and the path would answer 503, which reads to the operator as the
      flag not having been passed at all.
    """
    named = (console_upstream or "").strip().rstrip("/")
    permitted = bool(getattr(verdict, "permitted", False))
    if permitted and not named:
        raise ChatInstanceError(
            f"deployment {deployment!r}'s console bundle may be served under "
            f"{CONSOLE_PREFIX}, and no upstream was named to serve it from",
            data={
                "reason": "console_upstream_undeclared",
                "deployment": deployment,
                "console_prefix": CONSOLE_PREFIX,
                "console_evidence": str(getattr(verdict, "evidence", "")),
                "missing": ["--console-upstream"],
            },
            fix_hint=(
                "pass --console-upstream http://127.0.0.1:<port> naming the "
                "origin that serves the built console, or drop the "
                "--console-bundle-index/--console-base-aware evidence so the "
                f"front answers {CONSOLE_NOT_CONFIGURED_STATUS} at "
                f"{CONSOLE_PREFIX}. It is never inferred: "
                "chat.deep_links.operator_ui_base is the console's PUBLIC "
                "base, which on a fronted deployment is this hostname, so "
                "using it would point the front door at itself"
            ),
        )
    if named and not permitted:
        raise ChatInstanceError(
            f"deployment {deployment!r} named a console upstream, and its "
            f"bundle is not permitted under {CONSOLE_PREFIX}",
            data={
                "reason": "console_upstream_for_unpermitted_bundle",
                "deployment": deployment,
                "console_prefix": CONSOLE_PREFIX,
                "console_upstream": named,
                "console_evidence": str(getattr(verdict, "evidence", "")),
                "console_detail": str(getattr(verdict, "detail", "")),
            },
            fix_hint=(
                getattr(verdict, "fix_hint", None)
                or (
                    "pass --console-bundle-index <built index.html> so the "
                    "bundle is measured, or --console-base-aware to declare "
                    "it. Refused rather than ignored: silently rendering the "
                    f"{CONSOLE_NOT_CONFIGURED_STATUS} here would read as the "
                    "upstream flag never having been passed"
                )
            ),
        )
    if not named:
        return None
    split = urlsplit(named)
    if split.scheme not in {"http", "https"} or not split.hostname:
        raise ChatInstanceError(
            f"deployment {deployment!r}'s console upstream {named!r} is not an "
            f"absolute http or https origin",
            data={
                "reason": "console_upstream_not_an_origin",
                "deployment": deployment,
                "console_upstream": named,
            },
            fix_hint="use e.g. http://127.0.0.1:5173",
        )
    if split.path or split.query or split.fragment:
        # An ORIGIN, not a URL, and the difference is the whole prefix
        # contract. In nginx a `proxy_pass` carrying a URI part REPLACES the
        # matched location prefix, so `--console-upstream http://host/ui`
        # would send `/console/assets/app.js` upstream as `/ui/assets/app.js`
        # — the prefix stripped from under a bundle that asks for it, which is
        # the 404 storm this whole gate exists to prevent, arriving through
        # the flag that turns the gate on. The served handler concatenates
        # instead and would forward `/ui/console/assets/app.js`, so the two
        # artifacts would also disagree. Refused here rather than reconciled,
        # because there is no reading of a path on an upstream that both
        # renderers can honour.
        raise ChatInstanceError(
            f"deployment {deployment!r}'s console upstream {named!r} carries a "
            f"path, query or fragment; it must be a bare origin",
            data={
                "reason": "console_upstream_is_not_a_bare_origin",
                "deployment": deployment,
                "console_upstream": named,
                "path": split.path,
                "query": split.query,
                "fragment": split.fragment,
            },
            fix_hint=(
                f"pass just the scheme, host and port, e.g. "
                f"{split.scheme}://{split.netloc}. The {CONSOLE_PREFIX} prefix "
                "stays ON the forwarded request — that is what base-aware "
                "means — and an upstream with a path would make nginx strip it"
            ),
        )
    return named


#: The TLS floor for a generated block. Named here rather than inline so the
#: agreement check can assert the emitted block is COMPLETE — a `listen … ssl`
#: with no certificate is an nginx that will not start, and a certificate with
#: no protocol floor is one that will negotiate whatever the build's default
#: happens to be.
_TLS_PROTOCOLS = "TLSv1.2 TLSv1.3"


def _transport_header(plan: FrontDoorPlan) -> list[str]:
    """What the file says about its own transport, next to what it says about
    its own auth.

    Both sentences answer the same operator question — what does this front
    actually protect — and a file that states one and leaves the other to be
    inferred from a port number is how ``listen 443;`` with no ``ssl`` went
    unread.
    """
    if plan.tls is not None:
        return [
            f"# TRANSPORT: TLS terminated here on {plan.listen_port}, from",
            f"# {plan.tls.certificate}. The upstream hop is plain HTTP to the",
            "# instance on loopback.",
        ]
    return [
        f"# TRANSPORT: plain HTTP on {plan.listen_port}. The deployment's",
        "# chat_site_base is http, so nothing here terminates TLS and the",
        "# instance's own login travels in the clear. Declare an https",
        "# chat_site_base and pass --tls-certificate/--tls-private-key to",
        "# change that.",
    ]


def _listen_lines(plan: FrontDoorPlan) -> list[str]:
    """The listener, and the TLS directives that must accompany it.

    Kept together in one function because the defect being excluded is exactly
    their separation: the port came from the site base's scheme while the
    directives came from nowhere, so an ``https`` base rendered ``listen 443;``
    and nothing else.
    """
    if plan.tls is None:
        return [f"    listen {plan.listen_port};"]
    return [
        f"    listen {plan.listen_port} ssl;",
        "",
        "    # TLS terminates HERE. The upstream hop is plain HTTP to the",
        "    # instance on loopback, which is why X-Forwarded-Proto below is",
        "    # what tells Open WebUI the client's scheme.",
        f"    ssl_certificate {plan.tls.certificate};",
        f"    ssl_certificate_key {plan.tls.private_key};",
        f"    ssl_protocols {_TLS_PROTOCOLS};",
        "    ssl_prefer_server_ciphers off;",
        "    ssl_session_timeout 1d;",
        f"    ssl_session_cache shared:okg-{plan.deployment}:10m;",
        "    ssl_session_tickets off;",
    ]


def _console_header(plan: FrontDoorPlan) -> list[str]:
    """What the file says about the console, on the evidence it was decided on.

    Both cases name the evidence kind, so a file found on a host months later
    says whether the console behind it was MEASURED fit or merely declared —
    the distinction :func:`console_base_awareness` draws and the one an
    operator debugging a blank page needs first.
    """
    if plan.console_permitted:
        return [
            f"# The operator console IS served here, at {CONSOLE_PREFIX}, from",
            f"# {plan.console_upstream}. Evidence: {plan.console_evidence}.",
            f"# {plan.console_detail or 'the bundle is base-aware'}",
        ]
    return [
        f"# The operator console is NOT served here. {plan.console_evidence}:",
        f"# {plan.console_detail or 'no base-aware console bundle is available'}",
        f"# Owner: PACT {CONSOLE_OWNER_PACT}.",
    ]


#: What the console location matches, spelled so nginx agrees with the served
#: handler. A bare ``location /console`` is a PREFIX match in nginx, so it
#: also swallows ``/console-old`` — while the handler matches ``/console`` and
#: ``/console/…`` exactly. That divergence was harmless while both answered
#: 503 and stops being harmless the moment one of them proxies: nginx would
#: send ``/console-old`` to the console upstream and the handler would send it
#: to chat. ``^~`` on the slashed form plus an exact match on the bare one is
#: the pair that means what the handler means.
CONSOLE_LOCATION_EXACT = f"location = {CONSOLE_PREFIX}"
CONSOLE_LOCATION_PREFIX = f"location ^~ {CONSOLE_PREFIX}/"


def _console_location(plan: FrontDoorPlan) -> list[str]:
    """The console location, in whichever of its two shapes the plan decided.

    Two location blocks, not one, and the reason is in
    :data:`CONSOLE_LOCATION_PREFIX`: a bare ``location /console`` matches
    ``/console-old`` too, which the served handler does not.

    The served shape's ``proxy_pass`` deliberately carries NO URI part. In
    nginx that is what forwards the request URI unchanged, prefix included —
    and a base-aware bundle is by definition one that asks for
    ``/console/…``. A trailing slash, or any path on the upstream, would strip
    the prefix on the way to an upstream whose own asset paths still carry it,
    reproducing the 404 storm the base-awareness gate exists to prevent, from
    the other end. :func:`_resolve_console_upstream` refuses an upstream that
    could do it.
    """
    if plan.console_permitted:
        body = [
            f"        proxy_pass {plan.console_upstream};",
            "        proxy_http_version 1.1;",
            "        proxy_set_header Host $host;",
            "        proxy_set_header X-Real-IP $remote_addr;",
            "        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;",
            "        proxy_set_header X-Forwarded-Proto $scheme;",
            "        proxy_set_header Upgrade $http_upgrade;",
            "        proxy_set_header Connection $okg_connection_upgrade;",
        ]
        return [
            "    # The console, on the base-awareness verdict recorded in the",
            "    # header above. No trailing slash on proxy_pass and no path on",
            "    # the upstream: the prefix stays ON the forwarded request,",
            "    # because a base-aware bundle is one that asks for its assets",
            "    # under the prefix.",
            f"    {CONSOLE_LOCATION_PREFIX} {{",
            *body,
            "    }",
            "",
            f"    {CONSOLE_LOCATION_EXACT} {{",
            *body,
            "    }",
        ]
    console_body = CONSOLE_NOT_CONFIGURED_BODY_FOR(plan).replace("\n", "\\n")
    refusal = [
        "        default_type text/plain;",
        f"        return {CONSOLE_NOT_CONFIGURED_STATUS} \"{console_body}\";",
    ]
    return [
        "    # NOT a console route. No proxy_pass, no alias, no root: nothing",
        "    # here serves the console, and nothing claims to. It exists so a",
        f"    # request for {CONSOLE_PREFIX} gets a sentence instead of falling",
        "    # through to the chat upstream and rendering its single-page",
        "    # index as a 200 that looks like a console that failed to load.",
        f"    {CONSOLE_LOCATION_PREFIX} {{",
        *refusal,
        "    }",
        "",
        f"    {CONSOLE_LOCATION_EXACT} {{",
        *refusal,
        "    }",
    ]


def render_nginx_front_door(plan: FrontDoorPlan) -> str:
    """Render the operator-deployable configuration.

    The header is load-bearing, not decoration: the PACT requires the file to
    state in its OWN generated header that it is single-operator local auth,
    so a configuration found on a host months later cannot be mistaken for the
    multi-user shape.
    """
    lines = [
        f"# Rendered by okg for deployment {plan.deployment}"
        f" (PACT chat-instance-lifecycle).",
        f"# AUTH: {SINGLE_OPERATOR_HEADER_MARKER}. This front performs NO",
        "# authentication and NO authorization: the chat instance's own local",
        "# login is the only gate. It is the honest interim shape of",
        "# design.md section 5 mechanism 1 and it is NOT the multi-user",
        "# front. When chat-sso-integration lands, `okg chat sso-check",
        "# --render nginx` REPLACES this file; do not run both.",
        "#",
        *_transport_header(plan),
        "#",
        *_console_header(plan),
        "#",
        "# This map belongs in the http context, above the server block. It is",
        "# what makes the WebSocket upgrade below conditional; a bare",
        "# `Connection: upgrade` on every request breaks keep-alive, and chat",
        "# streaming rides a WebSocket.",
        "map $http_upgrade $okg_connection_upgrade {",
        "    default upgrade;",
        "    ''      close;",
        "}",
        "",
        "server {",
        *_listen_lines(plan),
        f"    server_name {plan.hostname};",
        "",
        "    # Chat is the front door. Streaming responses must not be",
        "    # buffered or the answer arrives all at once at the end.",
        "    location / {",
        f"        proxy_pass {plan.chat_upstream};",
        "        proxy_http_version 1.1;",
        "        proxy_set_header Host $host;",
        "        proxy_set_header X-Real-IP $remote_addr;",
        "        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;",
        "        proxy_set_header X-Forwarded-Proto $scheme;",
        "        proxy_set_header Upgrade $http_upgrade;",
        "        proxy_set_header Connection $okg_connection_upgrade;",
        "        proxy_buffering off;",
        "        proxy_read_timeout 3600s;",
        "    }",
        "",
        *_console_location(plan),
        "}",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# The reference front door: the same plan, served
# ---------------------------------------------------------------------------

class _FrontDoorHandler(http.server.BaseHTTPRequestHandler):
    """Routes exactly :func:`front_door_route_table`, longest prefix first."""

    plan: FrontDoorPlan

    # Quiet: a proof run should print its assertions, not an access log.
    def log_message(self, *_args: Any) -> None:  # noqa: D102
        return

    def _console_not_configured(self) -> None:
        body = CONSOLE_NOT_CONFIGURED_BODY_FOR(self.plan).encode("utf-8")
        self.send_response(CONSOLE_NOT_CONFIGURED_STATUS)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy(self, upstream: str) -> None:
        url = f"{upstream}{self.path}"
        request = urllib.request.Request(url, method="GET")
        for name in ("Accept", "Accept-Language", "User-Agent"):
            value = self.headers.get(name)
            if value:
                request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                body = response.read()
                status = response.status
                content_type = response.headers.get(
                    "Content-Type", "application/octet-stream",
                )
        except urllib.error.HTTPError as exc:
            body = exc.read()
            status = exc.code
            content_type = exc.headers.get("Content-Type", "text/plain")
        except (urllib.error.URLError, OSError):
            # The upstream is down. 502 with a body naming the upstream, so a
            # stopped-instance control cannot be satisfied by a page that
            # looks like the site.
            body = (
                f"front door: the upstream {upstream} did not answer\n"
            ).encode("utf-8")
            status = 502
            content_type = "text/plain; charset=utf-8"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's contract
        path = urlsplit(self.path).path
        if path == CONSOLE_PREFIX or path.startswith(CONSOLE_PREFIX + "/"):
            # The same verdict the nginx renderer reads, read here, so the two
            # artifacts cannot disagree about whether a console exists. The
            # forwarded path keeps the prefix, matching the `proxy_pass` with
            # no URI part that the rendered file emits.
            if self.plan.console_permitted and self.plan.console_upstream:
                self._proxy(self.plan.console_upstream)
                return
            self._console_not_configured()
            return
        self._proxy(self.plan.chat_upstream)


class FrontDoorServer:
    """A running reference front door. Context manager; binds a real socket."""

    def __init__(self, plan: FrontDoorPlan, *, host: str = LOOPBACK_CANONICAL,
                 port: int = 0) -> None:
        handler = type("_BoundHandler", (_FrontDoorHandler,), {"plan": plan})
        self.plan = plan
        self._server = http.server.ThreadingHTTPServer((host, port), handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="okg-front-door", daemon=True,
        )
        self.host, self.port = self._server.server_address[:2]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> "FrontDoorServer":
        self._thread.start()
        return self

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> "FrontDoorServer":
        return self.start()

    def __exit__(self, *_exc: Any) -> None:
        self.close()


def serve_front_door(
    plan: FrontDoorPlan, *, host: str = LOOPBACK_CANONICAL, port: int = 0,
) -> FrontDoorServer:
    """Start the reference front door for ``plan`` and return it running.

    Refuses a plan that terminates TLS. This server speaks plain HTTP, so
    serving such a plan would put the identical defect the renderer now
    refuses — a front that reads as TLS-terminated and is not — into the proof
    surface the route table is exercised through.
    """
    if plan.tls is not None:
        raise ChatInstanceError(
            f"the reference front door serves plain HTTP and deployment "
            f"{plan.deployment!r}'s plan terminates TLS",
            data={
                "reason": "reference_front_door_terminates_no_tls",
                "deployment": plan.deployment,
                "listen_port": plan.listen_port,
            },
            fix_hint=(
                "serve the RENDERED configuration with nginx for a TLS "
                "deployment. --serve is a local bring-up and proof aid for the "
                "route table, not the production front"
            ),
        )
    return FrontDoorServer(plan, host=host, port=port).start()


def fetch(url: str, *, timeout: float = 10.0) -> tuple[int, str]:
    """One GET, returning ``(status, body)`` for BOTH success and error codes.

    The console arm asserts on the body of a 503, which ``urlopen`` raises
    rather than returns, so a helper that keeps the body is the difference
    between proving the sentence is there and proving only that something
    failed.
    """
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def free_port(host: str = LOOPBACK_CANONICAL) -> int:
    """An unbound port, from the kernel. For proofs that need one to be free.

    Never used to CHOOSE an instance's port: that is manifest-owned and a
    conflict refuses. This exists so a proof can name a port it knows nothing
    is on.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((host, 0))
        return int(probe.getsockname()[1])


def front_door_report(plan: FrontDoorPlan, configuration: str) -> Mapping[str, Any]:
    """The operator-report payload: the plan, and the file that was generated."""
    return {
        "plan": plan.as_dict(),
        "configuration": configuration,
        "auth": SINGLE_OPERATOR_HEADER_MARKER,
        "console": {
            "served": bool(plan.console_permitted),
            "owner": CONSOLE_OWNER_PACT,
            "evidence": plan.console_evidence,
            "detail": plan.console_detail,
            "upstream": plan.console_upstream,
            # The not-configured status is what the path answers when the
            # console is not served, and is reported as None when it is, so a
            # report can never be read as carrying both answers at once.
            "status": (
                None if plan.console_permitted else CONSOLE_NOT_CONFIGURED_STATUS
            ),
        },
    }


def _transport_problems(plan: FrontDoorPlan, configuration: str) -> list[str]:
    """Disagreements between the plan's transport and the rendered listener.

    Checked here rather than only in the renderer so the invariant binds any
    renderer: the file must terminate TLS exactly when the plan says it does,
    and a bare listener on a TLS-by-convention port is a disagreement even when
    the plan agrees with it.
    """
    problems: list[str] = []
    listens_tls = f"listen {plan.listen_port} ssl;" in configuration
    listens_plain = f"listen {plan.listen_port};" in configuration
    if plan.tls is not None:
        if not listens_tls:
            problems.append(
                "the plan terminates TLS and the configuration does not listen "
                "with ssl"
            )
        for directive, value in (
            ("ssl_certificate", plan.tls.certificate),
            ("ssl_certificate_key", plan.tls.private_key),
        ):
            if f"{directive} {value};" not in configuration:
                problems.append(
                    f"the configuration's {directive} does not name the plan's "
                    f"material"
                )
        if f"ssl_protocols {_TLS_PROTOCOLS};" not in configuration:
            problems.append("the configuration sets no TLS protocol floor")
    else:
        if listens_tls:
            problems.append(
                "the configuration listens with ssl and the plan has no "
                "certificate or key"
            )
        if listens_plain and plan.listen_port in _TLS_BY_CONVENTION:
            problems.append(
                f"the configuration listens plain on {plan.listen_port}, a port "
                f"clients speak TLS to: it would read as TLS-terminated and "
                f"answer a ClientHello as a bad HTTP request"
            )
    return problems


def routes_agree(plan: FrontDoorPlan, configuration: str) -> Sequence[str]:
    """Disagreements between the rendered nginx and the plan's route table.

    Empty means the file an operator deploys and the proxy the proof exercised
    implement the same two routes. A non-empty result is the whole reason this
    function exists: two renderers over one plan is a design that only holds
    while something checks it.
    """
    problems: list[str] = []
    problems.extend(_transport_problems(plan, configuration))

    # Scoped to the ROOT location, not searched over the whole file. While
    # there was exactly one proxy_pass in the document a whole-file search was
    # the same check; once the console can carry a second one, an operator
    # whose console upstream happens to equal the chat upstream would satisfy
    # this from the console block alone, and a root location pointed anywhere
    # else would go unreported.
    root_block = _root_directives(configuration)
    if not root_block:
        problems.append("nginx has no root location, so / would not be served")
    elif f"proxy_pass {plan.chat_upstream};" not in root_block:
        problems.append("nginx does not proxy / to the plan's chat upstream")

    blocks = console_blocks(configuration)
    if len(blocks) != 2:
        problems.append(
            f"nginx has {len(blocks)} console location(s), not the exact-plus-"
            f"prefix pair the served handler's matching needs"
        )
    for console_block in blocks or [""]:
        if plan.console_permitted:
            # The permitted shape is checked as tightly as the refused one, and
            # in both directions: the console must reach the plan's upstream,
            # and it must not ALSO answer not-configured, which would leave a
            # file whose first matching directive decides a question the plan
            # already answered.
            if f"proxy_pass {plan.console_upstream};" not in console_block:
                problems.append(
                    "the plan permits the console and nginx's console location "
                    "does not proxy to its upstream"
                )
            if f"return {CONSOLE_NOT_CONFIGURED_STATUS}" in console_block:
                problems.append(
                    "the plan permits the console and nginx's console location "
                    "still returns not-configured"
                )
            continue
        # Scoped to the console block for the same reason the root check is:
        # a `return 503` anywhere else in the file used to satisfy this while
        # the console location served a 404.
        if f"return {CONSOLE_NOT_CONFIGURED_STATUS}" not in console_block:
            problems.append(
                "nginx's console location does not return not-configured"
            )
        for serving in ("proxy_pass", "alias", "root", "try_files"):
            if serving in console_block:
                problems.append(
                    f"nginx's console location contains {serving!r}: it claims "
                    "to serve a console that does not boot under a prefix"
                )
    return problems


#: Quoted strings, stripped before a block is scanned for DIRECTIVES. The
#: not-configured body is a quoted argument to `return`, and its English
#: prose can legitimately contain words like `root` that are directive names
#: elsewhere. Scanning raw text would report the body as a route.
_QUOTED = re.compile(r'"[^"]*"')


def _block_after(configuration: str, opener: str) -> str:
    """The directives of the block ``opener`` opens, quoted arguments removed."""
    parts = configuration.split(f"{opener} {{", 1)
    if len(parts) != 2:
        return ""
    return _QUOTED.sub('""', parts[1].split("}", 1)[0])


def _root_directives(configuration: str) -> str:
    return _block_after(configuration, "location /")


def console_blocks(configuration: str) -> list[str]:
    """Both console locations' directives — the exact one and the prefix one.

    Returned as a LIST rather than concatenated so every check runs against
    each block independently: two blocks that disagree with each other is one
    of the ways a file can route ``/console`` and ``/console/x`` differently,
    and a joined string would hide it.
    """
    return [
        block
        for block in (
            _block_after(configuration, CONSOLE_LOCATION_PREFIX),
            _block_after(configuration, CONSOLE_LOCATION_EXACT),
        )
        if block
    ]


def console_directives(configuration: str) -> str:
    """Every console location's directives, joined. Kept for callers that only
    ask "does anything here serve the console"."""
    return "\n".join(console_blocks(configuration))


def front_door_json(plan: FrontDoorPlan, configuration: str) -> str:
    return json.dumps(front_door_report(plan, configuration), indent=2, sort_keys=True)
