"""One SSO session in front of both graphical surfaces (PACT chat-sso-integration).

design.md section 5 mechanism 4 and section 7 Plane 1: the chat site and the
operator console sit behind ONE identity provider, so a person logs in once
and both surfaces answer; logging out ends both.

**Division of labour, stated so it is not re-blurred.** PACT
``mcp-principal-mapping`` (``okg.substrate.mcp.principal_mapping``) owns the
TRUST MODEL: how a per-request principal claim is verified, which headers are
identity, what the server stamps as ``verified_by``/``verified_at``, and which
refusals fire when a principal cannot be resolved. This module owns the IdP
CONFIGURATION that produces those claims: declaring the mode, naming the
provider's endpoints, fronting both surfaces with one session, and projecting
the deployment's groups (``sso_groups.py``). Nothing here relaxes a refusal
over there. Several checks below exist only to make one of those refusals fire
EARLIER, at bring-up, where the operator can read it.

**The console is not modified.** Its authentication is enforced here, at the
front, exactly as ``deployments/docs-site/nginx.conf`` already fronts the docs
site with ``auth_request`` + oauth2-proxy. The console keeps its own
application, contract and codebase (design.md section 5 mechanism 1); this
module never imports it.

**Where the console lives is the operator's choice, gated on evidence rather
than on shape.** Publishing it under a path prefix on the chat hostname is the
intended one-hostname topology and is what design.md section 10 decision 11
resolves with a base-aware build. So the prefix is PERMITTED; what
:func:`console_base_awareness` gates is whether the bundle behind it asks for
its assets somewhere the prefix serves them. With the built ``index.html`` in
reach that is MEASURED; without it, an explicit operator declaration is a
capability gate whose verdict says it is a declaration and not a measurement;
with neither, it refuses, because the failure it guards against is a console
that answers 200 and renders nothing. A gate on the topology instead would
refuse the very fix decision 11 prescribes.

**What is real HTTP and what is a template.** :class:`SsoFrontProxy` is a
working forwarding proxy used to VERIFY the contract end to end (and usable
for local bring-up). Production topology is the rendered nginx +
oauth2-proxy pair (:func:`render_nginx_site`, :func:`render_oauth2_proxy_cfg`),
which implements the same decisions in the operator's own proxy. The reference
proxy is deliberately plain HTTP/1.1 request-response: it does not carry
WebSocket upgrades, so it is a verification and development surface, never the
production front for an Open WebUI instance that needs them.
"""
from __future__ import annotations

import re
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, Sequence

from okg.substrate.deployments.chat_block import (
    SSO_IDP_ELEMENTS,
    SSO_MODE_MULTI_USER,
    SSO_MODE_SINGLE_OPERATOR,
    ChatBlock,
    ChatSso,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "AUTHORIZED",
    "CHAT_SURFACE",
    "CONSOLE_EVIDENCE_ABSENT",
    "CONSOLE_EVIDENCE_DECLARED",
    "CONSOLE_EVIDENCE_MEASURED",
    "CONSOLE_SURFACE",
    "ConsoleBuild",
    "ConsoleBuildVerdict",
    "DENY",
    "console_base_awareness",
    "GROUP_MEMBERSHIP_ABSENT",
    "IdpIdentity",
    "IdentityProviderClient",
    "OidcDiscoveryClient",
    "PROXY_CALLBACK_PATH",
    "PROXY_SIGN_IN_PATH",
    "PROXY_SIGN_OUT_PATH",
    "REDIRECT",
    "SERVE",
    "SESSION_COOKIE_NAME",
    "SURFACES",
    "SsoFrontProxy",
    "SsoSession",
    "SsoStartupDecision",
    "ChatSsoStartupError",
    "SessionStore",
    "SurfaceDecision",
    "SurfaceRoute",
    "authorize_surface",
    "membership_state",
    "render_nginx_site",
    "render_oauth2_proxy_cfg",
    "require_sso_startup_contract",
    "resolve_client_secret",
    "sanitize_inbound_headers",
    "surface_routes_from_deep_links",
]


CHAT_SURFACE = "chat"
CONSOLE_SURFACE = "console"
SURFACES: tuple[str, ...] = (CHAT_SURFACE, CONSOLE_SURFACE)

#: Outcomes. Three, and only three: there is no "serve a reduced view".
SERVE = "serve"
REDIRECT = "redirect"
DENY = "deny"

AUTHORIZED = "chat.sso.authorized"
UNAUTHENTICATED = "chat.sso.unauthenticated"
SESSION_EXPIRED = "chat.sso.session_expired"
GROUP_MEMBERSHIP_ABSENT = "chat.sso.group_membership_absent"

#: One cookie, both surfaces. This single name is why one login reaches two
#: hostnames and why one sign-out ends both: the session it keys is stored
#: once, so there is no second place for a stale copy to survive.
SESSION_COOKIE_NAME = "okg_sso_session"

#: Binds an in-flight login to the browser that started it. Without it,
#: ``state`` is only a lookup key: anyone who learns a live value can present
#: it with a code and be handed the session cookie.
LOGIN_NONCE_COOKIE_NAME = "okg_sso_login"

#: A login nobody completed is garbage after this long.
_PENDING_LOGIN_TTL_SECONDS = 600

#: Pending logins are bounded so an unauthenticated endpoint cannot grow the
#: process's memory. The bound REFUSES rather than evicting -- evicting would
#: drop a real person's in-flight login to make room for a flood.
_MAX_PENDING_LOGINS = 1024

#: Emitted when ``rd`` names somewhere this deployment does not publish.
REDIRECT_OFF_SITE = "chat.sso.return_to_off_site"

#: The claims a front can actually put in the principal header, mapped to
#: the nginx variable that carries each one.
#:
#: oauth2-proxy's ``set_xauthrequest`` publishes the signed-in user's email
#: and preferred username, and nothing else the substrate would want as a
#: principal. Declaring ``subject_claim: sub`` would therefore be a
#: deployment whose front cannot name its own principals -- refused at
#: bring-up rather than discovered when every request is refused as
#: unidentified.
FRONT_SUPPLIED_CLAIMS: Mapping[str, str] = {
    "email": "$auth_email",
    "preferred_username": "$auth_user",
}

#: Proxy-owned paths, named after the oauth2-proxy vocabulary this repo
#: already fronts the docs site with (``deployments/docs-site/nginx.conf``),
#: so an operator reads one convention across both sites.
PROXY_SIGN_IN_PATH = "/oauth2/start"
PROXY_CALLBACK_PATH = "/oauth2/callback"
PROXY_SIGN_OUT_PATH = "/oauth2/sign_out"
_PROXY_PATHS = frozenset({
    PROXY_SIGN_IN_PATH,
    PROXY_CALLBACK_PATH,
    PROXY_SIGN_OUT_PATH,
})

#: Sessions expire. An 8 hour ceiling matches the docs-site oauth2-proxy
#: ``cookie_expire`` already in the tree, so the two fronts age alike.
DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60


class ChatSsoStartupError(SubstrateError):
    """The site refuses to come up under the declared identity model."""

    code = "chat_sso_startup_refused"


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------

def resolve_client_secret(
    sso: ChatSso,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve the IdP client secret from the operator environment.

    Fail-closed and value-blind, the same contract ``mcp-http-auth`` applies
    to its bearer token: a named-but-unresolvable secret refuses to start
    rather than starting an SSO that cannot complete a code exchange, and
    neither the message nor the ``data`` payload ever carries the value.
    """
    import os

    env = os.environ if environ is None else environ
    name = sso.client_secret_env or ""
    if not name:
        raise ChatSsoStartupError(
            "chat.sso declares no client_secret_env, so the identity "
            "provider's client secret has no source; refusing to start the "
            "fronted site",
            data={"reason": "client_secret_env_absent"},
            fix_hint=(
                "declare chat.sso.client_secret_env with the NAME of an "
                "environment variable holding the secret"
            ),
        )
    raw = env.get(name)
    if raw is None:
        raise ChatSsoStartupError(
            f"chat.sso.client_secret_env names environment variable "
            f"{name!r}, but that variable is not set; refusing to start the "
            "fronted site with an identity provider it cannot authenticate "
            "to",
            data={"client_secret_env": name, "reason": "unset"},
            fix_hint=f"export {name} in the operator environment",
        )
    if not raw.strip():
        raise ChatSsoStartupError(
            f"chat.sso.client_secret_env names environment variable "
            f"{name!r}, but that variable is empty or whitespace-only; "
            "refusing to start unauthenticated to the identity provider",
            data={"client_secret_env": name, "reason": "empty"},
            fix_hint=f"set {name} to the client secret issued by the provider",
        )
    if raw != raw.strip():
        # The classic `SECRET=$(cat file)` trailing newline. Stripping it
        # silently would make the secret sent differ from the secret
        # configured, and the resulting provider rejection reads as a wrong
        # client id.
        raise ChatSsoStartupError(
            f"chat.sso.client_secret_env names environment variable "
            f"{name!r}, but its value has leading or trailing whitespace",
            data={"client_secret_env": name, "reason": "padded"},
            fix_hint=(
                f"set {name} to the exact secret with no surrounding "
                "whitespace"
            ),
        )
    return raw


# ---------------------------------------------------------------------------
# Startup contract
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SsoStartupDecision:
    """What the deployment's identity model is, once checked.

    Built only by :func:`require_sso_startup_contract`. ``multi_user`` is
    never computed from the presence of configuration; it restates the
    declared mode after the declaration survived every check.
    """

    deployment: str
    mode: str
    multi_user: bool
    sso: ChatSso | None
    principal_header: str | None = None
    projected_groups: tuple[str, ...] = ()
    routes: tuple["SurfaceRoute", ...] = ()
    #: ``nomos.principal_mapping.subject_claim``. Carried so the front reads
    #: the principal out of the SAME claim the deployment's subject bindings
    #: are written against, rather than picking one by what is present.
    subject_claim: str | None = None
    extra_claim_headers: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "mode": self.mode,
            "multi_user": self.multi_user,
            "principal_header": self.principal_header,
            "subject_claim": self.subject_claim,
            "projected_groups": list(self.projected_groups),
            "routes": [route.to_dict() for route in self.routes],
            "sso": self.sso.to_dict() if self.sso else None,
        }


def require_sso_startup_contract(
    *,
    deployment: str,
    chat: ChatBlock | None,
    principal_mapping: Any | None,
    environ: Mapping[str, str] | None = None,
    check_secret: bool = True,
    console_build: ConsoleBuild | None = None,
) -> SsoStartupDecision:
    """Decide the deployment's identity model, or refuse to bring the site up.

    Every refusal below is a way the two halves of the identity model could
    disagree while each half individually looked fine. They are checked HERE,
    at the SSO surface, and nowhere else: ``mcp-serve`` keeps its own
    contract unchanged, so a deployment that uses ``nomos.principal_mapping``
    without a chat site is unaffected.

    The checks, in order, and what each one stops:

    * **No ``chat.sso`` at all** — the mode was never declared, so the front
      has nothing to enforce. Refused rather than assumed.
    * **``multi_user`` with no ``nomos.principal_mapping``** — the site would
      authenticate people and then hand ``mcp-serve`` no principal, so every
      user collapses onto the process identity. That is precisely the state
      ``mcp-principal-mapping`` exists to end.
    * **``single_operator`` with a declared principal mapping** — the mirror:
      the MCP surface is per-caller, the site says one operator. One of the
      two is wrong and neither is safe to pick.
    * **A prefixed console with no evidence about its bundle** — see
      :func:`console_base_awareness`. The prefix TOPOLOGY is permitted;
      serving it on no evidence is not.
    * **Header skew** — the proxy stamps ``chat.sso.principal_header``;
      ``mcp-serve`` reads ``nomos.principal_mapping.subject_header``. If they
      differ, every request is refused ``header_absent``, which reads like a
      client bug rather than a manifest one.
    * **Provider skew** — issuer/audience/provider drift produces per-request
      denies that look like policy decisions (the same reasoning
      ``validate_mapping_against_providers`` gives for its own skew check).
    * **Group claim carried as a header** — refused here as well as there,
      so the SSO-side reason is stated: groups are PROJECTED server-side;
      a group a caller can name in a header is a group a caller can grant
      itself.
    * **No transport authentication** — ``multi_user_requires_auth_error``
      would refuse at ``mcp-serve`` start. Naming it at site bring-up means
      the operator sees it while reading the manifest that caused it.
    """
    if chat is None or not chat.enabled:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} has no enabled chat: block, so there "
            "is no fronted site to bring up",
            data={"deployment": deployment, "reason": "chat_disabled"},
            fix_hint="declare chat.enabled: true with a chat.sso block",
        )
    sso = chat.sso
    mapping_declared = principal_mapping is not None
    if sso is None:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} declares no chat.sso block, so its "
            "identity model is undeclared. It is not inferred from what else "
            "is configured"
            + (
                " -- note that nomos.principal_mapping IS declared, which "
                "makes the MCP surface per-caller while the site says "
                "nothing"
                if mapping_declared
                else ""
            ),
            data={
                "deployment": deployment,
                "reason": "sso_undeclared",
                "principal_mapping_declared": mapping_declared,
                "modes": [SSO_MODE_SINGLE_OPERATOR, SSO_MODE_MULTI_USER],
            },
            fix_hint=(
                f"declare chat.sso.mode: {SSO_MODE_MULTI_USER} with every "
                f"element of {list(SSO_IDP_ELEMENTS)}, or chat.sso.mode: "
                f"{SSO_MODE_SINGLE_OPERATOR}"
            ),
        )

    if not sso.multi_user:
        if mapping_declared:
            raise ChatSsoStartupError(
                f"deployment {deployment!r} declares chat.sso.mode: "
                f"{SSO_MODE_SINGLE_OPERATOR} while nomos.principal_mapping "
                "is declared. The MCP surface would identify each caller "
                "separately while the site authenticates nobody, so the two "
                "halves of the identity model disagree",
                data={
                    "deployment": deployment,
                    "reason": "mode_mapping_skew",
                    "mode": sso.mode,
                },
                fix_hint=(
                    f"set chat.sso.mode: {SSO_MODE_MULTI_USER}, or remove "
                    "nomos.principal_mapping"
                ),
            )
        return SsoStartupDecision(
            deployment=deployment,
            mode=sso.mode,
            multi_user=False,
            sso=sso,
        )

    if not mapping_declared:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} declares chat.sso.mode: "
            f"{SSO_MODE_MULTI_USER} but no nomos.principal_mapping. The site "
            "would authenticate each person and then present every one of "
            "them to mcp-serve as the same principal, which is the "
            "single-principal state PACT mcp-principal-mapping exists to end",
            data={
                "deployment": deployment,
                "reason": "principal_mapping_absent",
                "mode": sso.mode,
            },
            fix_hint=(
                "declare nomos.principal_mapping (provider_id, issuer, "
                "audience, subject_claim, subject_header, principal_kind)"
            ),
        )

    skew: dict[str, dict[str, Any]] = {}
    mapping_header = str(getattr(principal_mapping, "subject_header", "") or "")
    if mapping_header != sso.principal_header:
        skew["principal_header"] = {
            "chat.sso": sso.principal_header,
            "nomos.principal_mapping.subject_header": mapping_header,
        }
    for manifest_key, mapping_attr in (
        ("provider_id", "provider_id"),
        ("issuer", "issuer"),
        ("audience", "audience"),
    ):
        mapped = str(getattr(principal_mapping, mapping_attr, "") or "")
        declared = getattr(sso, manifest_key)
        if mapped != declared:
            skew[manifest_key] = {
                "chat.sso": declared,
                f"nomos.principal_mapping.{mapping_attr}": mapped,
            }
    if skew:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} declares a chat.sso block that "
            "disagrees with nomos.principal_mapping. The proxy stamps what "
            "chat.sso names and mcp-serve reads what the mapping names, so "
            "every request would be refused for a mismatch that reads as a "
            "client fault",
            data={
                "deployment": deployment,
                "reason": "principal_mapping_skew",
                "mode": sso.mode,
                "skew": skew,
            },
            fix_hint=(
                "align chat.sso.principal_header / provider_id / issuer / "
                "audience with nomos.principal_mapping"
            ),
        )

    extra_headers = dict(
        getattr(principal_mapping, "extra_claim_headers", {}) or {}
    )
    conflicting = sorted(
        claim
        for claim in extra_headers
        if claim.strip().lower() == (sso.groups_claim or "").strip().lower()
    )
    if conflicting:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} declares the group claim "
            f"{sso.groups_claim!r} as an extra principal header. Group facts "
            "are projected server-side from the identity provider's claims "
            "(sso_groups.py); a group a caller can name in a header is a "
            "group a caller can grant itself",
            data={
                "deployment": deployment,
                "reason": "group_claim_as_header",
                "groups_claim": sso.groups_claim,
                "conflicting_claims": conflicting,
            },
            fix_hint=(
                "remove the claim from nomos.principal_mapping."
                "extra_claim_headers; the projection is the group channel"
            ),
        )

    auth_token_env = getattr(chat.mcp, "auth_token_env", None) if chat.mcp else None
    if not auth_token_env:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} declares chat.sso.mode: "
            f"{SSO_MODE_MULTI_USER} but chat.mcp.auth_token_env is absent, "
            "so the MCP transport authenticates nobody. A principal header "
            "asserted over an unauthenticated transport is a claim anyone "
            "who can reach the port may forge -- mcp-serve refuses to start "
            "in that state, and so does the site",
            data={
                "deployment": deployment,
                "reason": "transport_auth_absent",
                "mode": sso.mode,
            },
            fix_hint=(
                "declare chat.mcp.auth_token_env with the NAME of the "
                "variable holding the bearer token, and serve with "
                "--auth-token-env that name"
            ),
        )

    subject_claim = str(getattr(principal_mapping, "subject_claim", "") or "")
    if subject_claim not in FRONT_SUPPLIED_CLAIMS:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} declares "
            f"nomos.principal_mapping.subject_claim {subject_claim!r}, which "
            "the fronting proxy cannot supply. The front can only name the "
            f"principal from a claim it receives: {sorted(FRONT_SUPPLIED_CLAIMS)}. "
            "A claim outside that set would leave the front stamping one "
            "identifier while the deployment's subject bindings are written "
            "against another, and every authenticated person would be refused "
            "as unidentified",
            data={
                "deployment": deployment,
                "reason": "subject_claim_unsuppliable",
                "subject_claim": subject_claim,
                "supported": sorted(FRONT_SUPPLIED_CLAIMS),
            },
            fix_hint=(
                "set nomos.principal_mapping.subject_claim to one of "
                f"{sorted(FRONT_SUPPLIED_CLAIMS)}"
            ),
        )

    routes = surface_routes_from_deep_links(
        chat.deep_links, deployment=deployment, console_build=console_build,
    )
    if check_secret:
        resolve_client_secret(sso, environ=environ)
    return SsoStartupDecision(
        deployment=deployment,
        mode=sso.mode,
        multi_user=True,
        sso=sso,
        principal_header=sso.principal_header,
        projected_groups=sso.projected_groups,
        routes=routes,
        subject_claim=subject_claim,
        extra_claim_headers=tuple(sorted(extra_headers.values())),
    )


# ---------------------------------------------------------------------------
# Surface routing
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SurfaceRoute:
    """Where one surface lives, as the front sees it."""

    surface: str
    host: str
    path_prefix: str = ""

    def matches(self, host: str, path: str) -> bool:
        if _bare_host(host) != _bare_host(self.host):
            return False
        if not self.path_prefix:
            return True
        return path == self.path_prefix or path.startswith(
            self.path_prefix.rstrip("/") + "/"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "surface": self.surface,
            "host": self.host,
            "path_prefix": self.path_prefix,
        }


def _bare_host(host: str) -> str:
    return str(host or "").strip().lower()


@dataclass(frozen=True)
class ConsoleBuild:
    """What is known about the console bundle the operator is serving.

    Operator-state topology, so it arrives as an argument rather than out of
    the manifest: which bytes are behind the prefix is a property of the
    deployment's checkout and its build, not of its identity.

    ``index_path`` is the built ``index.html``. When it is reachable the
    bundle is MEASURED and ``declared_base_aware`` is not consulted for the
    permit -- a declaration contradicted by the file is worth an error, not a
    tie-break.
    """

    declared_base_aware: bool = False
    index_path: Any = None


@dataclass(frozen=True)
class ConsoleBuildVerdict:
    """Whether a prefixed console may be served, and on what evidence."""

    permitted: bool
    evidence: str
    detail: str = ""
    data: Mapping[str, Any] = field(default_factory=dict)
    fix_hint: str | None = None


#: Evidence kinds, so a caller can tell a measurement from a promise.
CONSOLE_EVIDENCE_MEASURED = "bundle_measured"
CONSOLE_EVIDENCE_DECLARED = "operator_declared"
CONSOLE_EVIDENCE_ABSENT = "no_evidence"

#: `src=` / `href=` in the built index. Enough to see where the app asks for
#: its own assets, which is the whole question.
_ASSET_REF_RE = re.compile(r"""(?:src|href)\s*=\s*["']([^"']+)["']""", re.I)


def console_base_awareness(
    console_prefix: str,
    build: ConsoleBuild | None,
    *,
    deployment: str,
) -> ConsoleBuildVerdict:
    """Decide whether a bundle can be served under ``console_prefix``.

    The question is not "is the console on a path?" -- design decision 11
    picks the base-aware build as the resolution of the ``/console`` question,
    and hash routing is already prefix-safe, so a prefixed console is the
    SANCTIONED topology. The question is whether THIS bundle asks for its
    assets somewhere the prefix serves them.

    Three states, ordered by the strength of what is known:

    * **Measured.** With the built ``index.html`` in hand, every root-relative
      asset reference is checked. A reference that is relative (Vite
      ``base: './'``) or already carries the prefix (``base: '/console/'``) is
      prefix-safe; one that points at the origin root is the falsified build
      the handshake measured, and is refused with the offending path named.
      Measurement beats declaration in both directions: a bundle that is
      base-aware is permitted even if nobody declared it, and one that is not
      is refused even if somebody did.
    * **Declared.** With no bundle to read -- the front frequently runs
      somewhere the console's files are not -- an explicit operator
      declaration permits it. This is a capability gate, not a proof, and the
      verdict says so, so nothing downstream can report a promise as a
      measurement.
    * **Absent.** No bundle and no declaration is not "probably fine". The
      failure it guards against is a console that answers 200 with a blank
      page, which is exactly the silent failure a default-open would produce.
      Refused, naming both ways to satisfy it.
    """
    prefix = console_prefix.rstrip("/")
    index_path = getattr(build, "index_path", None) if build else None
    declared = bool(getattr(build, "declared_base_aware", False)) if build else False

    if index_path is not None:
        from pathlib import Path

        path = Path(str(index_path))
        try:
            markup = path.read_text(encoding="utf-8", errors="replace")
        except OSError as err:
            return ConsoleBuildVerdict(
                permitted=False,
                evidence=CONSOLE_EVIDENCE_ABSENT,
                detail=(
                    f"deployment {deployment!r} publishes the console under "
                    f"{prefix!r} and names a bundle index at {path} that "
                    f"cannot be read ({err}). An unreadable bundle is not a "
                    "base-aware one; it is a check that did not happen"
                ),
                data={
                    "deployment": deployment,
                    "reason": "console_bundle_unreadable",
                    "console_prefix": prefix,
                    "index_path": str(path),
                },
                fix_hint="point --console-bundle-index at the built index.html",
            )
        references = _ASSET_REF_RE.findall(markup)
        if not references:
            # "Found no offending reference" is not "found references and all
            # were safe". An index with no `src=`/`href=` at all -- an empty
            # file, a truncated write, the wrong file -- would otherwise be
            # PERMITTED on evidence `bundle_measured`, which is absence
            # recorded as a measurement. It was inert while nothing read this
            # verdict; a front door that routes on it makes it a served route
            # labelled as measured.
            return ConsoleBuildVerdict(
                permitted=False,
                evidence=CONSOLE_EVIDENCE_ABSENT,
                detail=(
                    f"the console bundle at {path} references no assets at "
                    f"all, so there is nothing to measure base-awareness "
                    f"against. A built index requests at least its own script; "
                    f"one that requests nothing is an empty, truncated or "
                    f"wrong file, not a bundle proved fit for {prefix!r}"
                ),
                data={
                    "deployment": deployment,
                    "reason": "console_bundle_references_no_assets",
                    "console_prefix": prefix,
                    "index_path": str(path),
                    "declared_base_aware": declared,
                },
                fix_hint=(
                    "point --console-bundle-index at the BUILT index.html (the "
                    "one under the build output directory), not at a source "
                    "template or an empty file"
                ),
            )
        offending = [
            ref
            for ref in references
            if _is_root_relative(ref) and not _under_prefix(ref, prefix)
        ]
        if offending:
            return ConsoleBuildVerdict(
                permitted=False,
                evidence=CONSOLE_EVIDENCE_MEASURED,
                detail=(
                    f"deployment {deployment!r} publishes the console under "
                    f"{prefix!r}, but its bundle asks for assets at the origin "
                    f"root: {sorted(set(offending))[:4]}. Those requests are "
                    "issued by the BROWSER against the origin, so the prefix "
                    "never sees them -- they 404 and the app never boots, "
                    "while the front answers 200 with a blank page. This is "
                    "the build the read-handshake spike measured"
                ),
                data={
                    "deployment": deployment,
                    "reason": "console_bundle_not_base_aware",
                    "console_prefix": prefix,
                    "index_path": str(path),
                    "root_relative_assets": sorted(set(offending)),
                    "declared_base_aware": declared,
                    "evidence": (
                        "pact/changes/openwebui-chat-frontend/evidence/"
                        "read-handshake-transcript.md section 7"
                    ),
                },
                fix_hint=(
                    f"rebuild the console with Vite base='{prefix}/' (design "
                    "decision 11; hash routing is already prefix-safe), or "
                    "publish it at the root of its own hostname"
                ),
            )
        return ConsoleBuildVerdict(
            permitted=True,
            evidence=CONSOLE_EVIDENCE_MEASURED,
            detail=(
                f"the console bundle at {path} references no assets outside "
                f"{prefix!r}, so it boots under the prefix"
            ),
            data={
                "deployment": deployment,
                "console_prefix": prefix,
                "index_path": str(path),
            },
        )

    if declared:
        return ConsoleBuildVerdict(
            permitted=True,
            evidence=CONSOLE_EVIDENCE_DECLARED,
            detail=(
                f"the operator declares a base-aware console build under "
                f"{prefix!r}. Declared, not measured: no bundle was reachable "
                "from here to check it against"
            ),
            data={
                "deployment": deployment,
                "console_prefix": prefix,
                "declared_base_aware": True,
            },
        )

    return ConsoleBuildVerdict(
        permitted=False,
        evidence=CONSOLE_EVIDENCE_ABSENT,
        detail=(
            f"deployment {deployment!r} publishes the console under the path "
            f"prefix {prefix!r}, and nothing here says whether its bundle is "
            "base-aware. A bundle that is not asks for its assets at the "
            "origin root, which 404 under a prefix -- the front answers 200 "
            "and the console renders nothing, with no error anywhere. The "
            "prefix topology is permitted (design decision 11); what is "
            "refused is serving it on no evidence"
        ),
        data={
            "deployment": deployment,
            "reason": "console_base_awareness_undeclared",
            "console_prefix": prefix,
            "evidence": (
                "pact/changes/openwebui-chat-frontend/evidence/"
                "read-handshake-transcript.md section 7"
            ),
        },
        fix_hint=(
            "pass --console-bundle-index <path to the built index.html> so "
            "the build is measured, or --console-base-aware to declare it; "
            "or publish the console at the root of its own hostname"
        ),
    )


def _is_root_relative(ref: str) -> bool:
    """True for a reference the browser resolves against the ORIGIN root.

    Those are the only ones a prefix can strand. A scheme-relative ``//host``
    or an absolute URL goes somewhere else entirely and is not this check's
    business; a relative ``assets/x.js`` resolves against the current
    directory, which IS the prefix, so it is already safe.
    """
    return ref.startswith("/") and not ref.startswith("//")


def _under_prefix(ref: str, prefix: str) -> bool:
    return ref == prefix or ref.startswith(prefix + "/")


def surface_routes_from_deep_links(
    deep_links: Any,
    *,
    deployment: str,
    console_build: "ConsoleBuild | None" = None,
) -> tuple[SurfaceRoute, ...]:
    """Derive both surfaces' routes from the declared public base URLs.

    The topology is not invented here: ``chat.deep_links`` already declares
    where each surface is published (design.md section 4), so the front reads
    the same two facts the deep links are built from rather than a third
    declaration that could drift.

    **Publishing the console under a path prefix is permitted, and gated on
    the property that actually decides whether it works.** The read-handshake
    spike (``pact/changes/openwebui-chat-frontend/evidence/
    read-handshake-transcript.md`` section 7) served the pinned console build
    behind ``/console/`` and got a blank page: that bundle's index requests
    its assets at the ORIGIN ROOT, which the chat site owns in that topology,
    so every asset 404s and the app never boots while the front answers 200.

    What that measured is one BUILD, not the topology. Design decision 11
    picks the base-aware build as the resolution, and hash routing is already
    prefix-safe, so a bundle built with Vite's ``base`` works under a prefix.
    Refusing the shape would refuse the sanctioned fix. The gate is therefore
    :func:`console_base_awareness`, on the bundle -- see it for what counts as
    evidence and why an undeclared, unverifiable bundle still refuses.
    """
    chat_base = getattr(deep_links, "chat_site_base", None) if deep_links else None
    console_base = (
        getattr(deep_links, "operator_ui_base", None) if deep_links else None
    )
    missing = [
        name
        for name, value in (
            ("chat_site_base", chat_base),
            ("operator_ui_base", console_base),
        )
        if not value
    ]
    if missing:
        raise ChatSsoStartupError(
            f"deployment {deployment!r} runs multi-user but declares no "
            f"chat.deep_links.{' and no chat.deep_links.'.join(missing)}. "
            "The front needs both surfaces' public base URLs to route one "
            "session to both",
            data={
                "deployment": deployment,
                "reason": "surface_base_absent",
                "missing": missing,
            },
            fix_hint=(
                "declare chat.deep_links.chat_site_base and "
                "chat.deep_links.operator_ui_base"
            ),
        )
    chat_parts = urllib.parse.urlsplit(str(chat_base))
    console_parts = urllib.parse.urlsplit(str(console_base))
    for label, parts, raw in (
        ("chat_site_base", chat_parts, chat_base),
        ("operator_ui_base", console_parts, console_base),
    ):
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            raise ChatSsoStartupError(
                f"deployment {deployment!r} declares chat.deep_links.{label} "
                f"= {raw!r}, which is not an absolute http(s) base URL",
                data={
                    "deployment": deployment,
                    "reason": "surface_base_malformed",
                    "path": f"chat.deep_links.{label}",
                },
                fix_hint=f"declare {label} as e.g. https://console.example.edu",
            )
    chat_prefix = chat_parts.path.rstrip("/")
    console_prefix = console_parts.path.rstrip("/")
    if console_prefix:
        # Keyed on the BUNDLE, not on whether the hosts happen to match. A
        # prefix is equally broken on its own hostname if the build is not
        # base-aware -- the assets 404 against that host's root either way --
        # and equally fine on a shared one if it is.
        verdict = console_base_awareness(
            console_prefix,
            console_build,
            deployment=deployment,
        )
        if not verdict.permitted:
            raise ChatSsoStartupError(verdict.detail, data=verdict.data, fix_hint=verdict.fix_hint)
    return (
        SurfaceRoute(
            surface=CHAT_SURFACE,
            host=chat_parts.netloc,
            path_prefix=chat_prefix,
        ),
        SurfaceRoute(
            surface=CONSOLE_SURFACE,
            host=console_parts.netloc,
            path_prefix=console_prefix,
        ),
    )


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SsoSession:
    """One authenticated person. ONE of these serves both surfaces."""

    session_id: str
    principal: str
    groups: tuple[str, ...]
    issued_at: float
    expires_at: float

    def expired(self, *, now: float) -> bool:
        return now >= self.expires_at


class SessionStore:
    """The single place a session lives.

    "Logout invalidates both surfaces" is a property of there being ONE
    store, not of two invalidations that must both be remembered. Nothing
    here caches a decision per surface, so no surface can keep serving a
    session another surface ended.
    """

    def __init__(self, *, ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS) -> None:
        self._ttl = int(ttl_seconds)
        self._sessions: dict[str, SsoSession] = {}
        self._lock = threading.Lock()

    def create(
        self,
        *,
        principal: str,
        groups: Sequence[str],
        now: float | None = None,
    ) -> SsoSession:
        moment = time.time() if now is None else now
        session = SsoSession(
            session_id=secrets.token_urlsafe(32),
            principal=principal,
            groups=tuple(groups),
            issued_at=moment,
            expires_at=moment + self._ttl,
        )
        with self._lock:
            self._sessions[session.session_id] = session
        return session

    def get(self, session_id: str | None) -> SsoSession | None:
        if not session_id:
            return None
        with self._lock:
            return self._sessions.get(session_id)

    def invalidate(self, session_id: str | None) -> bool:
        if not session_id:
            return False
        with self._lock:
            return self._sessions.pop(session_id, None) is not None

    def __len__(self) -> int:  # pragma: no cover - trivial
        with self._lock:
            return len(self._sessions)


# ---------------------------------------------------------------------------
# The authorization decision
# ---------------------------------------------------------------------------

MEMBER_ADMIN = "admin"
MEMBER_USER = "user"
MEMBER_NONE = "none"


def membership_state(groups: Iterable[str], sso: ChatSso) -> str:
    """Which of the three membership states this person is in.

    Admin wins when someone holds both, because the admin group is a
    superset of capability, not an alternative to it. "Neither" is its own
    value and is never spelled as an empty admin check.
    """
    held = {str(name) for name in groups}
    if sso.admin_group and sso.admin_group in held:
        return MEMBER_ADMIN
    if sso.user_group and sso.user_group in held:
        return MEMBER_USER
    return MEMBER_NONE


@dataclass(frozen=True)
class SurfaceDecision:
    """What the front does with one request to one surface."""

    surface: str
    outcome: str
    status: int
    reason_code: str
    detail: str
    location: str | None = None
    principal: str | None = None
    membership: str = MEMBER_NONE
    upstream_headers: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "surface": self.surface,
            "outcome": self.outcome,
            "status": self.status,
            "reason_code": self.reason_code,
            "detail": self.detail,
            "location": self.location,
            "principal": self.principal,
            "membership": self.membership,
        }


def authorize_surface(
    *,
    surface: str,
    sso: ChatSso,
    session: SsoSession | None,
    sign_in_location: str,
    now: float | None = None,
) -> SurfaceDecision:
    """Decide one request. Serve, redirect, or deny -- never a fourth thing.

    Two properties this function exists to hold:

    * **An unauthenticated request is never served.** There is no anonymous
      arm, on either surface, in either mode. The absence of a session is a
      redirect to the identity provider.
    * **Membership absence is an explicit denial.** A person who holds
      neither group is denied with :data:`GROUP_MEMBERSHIP_ABSENT` on BOTH
      surfaces. It is not "no groups, so no restrictions"; an empty
      membership set fails every membership test, which is the same rule
      ``chat_block.validate_requested_tools`` applies to an empty admitted
      set.
    """
    moment = time.time() if now is None else now
    if session is None:
        return SurfaceDecision(
            surface=surface,
            outcome=REDIRECT,
            status=302,
            reason_code=UNAUTHENTICATED,
            detail=(
                "no SSO session; the request is redirected to the identity "
                "provider and never served"
            ),
            location=sign_in_location,
        )
    if session.expired(now=moment):
        return SurfaceDecision(
            surface=surface,
            outcome=REDIRECT,
            status=302,
            reason_code=SESSION_EXPIRED,
            detail="the SSO session has expired; re-authentication required",
            location=sign_in_location,
            principal=session.principal,
        )
    membership = membership_state(session.groups, sso)
    if membership == MEMBER_NONE:
        return SurfaceDecision(
            surface=surface,
            outcome=DENY,
            status=403,
            reason_code=GROUP_MEMBERSHIP_ABSENT,
            detail=(
                f"principal is a member of neither {sso.admin_group!r} nor "
                f"{sso.user_group!r}; membership absence is a denial, not an "
                "absence of restrictions"
            ),
            principal=session.principal,
            membership=membership,
        )
    return SurfaceDecision(
        surface=surface,
        outcome=SERVE,
        status=200,
        reason_code=AUTHORIZED,
        detail=f"authenticated principal in the {membership} group",
        principal=session.principal,
        membership=membership,
        upstream_headers={str(sso.principal_header): session.principal},
    )


#: Identity headers the front owns outright, beyond the deployment's
#: declared principal header.
#:
#: Stripping only the declared header is not enough, and the gap is
#: concrete rather than theoretical. ``set_xauthrequest`` makes
#: oauth2-proxy emit the ``X-Auth-Request-*`` family, and Open WebUI's own
#: trusted-header authentication (``WEBUI_AUTH_TRUSTED_EMAIL_HEADER``) is
#: conventionally pointed at ``X-Auth-Request-Email``. A front that
#: stamped a correct principal for the MCP side while forwarding a
#: client's ``X-Auth-Request-Email`` would hand the caller its own chat
#: identity. ``Authorization`` is here for the same reason one step
#: further out: it is the transport credential of whatever sits behind the
#: front, and a client must not choose it.
FRONT_OWNED_HEADER_PREFIXES: tuple[str, ...] = ("x-auth-request-",)
FRONT_OWNED_HEADERS: frozenset[str] = frozenset({
    "authorization",
    "x-forwarded-user",
    "x-forwarded-email",
    "x-forwarded-preferred-username",
    "x-forwarded-groups",
    "x-remote-user",
    "remote-user",
})


def sanitize_inbound_headers(
    headers: Mapping[str, str],
    sso: ChatSso,
    *,
    extra_claim_headers: Iterable[str] = (),
) -> dict[str, str]:
    """Drop every client-supplied identity header, not merely one of them.

    The front is the only thing permitted to name who is calling. Three
    families are removed, and each closes a different way a caller could
    name itself:

    * **The declared principal header.** Read straight off
      ``principal_mapping._principal_value``: a front that APPENDS its
      trusted header rather than replacing it makes the substrate see the
      header twice and refuse the request as ``header_ambiguous``; and
      where only the first value is read, the client's own copy wins.
    * **Every declared extra claim header.** ``extra_claim_headers`` are
      claims the deployment's policy MATCHES on. They are not subject
      bindings — ``mcp-principal-mapping`` refuses those outright — but a
      caller choosing its own department is still a caller choosing policy
      input.
    * **The front's own vocabulary** (:data:`FRONT_OWNED_HEADERS`,
      :data:`FRONT_OWNED_HEADER_PREFIXES`), which the upstreams may be
      configured to trust independently of anything this module declared.

    Case-insensitively, because HTTP field names are.
    """
    removed = {str(sso.principal_header or "").strip().lower()}
    removed.update(
        str(name).strip().lower() for name in extra_claim_headers if str(name).strip()
    )
    removed.update(FRONT_OWNED_HEADERS)

    def _owned(name: str) -> bool:
        lowered = str(name).strip().lower()
        if lowered in removed:
            return True
        return any(
            lowered.startswith(prefix) for prefix in FRONT_OWNED_HEADER_PREFIXES
        )

    return {
        key: value for key, value in headers.items() if not _owned(key)
    }


# ---------------------------------------------------------------------------
# Identity provider
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IdpIdentity:
    """What one completed login established."""

    principal: str
    groups: tuple[str, ...]


class IdentityProviderClient(Protocol):
    """The two things the front needs an identity provider to do."""

    def authorize_url(self, *, state: str, redirect_uri: str) -> str:
        raise NotImplementedError

    def exchange(self, *, code: str, redirect_uri: str) -> IdpIdentity:
        raise NotImplementedError


class OidcDiscoveryClient:
    """An OIDC client built from the declared discovery endpoint.

    Deliberately small: authorization-code exchange, then read the
    principal and the group claim out of the token response's id token
    payload. It is NOT a token verifier -- the substrate's verifier lives in
    ``nomos/identity.py`` and ``nomos/oidc_login.py``, and this client is the
    front's plumbing, not a second trust model. What the front produces is a
    SESSION; what the substrate later trusts is the header the front stamps,
    checked against the declared provider by ``mcp-principal-mapping``.

    **The id token's signature is not checked, and that is a decision with a
    stated basis, not an omission.** The token is not accepted from the
    browser: it is fetched over the back channel from the ``token_endpoint``
    the provider's own discovery document names, which
    :meth:`_endpoint` holds to TLS on any routable host. OIDC Core's ID Token
    Validation section permits skipping signature validation exactly in that case,
    because TLS already authenticates the issuer. Two things follow and are
    enforced rather than assumed: a cleartext token endpoint on a routable
    host is REFUSED (otherwise the justification evaporates), and this class
    is the only place a token is read -- nothing else in the substrate treats
    what it returns as verified evidence. If a deployment needs real
    signature verification (a provider reached through an untrusted hop, or
    an audit requirement), ``nomos/oidc_login.py`` already does JWKS-backed
    verification and is the thing to reach for.
    """

    def __init__(
        self,
        sso: ChatSso,
        *,
        subject_claim: str,
        environ: Mapping[str, str] | None = None,
        opener: Any | None = None,
        timeout: float = 15.0,
    ) -> None:
        self._sso = sso
        self._subject_claim = subject_claim
        self._secret = resolve_client_secret(sso, environ=environ)
        self._timeout = timeout
        # No redirects. The manifest refuses a cleartext discovery endpoint
        # on a routable host and :meth:`_endpoint` refuses cleartext
        # endpoints inside the document -- both of which a followed
        # https-to-http redirect would walk straight past, carrying the
        # client secret with it.
        self._opener = opener or urllib.request.build_opener(_RefuseRedirect())
        self._metadata: dict[str, Any] | None = None

    def _discover(self) -> dict[str, Any]:
        if self._metadata is None:
            import json

            request = urllib.request.Request(  # noqa: S310 - operator-declared
                str(self._sso.discovery_endpoint),
                headers={"Accept": "application/json"},
            )
            with self._opener.open(request, timeout=self._timeout) as response:
                self._metadata = json.loads(response.read().decode("utf-8"))
        return self._metadata

    def _endpoint(self, name: str) -> str:
        """One endpoint from the discovery document, checked before use.

        Two checks, and the second is the one that matters. Absent is
        refused by name -- a login that cannot start should say which half
        of the document is missing. And the URL is held to the SAME
        transport rule the manifest's ``discovery_endpoint`` is held to:
        https, or loopback for a local test provider. The document is
        fetched from the provider, but it is still a document that names
        where credentials get sent, and "the provider said so" is not a
        reason to send a client secret over cleartext to a routable host.
        """
        metadata = self._discover()
        endpoint = str(metadata.get(name) or "").strip()
        if not endpoint:
            raise ChatSsoStartupError(
                f"the identity provider's discovery document declares no "
                f"{name}, so the login cannot proceed",
                data={
                    "reason": "discovery_incomplete",
                    "discovery_endpoint": self._sso.discovery_endpoint,
                    "missing": name,
                },
                fix_hint=(
                    "point chat.sso.discovery_endpoint at a complete OpenID "
                    "configuration document"
                ),
            )
        parts = urllib.parse.urlsplit(endpoint)
        host = (parts.hostname or "").lower()
        loopback = host in {"localhost", "127.0.0.1", "::1"}
        if parts.scheme == "https" or (parts.scheme == "http" and loopback):
            return endpoint
        raise ChatSsoStartupError(
            f"the identity provider's discovery document points {name} at "
            f"{parts.scheme or 'no'} scheme on host {host!r}; refusing to "
            "carry an authorization code or a client secret over anything "
            "but TLS to a routable host",
            data={
                "reason": "discovery_endpoint_insecure",
                "endpoint_name": name,
                "scheme": parts.scheme,
                "host": host,
            },
            fix_hint=(
                "the provider must publish https endpoints; a local test "
                "provider may use loopback"
            ),
        )

    def authorize_url(self, *, state: str, redirect_uri: str) -> str:
        endpoint = self._endpoint("authorization_endpoint")
        query = urllib.parse.urlencode({
            "response_type": "code",
            "client_id": self._sso.client_id,
            "redirect_uri": redirect_uri,
            "scope": "openid profile email",
            "state": state,
        })
        joiner = "&" if urllib.parse.urlsplit(endpoint).query else "?"
        return f"{endpoint}{joiner}{query}"

    def exchange(self, *, code: str, redirect_uri: str) -> IdpIdentity:
        import base64
        import json

        endpoint = self._endpoint("token_endpoint")
        body = urllib.parse.urlencode({
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": self._sso.client_id,
            "client_secret": self._secret,
        }).encode("utf-8")
        request = urllib.request.Request(  # noqa: S310 - provider-declared
            endpoint,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )
        with self._opener.open(request, timeout=self._timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        id_token = payload.get("id_token")
        if not id_token or id_token.count(".") != 2:
            raise ChatSsoStartupError(
                "the identity provider returned no id_token, so the login "
                "established no principal",
                data={"reason": "no_id_token"},
            )
        raw = id_token.split(".")[1]
        raw += "=" * (-len(raw) % 4)
        claims = json.loads(base64.urlsafe_b64decode(raw).decode("utf-8"))
        self._require_token_addressed_to_us(claims)
        return identity_from_claims(
            claims, self._sso, subject_claim=self._subject_claim,
        )

    def _require_token_addressed_to_us(self, claims: Mapping[str, Any]) -> None:
        """Refuse a token issued to somebody else, or already expired.

        TLS on the code exchange authenticates the ENDPOINT; it does not say
        the token in the response is for this client or still current. These
        are the two checks whose absence would let a token minted for a
        different relying party -- or one recovered from a log -- establish
        a session here.
        """
        issuer = str(claims.get("iss") or "")
        if issuer and self._sso.issuer and issuer != self._sso.issuer:
            raise ChatSsoStartupError(
                "the identity provider's token names a different issuer than "
                "the deployment declares",
                data={
                    "reason": "token_issuer_mismatch",
                    "declared": self._sso.issuer,
                    "token": issuer,
                },
            )
        raw_aud = claims.get("aud")
        audiences = (
            [str(raw_aud)]
            if isinstance(raw_aud, str)
            else [str(item) for item in raw_aud]
            if isinstance(raw_aud, Sequence)
            else []
        )
        if audiences and self._sso.client_id not in audiences:
            raise ChatSsoStartupError(
                "the identity provider's token is addressed to a different "
                "client; refusing to establish a session from a token issued "
                "to somebody else",
                data={
                    "reason": "token_audience_mismatch",
                    "client_id": self._sso.client_id,
                    "token_audiences": audiences,
                },
            )
        expiry = claims.get("exp")
        if expiry is not None:
            try:
                expires_at = float(expiry)
            except (TypeError, ValueError):
                raise ChatSsoStartupError(
                    "the identity provider's token carries an unreadable exp",
                    data={"reason": "token_exp_malformed"},
                ) from None
            if expires_at <= time.time():
                raise ChatSsoStartupError(
                    "the identity provider's token has expired",
                    data={"reason": "token_expired", "exp": expires_at},
                )


def identity_from_claims(
    claims: Mapping[str, Any],
    sso: ChatSso,
    *,
    subject_claim: str,
) -> IdpIdentity:
    """Read the principal and the declared group claim out of one token.

    ``subject_claim`` is a REQUIRED argument, and that is the point. It was
    once chosen here as ``"email" if "email" in claims else "sub"``, which
    is presence-sniffing an identifier NAMESPACE: a provider that stopped
    sending ``email`` would silently start producing ``sub`` values, the
    projection would still be writing bindings on emails, and every
    authenticated person would be refused ``unidentified_principal`` --
    absence read as a decision. The claim now comes from
    ``nomos.principal_mapping.subject_claim``, the same declaration the
    bindings are written against, and a token that does not carry it is
    refused rather than substituted.

    Only the DECLARED claim names are read. A provider that also sends
    ``roles`` or ``entitlements`` contributes nothing: this front projects
    exactly the group claim the manifest names, so a provider-side change
    cannot quietly widen who is an admin here.
    """
    name = str(subject_claim or "").strip()
    if not name:
        raise ChatSsoStartupError(
            "no subject claim was supplied, so there is no declared name to "
            "read the principal from; refusing to guess one",
            data={"reason": "subject_claim_absent"},
            fix_hint="declare nomos.principal_mapping.subject_claim",
        )
    principal = str(claims.get(name) or "").strip()
    if not principal:
        raise ChatSsoStartupError(
            f"the identity provider's token carries no {name!r} claim, so "
            "the login established nobody. The claim is not substituted with "
            "another: a different claim is a different identifier namespace, "
            "and the deployment's subject bindings are written against this "
            "one",
            data={
                "reason": "no_principal_claim",
                "subject_claim": name,
                "claims_present": sorted(str(key) for key in claims),
            },
            fix_hint=(
                f"configure the provider to release {name!r}, or change "
                "nomos.principal_mapping.subject_claim to a claim it does "
                "release"
            ),
        )
    raw_groups = claims.get(str(sso.groups_claim))
    if raw_groups is None:
        groups: tuple[str, ...] = ()
    elif isinstance(raw_groups, str):
        groups = (raw_groups,)
    elif isinstance(raw_groups, Sequence):
        groups = tuple(str(item) for item in raw_groups)
    else:
        groups = ()
    return IdpIdentity(principal=principal, groups=groups)


# ---------------------------------------------------------------------------
# The reference front
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Upstream:
    """Where one surface actually runs. Operator-state topology, not manifest."""

    surface: str
    base_url: str


@dataclass(frozen=True)
class _PendingLogin:
    """One in-flight authorization request.

    Carries the browser-binding nonce and a creation time, so a ``state``
    value is neither a bearer token for a login nor an entry that lives
    forever.
    """

    target: str
    nonce: str
    created_at: float


class SsoFrontProxy:
    """A WSGI front that serves both surfaces from one session.

    This is the executable form of the contract: the same object routes
    chat and console, so "one login, two surfaces" and "logout ends both"
    are properties of one code path rather than of two configurations that
    happen to agree today.

    It forwards plain request-response HTTP. It does not carry WebSocket
    upgrades, so it is a verification and local bring-up surface; the
    production front is the rendered nginx + oauth2-proxy pair, which makes
    the same decisions.
    """

    def __init__(
        self,
        *,
        deployment: str,
        sso: ChatSso,
        routes: Sequence[SurfaceRoute],
        upstreams: Sequence[Upstream],
        idp: IdentityProviderClient,
        store: SessionStore | None = None,
        public_scheme: str = "http",
        opener: Any | None = None,
        extra_claim_headers: Sequence[str] = (),
    ) -> None:
        if not sso.multi_user:
            raise ChatSsoStartupError(
                f"refusing to front deployment {deployment!r}: chat.sso.mode "
                f"is {sso.mode!r}. The front exists to enforce multi-user "
                "identity; running it over a single-operator declaration "
                "would authenticate people the deployment never said it "
                "serves",
                data={"deployment": deployment, "reason": "front_without_multi_user", "mode": sso.mode},
                fix_hint=f"declare chat.sso.mode: {SSO_MODE_MULTI_USER}",
            )
        self.deployment = deployment
        self.sso = sso
        self.routes = tuple(routes)
        self.upstreams = {item.surface: item.base_url for item in upstreams}
        missing = sorted(
            route.surface for route in self.routes
            if route.surface not in self.upstreams
        )
        if missing:
            raise ChatSsoStartupError(
                f"refusing to front deployment {deployment!r}: no upstream "
                f"declared for surface(s) {missing}. A route with no upstream "
                "would answer 502 to an authenticated person, which reads as "
                "an outage rather than a misconfiguration",
                data={"deployment": deployment, "reason": "upstream_absent", "missing": missing},
                fix_hint="declare an upstream for every routed surface",
            )
        self.idp = idp
        self.store = store or SessionStore()
        self.public_scheme = public_scheme
        #: Header names the deployment declared as extra claim inputs. The
        #: front owns them the same way it owns the principal header: they
        #: are policy input, so a caller supplying its own is a caller
        #: choosing the policy applied to it.
        self.extra_claim_headers = tuple(extra_claim_headers)
        self._opener = opener or urllib.request.build_opener(_NoRedirect())
        self._pending: dict[str, _PendingLogin] = {}
        self._lock = threading.Lock()
        #: Decisions made, newest last. The proxy-level evidence a test or an
        #: operator reads instead of re-deriving intent from status codes.
        self.decisions: list[SurfaceDecision] = []
        #: The header set handed to each upstream, newest last. Recorded
        #: because the wire is a lossy place to check this: an HTTP client
        #: library folds two spellings of one field name into a single
        #: header before it leaves the process, so an upstream that receives
        #: the right value proves nothing about whether a client's copy was
        #: dropped. What was CONSTRUCTED is the checkable fact.
        self.forwarded_headers: list[dict[str, str]] = []

    # -- WSGI ------------------------------------------------------------

    def __call__(self, environ, start_response):  # noqa: ANN001, ANN201
        host = environ.get("HTTP_HOST", "")
        path = environ.get("PATH_INFO", "") or "/"
        query = environ.get("QUERY_STRING", "")
        cookies = _parse_cookies(environ.get("HTTP_COOKIE", ""))
        session = self.store.get(cookies.get(SESSION_COOKIE_NAME))

        if _has_dot_segment(path):
            # A `..` segment survives WSGI, and the prefix strip below is
            # string arithmetic: `/console/../secret` would be forwarded to
            # the console upstream as `/../secret`, letting the caller reach
            # paths outside what the route publishes. Refused rather than
            # normalized, because normalizing changes the request the person
            # asked for without telling them.
            return _respond(
                start_response,
                400,
                b"path contains a relative segment\n",
                headers=[("X-OKG-SSO-Reason", "chat.sso.path_not_absolute")],
            )

        if path in _PROXY_PATHS:
            return self._proxy_endpoint(
                path, host, query, session, cookies, start_response,
            )

        route = self._route_for(host, path)
        if route is None:
            return _respond(
                start_response,
                404,
                b"no surface is published at this host and path\n",
            )
        decision = authorize_surface(
            surface=route.surface,
            sso=self.sso,
            session=session,
            sign_in_location=self._sign_in_location(host, path, query),
        )
        self.decisions.append(decision)
        if decision.outcome == REDIRECT:
            return _respond(
                start_response,
                302,
                b"",
                headers=[
                    ("Location", decision.location or ""),
                    ("X-OKG-SSO-Reason", decision.reason_code),
                ],
            )
        if decision.outcome == DENY:
            return _respond(
                start_response,
                403,
                decision.detail.encode("utf-8") + b"\n",
                headers=[("X-OKG-SSO-Reason", decision.reason_code)],
            )
        return self._forward(route, environ, decision, start_response)

    # -- proxy-owned endpoints -------------------------------------------

    def _proxy_endpoint(
        self, path, host, query, session, cookies, start_response,
    ):  # noqa: ANN001, ANN202
        params = urllib.parse.parse_qs(query)
        if path == PROXY_SIGN_IN_PATH:
            target = self._safe_return_to((params.get("rd") or ["/"])[0])
            if target is None:
                return _respond(
                    start_response,
                    400,
                    b"refusing to return to a location outside this "
                    b"deployment's surfaces\n",
                    headers=[("X-OKG-SSO-Reason", REDIRECT_OFF_SITE)],
                )
            state = secrets.token_urlsafe(24)
            nonce = secrets.token_urlsafe(24)
            with self._lock:
                self._expire_pending()
                if len(self._pending) >= _MAX_PENDING_LOGINS:
                    # Bounded, and the bound REFUSES rather than evicting: a
                    # flood of sign-in starts must not silently drop the
                    # pending state of a real person mid-login.
                    return _respond(
                        start_response,
                        503,
                        b"too many sign-ins in flight\n",
                        headers=[("X-OKG-SSO-Reason", "chat.sso.pending_overflow")],
                    )
                self._pending[state] = _PendingLogin(
                    target=target, nonce=nonce, created_at=time.time(),
                )
            location = self.idp.authorize_url(
                state=state, redirect_uri=self._redirect_uri(host),
            )
            return _respond(
                start_response,
                302,
                b"",
                headers=[
                    ("Location", location),
                    # The state is bound to THIS browser. Without it, anyone
                    # who observes a live `state` can complete the login and
                    # be handed the session cookie -- login CSRF, and the
                    # victim ends up signed in as the attacker.
                    ("Set-Cookie", _nonce_cookie(nonce)),
                ],
            )
        if path == PROXY_CALLBACK_PATH:
            code = (params.get("code") or [""])[0]
            state = (params.get("state") or [""])[0]
            with self._lock:
                self._expire_pending()
                pending = self._pending.pop(state, None)
            presented = cookies.get(LOGIN_NONCE_COOKIE_NAME, "")
            if (
                pending is None
                or not code
                or not presented
                or not secrets.compare_digest(presented, pending.nonce)
            ):
                # An unsolicited callback is not a login, and neither is one
                # replayed by a browser that never started it. Refusing both
                # is what keeps a session from being minted by anyone who can
                # reach the callback path.
                return _respond(
                    start_response,
                    400,
                    b"unsolicited or incomplete authorization callback\n",
                    headers=[("X-OKG-SSO-Reason", "chat.sso.callback_unsolicited")],
                )
            identity = self.idp.exchange(
                code=code, redirect_uri=self._redirect_uri(host),
            )
            new_session = self.store.create(
                principal=identity.principal, groups=identity.groups,
            )
            return _respond(
                start_response,
                302,
                b"",
                headers=[
                    ("Location", pending.target),
                    ("Set-Cookie", self._session_cookie(new_session.session_id)),
                    ("Set-Cookie", _expired_cookie(LOGIN_NONCE_COOKIE_NAME)),
                ],
            )
        # PROXY_SIGN_OUT_PATH
        target = self._safe_return_to((params.get("rd") or ["/"])[0])
        if target is None:
            return _respond(
                start_response,
                400,
                b"refusing to return to a location outside this "
                b"deployment's surfaces\n",
                headers=[("X-OKG-SSO-Reason", REDIRECT_OFF_SITE)],
            )
        self.store.invalidate(cookies.get(SESSION_COOKIE_NAME))
        return _respond(
            start_response,
            302,
            b"",
            headers=[
                ("Location", target),
                ("Set-Cookie", _expired_cookie(SESSION_COOKIE_NAME)),
            ],
        )

    def _safe_return_to(self, raw: str) -> str | None:
        """The post-login location, or None when it leaves the deployment.

        ``rd`` is caller-supplied, so without this an unauthenticated
        ``GET /oauth2/start?rd=https://evil.example/`` is an open redirect
        wearing the deployment's hostname -- and after a real login, which
        is exactly the moment a person is most likely to trust where they
        land. Relative targets are kept; absolute ones must name a host the
        deployment actually publishes. Refused, never rewritten to "/": a
        silent rewrite would hide a link that someone deliberately crafted.
        """
        candidate = str(raw or "/")
        parts = urllib.parse.urlsplit(candidate)
        if not parts.scheme and not parts.netloc:
            # A path-relative target. It must be a path, not a
            # scheme-relative "//evil.example" (urlsplit gives that one a
            # netloc, so it is already handled) and not empty.
            return candidate if candidate.startswith("/") else None
        if parts.scheme not in {"http", "https"}:
            return None
        published = {_bare_host(route.host) for route in self.routes}
        return candidate if _bare_host(parts.netloc) in published else None

    def _expire_pending(self) -> None:
        """Drop pending logins nobody completed. Caller holds the lock."""
        cutoff = time.time() - _PENDING_LOGIN_TTL_SECONDS
        for state in [
            key for key, item in self._pending.items() if item.created_at < cutoff
        ]:
            self._pending.pop(state, None)

    def _session_cookie(self, session_id: str) -> str:
        return _cookie(
            SESSION_COOKIE_NAME,
            session_id,
            secure=self.public_scheme == "https",
        )

    # -- helpers ---------------------------------------------------------

    def _route_for(self, host: str, path: str) -> SurfaceRoute | None:
        # Longest prefix first, so a console published under a prefix on a
        # shared host is not shadowed by the chat route at "/". (The shared
        # host topology is refused at startup; the ordering keeps this
        # function honest anyway rather than relying on that refusal.)
        for route in sorted(
            self.routes, key=lambda item: len(item.path_prefix), reverse=True,
        ):
            if route.matches(host, path):
                return route
        return None

    def _redirect_uri(self, host: str) -> str:
        return f"{self.public_scheme}://{host}{PROXY_CALLBACK_PATH}"

    def _sign_in_location(self, host: str, path: str, query: str) -> str:
        target = f"{self.public_scheme}://{host}{path}"
        if query:
            target += f"?{query}"
        return (
            f"{PROXY_SIGN_IN_PATH}?"
            + urllib.parse.urlencode({"rd": target})
        )

    def _forward(self, route, environ, decision, start_response):  # noqa: ANN001, ANN202
        upstream = self.upstreams[route.surface].rstrip("/")
        path = environ.get("PATH_INFO", "") or "/"
        if route.path_prefix:
            path = path[len(route.path_prefix):] or "/"
        query = environ.get("QUERY_STRING", "")
        url = upstream + path + (f"?{query}" if query else "")
        inbound = _request_headers(environ)
        # Strip first, stamp second. See sanitize_inbound_headers.
        forwarded = sanitize_inbound_headers(
            inbound, self.sso, extra_claim_headers=self.extra_claim_headers,
        )
        forwarded.update(decision.upstream_headers)
        forwarded.pop("Host", None)
        # The upstream keeps its OWN cookies -- Open WebUI has a session of
        # its own and a front that ate every cookie would leave it unable to
        # hold one. Only the front's two cookies are withheld, because they
        # are the front's business and an upstream has no use for them.
        upstream_cookies = _without_cookies(
            environ.get("HTTP_COOKIE", ""),
            {SESSION_COOKIE_NAME, LOGIN_NONCE_COOKIE_NAME},
        )
        if upstream_cookies:
            forwarded["Cookie"] = upstream_cookies
        else:
            forwarded.pop("Cookie", None)
        self.forwarded_headers.append(dict(forwarded))
        method = environ.get("REQUEST_METHOD", "GET")
        length = environ.get("CONTENT_LENGTH") or ""
        body = (
            environ["wsgi.input"].read(int(length))
            if length.isdigit() and int(length) > 0
            else None
        )
        request = urllib.request.Request(  # noqa: S310 - operator-declared upstream
            url, data=body, headers=forwarded, method=method,
        )
        try:
            with self._opener.open(request, timeout=30.0) as response:
                payload = response.read()
                headers = [
                    (key, value)
                    for key, value in response.headers.items()
                    if key.lower() not in {"transfer-encoding", "content-length"}
                ]
                status = response.status
        except urllib.error.HTTPError as err:
            payload = err.read()
            headers = [
                (key, value)
                for key, value in err.headers.items()
                if key.lower() not in {"transfer-encoding", "content-length"}
            ]
            status = err.code
        except (urllib.error.URLError, OSError) as err:
            return _respond(
                start_response,
                502,
                f"upstream for surface {route.surface!r} did not answer: "
                f"{err}\n".encode("utf-8"),
            )
        headers.append(("X-OKG-SSO-Reason", decision.reason_code))
        return _respond(start_response, status, payload, headers=headers)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Return an upstream's 3xx to the browser instead of following it.

    A front that followed redirects on the client's behalf would turn one
    validated hop into an unvalidated second one, and would hide the
    upstream's own location handling from the person browsing.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102, ANN001
        return None


class _RefuseRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse a redirect from the identity provider instead of following it.

    The manifest refuses a cleartext discovery endpoint on a routable host
    and ``OidcDiscoveryClient._endpoint`` refuses cleartext endpoints inside
    the document. A followed https-to-http redirect walks past both, with
    the client secret in the request body -- so the hop is an error, not a
    detour.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102, ANN001
        raise urllib.error.HTTPError(
            req.full_url,
            code,
            "the identity provider redirected; refusing to follow a hop that "
            "the endpoint checks never saw",
            headers,
            fp,
        )


def _request_headers(environ: Mapping[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in environ.items():
        if not key.startswith("HTTP_"):
            continue
        name = "-".join(part.capitalize() for part in key[5:].split("_"))
        headers[name] = str(value)
    if environ.get("CONTENT_TYPE"):
        headers["Content-Type"] = str(environ["CONTENT_TYPE"])
    return headers


def _has_dot_segment(path: str) -> bool:
    return any(segment in {".", ".."} for segment in str(path or "").split("/"))


def _without_cookies(raw: str, drop: set[str]) -> str:
    kept = [
        chunk.strip()
        for chunk in str(raw or "").split(";")
        if chunk.strip() and chunk.split("=", 1)[0].strip() not in drop
    ]
    return "; ".join(kept)


def _parse_cookies(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for chunk in str(raw or "").split(";"):
        if "=" not in chunk:
            continue
        key, _, value = chunk.partition("=")
        out[key.strip()] = value.strip()
    return out


def _cookie(name: str, value: str, *, secure: bool, max_age: int | None = None) -> str:
    parts = [f"{name}={value}", "Path=/", "HttpOnly", "SameSite=Lax"]
    if secure:
        # Conditional on the PUBLIC scheme rather than unconditional: a
        # `Secure` cookie is never sent over http, so setting it on a
        # loopback http bring-up would produce a front that logs a person in
        # and then cannot see their session -- a broken site, not a safer
        # one. Every https deployment gets it.
        parts.append("Secure")
    if max_age is not None:
        parts.append(f"Max-Age={max_age}")
    return "; ".join(parts)


def _nonce_cookie(nonce: str) -> str:
    return _cookie(
        LOGIN_NONCE_COOKIE_NAME, nonce, secure=False, max_age=_PENDING_LOGIN_TTL_SECONDS,
    )


def _expired_cookie(name: str) -> str:
    return _cookie(name, "", secure=False, max_age=0)


def _respond(start_response, status: int, body: bytes, *, headers=None):  # noqa: ANN001, ANN202
    import http

    try:
        phrase = http.HTTPStatus(status).phrase
    except ValueError:  # pragma: no cover - non-standard upstream status
        phrase = ""
    out = list(headers or [])
    out.append(("Content-Length", str(len(body))))
    if not any(key.lower() == "content-type" for key, _ in out):
        out.append(("Content-Type", "text/plain; charset=utf-8"))
    start_response(f"{status} {phrase}".strip(), out)
    return [body]


# ---------------------------------------------------------------------------
# Production front: rendered configuration
# ---------------------------------------------------------------------------

def render_oauth2_proxy_cfg(
    decision: SsoStartupDecision,
    *,
    listen: str = "127.0.0.1:4180",
) -> str:
    """Render the oauth2-proxy configuration for BOTH surfaces.

    One oauth2-proxy, one cookie, one ``cookie_domain`` covering both
    hostnames: that is the mechanism by which one login reaches two surfaces
    and one sign-out ends both. Two oauth2-proxy instances would be two
    sessions wearing one name.

    The client secret is NOT rendered. oauth2-proxy reads
    ``OAUTH2_PROXY_CLIENT_SECRET`` from its unit environment, which is where
    the manifest's ``client_secret_env`` variable is exported --
    design.md section 4's no-secrets rule, kept mechanical: this function has
    no parameter that could carry the value.
    """
    sso = decision.sso
    if sso is None or not decision.multi_user:  # pragma: no cover - guarded upstream
        raise ChatSsoStartupError(
            "refusing to render a front for a deployment that is not "
            "multi-user",
            data={"deployment": decision.deployment, "reason": "not_multi_user"},
        )
    hosts = [_bare_host(route.host) for route in decision.routes]
    domain = _common_cookie_domain(hosts)
    chat_route = _route_named(decision, CHAT_SURFACE)
    return "\n".join([
        f"# Rendered by okg for deployment {decision.deployment}.",
        "# One oauth2-proxy fronts BOTH surfaces: one cookie, one session.",
        "# The client secret is NOT here. Export "
        f"{sso.client_secret_env} in the unit environment as",
        "# OAUTH2_PROXY_CLIENT_SECRET.",
        "",
        'provider = "oidc"',
        f'oidc_issuer_url = "{sso.issuer}"',
        f'client_id = "{sso.client_id}"',
        f'http_address = "{listen}"',
        f'redirect_url = "https://{chat_route.host}{PROXY_CALLBACK_PATH}"',
        "",
        'scope = "openid profile email groups"',
        f'oidc_groups_claim = "{sso.groups_claim}"',
        "allowed_groups = ["
        + ", ".join(f'"{name}"' for name in sso.projected_groups)
        + "]",
        'upstreams = [ "static://202" ]',
        "",
        "# One registered redirect URI, on the chat host. A sign-in that",
        "# began on the console therefore completes on the chat host and is",
        "# sent back by the `rd` parameter -- which oauth2-proxy DROPS when",
        "# it points at another host unless that host is whitelisted. Without",
        "# this line the console login succeeds and lands the person on the",
        "# chat site instead, which reads as a broken link rather than as a",
        "# missing setting.",
        f'whitelist_domains = [ ".{domain}" ]',
        "",
        "reverse_proxy = true",
        "set_xauthrequest = true",
        "pass_access_token = false",
        "pass_authorization_header = false",
        "",
        f'cookie_domain = "{domain}"',
        f'cookie_name = "{SESSION_COOKIE_NAME}"',
        "cookie_secure = true",
        "cookie_httponly = true",
        'cookie_samesite = "lax"',
        'cookie_expire = "8h"',
        'cookie_refresh = "1h"',
        "",
    ])


def render_nginx_site(
    decision: SsoStartupDecision,
    *,
    upstreams: Sequence[Upstream],
    oauth2_proxy: str = "127.0.0.1:4180",
) -> str:
    """Render the nginx server blocks that front both surfaces.

    Same ``auth_request`` shape as ``deployments/docs-site/nginx.conf``,
    with three differences the docs site does not need:

    * the principal header the substrate reads is set from the
      ``auth_request`` response, and the client's own copy is cleared --
      otherwise a caller could name its own principal and the fronting app's
      token would vouch for it. An ``auth_request`` that succeeded while
      naming nobody is a 403, not a forwarded request with no principal;
    * ``401`` and ``403`` are routed differently. The docs site maps only
      401; mapping 403 to the sign-in flow as well would send a person whom
      ``allowed_groups`` refused back through a login they already completed,
      looping instead of stating that they are in neither group. Membership
      absence gets the same typed reason the reference front emits;
    * both server blocks point at the SAME oauth2-proxy, which is what makes
      the session shared.
    """
    sso = decision.sso
    if sso is None or not decision.multi_user:  # pragma: no cover - guarded upstream
        raise ChatSsoStartupError(
            "refusing to render a front for a deployment that is not "
            "multi-user",
            data={"deployment": decision.deployment, "reason": "not_multi_user"},
        )
    by_surface = {item.surface: item.base_url for item in upstreams}
    blocks = [
        "\n".join([
            f"# Rendered by okg for deployment {decision.deployment}.",
            "# Both server blocks share ONE oauth2-proxy, so one login serves",
            "# both surfaces and /oauth2/sign_out ends both.",
            "#",
            "# This map belongs in the http context, above the server blocks.",
            "# It is what makes the WebSocket upgrade below conditional; a",
            "# bare `Connection: upgrade` on every request breaks keep-alive.",
            "map $http_upgrade $okg_connection_upgrade {",
            "    default upgrade;",
            "    ''      close;",
            "}",
        ]),
    ]
    # Grouped by hostname, NOT one block per surface. Two `server` blocks
    # carrying the same `server_name` are not two sites: nginx matches the
    # first and the second is dead config, so the console published under a
    # prefix on the chat host would never be reached. One block per host,
    # with a location per surface, is the only shape that serves both. (This
    # became reachable when the prefix topology stopped being banned; while
    # it was refused, the bug had nowhere to appear.)
    by_host: dict[str, list[SurfaceRoute]] = {}
    for route in decision.routes:
        if route.surface not in by_surface:
            raise ChatSsoStartupError(
                f"refusing to render the front: no upstream for surface "
                f"{route.surface!r}",
                data={
                    "deployment": decision.deployment,
                    "reason": "upstream_absent",
                    "surface": route.surface,
                },
            )
        by_host.setdefault(_bare_host(route.host).split(":")[0], []).append(route)
    for host, routes in by_host.items():
        blocks.append(_nginx_server_block(
            host=host,
            routes=routes,
            upstreams=by_surface,
            principal_header=str(sso.principal_header),
            principal_variable=FRONT_SUPPLIED_CLAIMS[str(decision.subject_claim)],
            cleared_headers=decision.extra_claim_headers,
            oauth2_proxy=oauth2_proxy,
        ))
    return "\n\n".join(blocks) + "\n"


def _nginx_server_block(
    *,
    host: str,
    routes: Sequence[SurfaceRoute],
    upstreams: Mapping[str, str],
    principal_header: str,
    principal_variable: str,
    cleared_headers: Sequence[str],
    oauth2_proxy: str,
) -> str:
    """One server block per HOSTNAME, with a location per surface on it."""
    # Every identity header the front owns, cleared by name. Naming a header
    # in proxy_set_header is what stops the CLIENT's copy from reaching the
    # upstream, so this list IS the strip -- the same set the reference
    # front removes in sanitize_inbound_headers.
    owned = sorted(
        {principal_header}
        | {str(name) for name in cleared_headers}
        | {
            "X-Auth-Request-User",
            "X-Auth-Request-Email",
            "X-Auth-Request-Preferred-Username",
            "X-Auth-Request-Groups",
            "X-Forwarded-User",
            "X-Forwarded-Email",
            "Remote-User",
        }
    )
    clearing = [f'            proxy_set_header {name} "";' for name in owned]

    lines = [
        "server {",
        "    listen 443 ssl http2;",
        f"    server_name {host};",
        "    # surfaces: "
        + ", ".join(
            f"{route.surface} at {route.path_prefix or '/'}" for route in routes
        ),
        "",
        "    location /oauth2/ {",
        f"        proxy_pass http://{oauth2_proxy};",
        "        proxy_set_header Host $host;",
        "        proxy_set_header X-Real-IP $remote_addr;",
        "        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;",
        "        proxy_set_header X-Forwarded-Proto $scheme;",
        "        proxy_set_header X-Auth-Request-Redirect $request_uri;",
        "    }",
        "",
        "    location = /oauth2/auth {",
        "        internal;",
        f"        proxy_pass http://{oauth2_proxy};",
        "        proxy_pass_request_body off;",
        '        proxy_set_header Content-Length "";',
        "        proxy_set_header Host $host;",
        "        proxy_set_header X-Forwarded-Proto $scheme;",
        "        proxy_set_header X-Forwarded-Uri $request_uri;",
        "    }",
    ]
    # Longest prefix first for readability; nginx matches by longest prefix
    # regardless of the order they appear in.
    for route in sorted(routes, key=lambda item: -len(item.path_prefix)):
        location = route.path_prefix or "/"
        if not location.endswith("/"):
            location += "/"
        lines.extend([
            "",
            f"    # surface: {route.surface}",
            f"    location {location} {{",
            "        auth_request /oauth2/auth;",
            "        # 401 and 403 are DIFFERENT answers and get different",
            "        # handling. 401 is 'nobody is signed in' -> go and sign",
            "        # in. 403 is oauth2-proxy's allowed_groups refusing a",
            "        # signed-in person: sending that to the sign-in flow",
            "        # would loop them through a login they already completed",
            "        # instead of telling them they are in neither group.",
            "        error_page 401 = @oauth2_sign_in;",
            f"        error_page 403 = @{_denial_location(route)};",
            "",
            "        auth_request_set $auth_user $upstream_http_x_auth_request_preferred_username;",
            "        auth_request_set $auth_email $upstream_http_x_auth_request_email;",
            "        # An authenticated request that named NOBODY is refused,",
            "        # not forwarded: with the variable empty the header below",
            "        # would be skipped and the upstream would see a request",
            "        # with no principal, which mcp-serve reports as a missing",
            "        # header and an operator reads as a client fault.",
            f'        if ({principal_variable} = "") {{',
            f"            return 403 \"{GROUP_MEMBERSHIP_ABSENT}\";",
            "        }",
            "        # Naming a header in proxy_set_header is what stops the",
            "        # CLIENT's copy from being forwarded. The whole identity",
            "        # namespace is cleared, not just the declared principal:",
            "        # Open WebUI's own trusted-header auth reads",
            "        # X-Auth-Request-Email, so forwarding a client's copy",
            "        # would let it choose its chat identity while this front",
            "        # stamped a correct one for the graph.",
            *[line[4:] for line in clearing],
            f"        proxy_set_header {principal_header} {principal_variable};",
            "",
            f"        proxy_pass {upstreams[route.surface]};",
            "        proxy_set_header Host $host;",
            "        proxy_set_header X-Forwarded-Proto $scheme;",
            "        # Open WebUI holds a socket.io channel; without these the",
            "        # site loads and then never updates.",
            "        proxy_http_version 1.1;",
            "        proxy_set_header Upgrade $http_upgrade;",
            "        proxy_set_header Connection $okg_connection_upgrade;",
            "    }",
        ])
    lines.extend([
        "",
        "    location @oauth2_sign_in {",
        "        return 302 /oauth2/start?rd=$scheme://$http_host$request_uri;",
        "    }",
    ])
    for route in routes:
        lines.extend([
            "",
            f"    location @{_denial_location(route)} {{",
            "        default_type text/plain;",
            f"        add_header X-OKG-SSO-Reason {GROUP_MEMBERSHIP_ABSENT} always;",
            f"        return 403 \"{GROUP_MEMBERSHIP_ABSENT}: this account is in "
            "neither of the deployment's groups.\\n\";",
            "    }",
        ])
    lines.append("}")
    return "\n".join(lines)


def _denial_location(route: SurfaceRoute) -> str:
    return f"sso_group_denied_{route.surface}"


def _route_named(decision: SsoStartupDecision, surface: str) -> SurfaceRoute:
    for route in decision.routes:
        if route.surface == surface:
            return route
    raise ChatSsoStartupError(  # pragma: no cover - routes are built as a pair
        f"no route for surface {surface!r}",
        data={"deployment": decision.deployment, "reason": "route_absent"},
    )


def _common_cookie_domain(hosts: Sequence[str]) -> str:
    """The narrowest domain covering both surfaces.

    Refuses rather than widening: hosts with no shared parent would need a
    cookie domain broad enough to reach unrelated sites, which is a session
    handed to whoever runs them.
    """
    cleaned = [host.split(":")[0] for host in hosts if host]
    if not cleaned:  # pragma: no cover - routes are built as a pair
        raise ChatSsoStartupError(
            "no surface hostnames to derive a cookie domain from",
            data={"reason": "no_hosts"},
        )
    if len(set(cleaned)) == 1:
        return cleaned[0]
    parts = [host.split(".") for host in cleaned]
    shared: list[str] = []
    for labels in zip(*(reversed(item) for item in parts)):
        if len(set(labels)) != 1:
            break
        shared.append(labels[0])
    domain = ".".join(reversed(shared))
    if domain.count(".") < 1 or domain.lower() in _PUBLIC_SUFFIXES:
        raise ChatSsoStartupError(
            f"the two surfaces {cleaned} share no domain narrower than "
            f"{domain!r}, so one cookie cannot serve both without being "
            "readable by unrelated sites",
            data={
                "reason": "no_common_cookie_domain",
                "hosts": cleaned,
                "widest_shared": domain,
            },
            fix_hint=(
                "publish both surfaces under one parent domain, e.g. "
                "chat.okg.example and console.okg.example"
            ),
        )
    return domain


#: Multi-label public suffixes common enough to be worth refusing by name.
#:
#: Not a Public Suffix List, and deliberately not: pulling the real PSL in
#: would add a data dependency that goes stale. What this catches is the
#: shape that a label count alone misses -- ``chat.a.ac.uk`` and
#: ``console.b.ac.uk`` share ``ac.uk``, which has a dot and is nonetheless a
#: registry, not a deployment. The residual gap is stated rather than
#: papered over: a suffix outside this set yields a cookie domain a browser
#: will reject, so the failure mode is a login that visibly does not work,
#: not a session quietly shared with strangers.
_PUBLIC_SUFFIXES: frozenset[str] = frozenset({
    "ac.uk", "co.uk", "gov.uk", "ltd.uk", "me.uk", "net.uk", "org.uk",
    "plc.uk", "sch.uk",
    "com.au", "edu.au", "gov.au", "net.au", "org.au",
    "co.jp", "ac.jp", "go.jp", "ne.jp", "or.jp",
    "com.br", "com.cn", "edu.cn", "gov.cn", "net.cn", "org.cn",
    "co.in", "net.in", "org.in", "gen.in", "firm.in", "ind.in",
    "co.nz", "ac.nz", "govt.nz", "org.nz", "net.nz",
    "co.za", "org.za", "web.za",
    "com.mx", "com.ar", "com.sg", "com.hk", "com.tw", "com.tr",
    "github.io", "gitlab.io", "pages.dev", "workers.dev", "vercel.app",
    "herokuapp.com", "azurewebsites.net", "cloudfront.net",
    "s3.amazonaws.com", "appspot.com", "web.app", "firebaseapp.com",
})
