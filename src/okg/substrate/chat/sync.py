"""Declarative projection of a deployment's ``chat:`` block (PACT chat-sync-projection).

``okg chat sync --deployment D`` renders D's declared chat surface into its
Open WebUI instance and PROVES every write by reading it back through the
instance's own HTTP API. Nothing here is best-effort: a value that cannot be
read back, a value that disagrees with the effective desired state, and a
surface that could not be written all fail the run with the surface, the
expected value, and the observed value named.

Five written surfaces plus one probe
------------------------------------
1. ``instance_ui``  — the census-admitted ``chat.ui`` knobs and the governed
   feature toggles.
2. ``models``       — the admin-curated model list, its access grants, and the
   default model.
3. ``preset``       — the MCP tool-server registration (the wire that carries
   the preset's bound tools) and the preset model row: composed system
   prompt, description, suggestions, bound tools, group access.
4. ``prompts``      — the deployment's prompt-library entries. DRIVEN ONLY
   when a recorded write-plus-readback proof exists for the running version;
   otherwise the surface is DROPPED with a named refusal, never written blind.
5. ``deep_links``   — the declared link bases carried into the valves of the
   installed components that consume them.
6. ``mcp``          — a PROBE. It reports reachable or unreachable, never
   changed or unchanged.

And one BYTE-DELIVERY surface, deliberately outside that count
--------------------------------------------------------------
``css_bundle`` (PACT ``chat-css-bundle-contract``) delivers the deployment's
declared CSS file into the instance's static directory and proves byte
identity on readback. It is reported under its own key rather than joining
:data:`WRITTEN_SURFACES` for two reasons, and both are properties of the
mechanism rather than bookkeeping. It does not go through the admin API at
all — the vendor exposes no upload route for that directory, so the bytes ride
a container exec — and its claim is of a different KIND: the five surfaces
above each write a value and read that value back, whereas this one can only
ever assert that the bytes it wrote are the bytes being served. Counting it
among them would let "the file matches" read as "the styling is what the
manifest says", which is exactly the claim the recorded census refused. See
``okg.substrate.chat.css_bundle``.

Measured facts this module is built on
--------------------------------------
Every number below was measured against a live Open WebUI v0.11.0 container
(image digest ``sha256:72c0ba64…``) and is recorded in
``pact/changes/openwebui-chat-frontend/evidence/``. They are requirements
with evidence, not folklore.

**F1 — Open WebUI never surfaces MCP ``instructions`` to the model.**
``okg mcp-serve`` advertises 2243 characters of operator instructions in its
``initialize`` result; nothing under ``utils/mcp/``, ``utils/middleware.py``
or ``utils/tools.py`` reads them. The preset's ``params.system`` is therefore
the ONLY path an operator instruction can take, which is what makes
:func:`compose_system_prompt` load-bearing rather than decorative.

**F2 — an MCP registration REQUIRES ``info.id``.** ``routers/tools.py:143``
builds the tool identity as ``f'server:mcp:{info.get("id")}'`` and
``utils/middleware.py:2210`` resolves a server by matching
``(connection['info'] or {})['id'] == server_id``. A connection with no
``info.id`` registers ``server:mcp:None``: configured-looking, dead at call
time. :func:`_connection_payload` always writes one.

**F3 — "Initialized N tool server(s)" at startup is NOT an MCP health
signal.** ``utils/tools.py`` filters that count to
``type == 'openapi'``, so MCP entries are skipped there BY DESIGN and it logs
0. MCP resolves per request. This module never reads that count; the mcp
surface is proved by an ``initialize`` + ``tools/list`` round trip instead.

**F4 — ``tool_server.connections`` entry shape** (``routers/configs.py:217``
``ToolServerConnection``, ``extra='allow'``): ``url, path, type
('openapi'|'mcp'), auth_type, headers, key, config, info``. Enablement is
``config.enable``; ACLs are ``config.access_grants``; and — measured in
``utils/middleware.py:2237`` — ``config.function_name_filter_list`` is a
comma-separated allow/block list (``!name`` blocks) applied to the server's
tool specs. That list is the WIRE on which the query exclusion is enforced.

**F5 — a preset carries both halves the chat agent needs**: ``params.system``
(the composed instructions) and ``meta.toolIds = ['server:mcp:<info.id>']``.

**F6 — PersistentConfig is real.** Restarting the container with changed
``ENABLE_*`` / ``OPENAI_*`` env on an EXISTING volume is IGNORED: the values
are seeded into the config table on first boot and served from the database
forever after. Every toggle here is therefore driven through the admin API
and never through the environment.

**F7 — ``/api/v1/models/sync`` is BROKEN at v0.11.0.** It passes ``user_id``
twice into ``values()``, swallows the exception and returns HTTP 200 with
``[]`` while the row is UNCHANGED; its semantics are REPLACE, so an empty
sync deletes. This module never calls it. Presets go through
``/api/v1/models/create`` and ``/api/v1/models/model/update``, and the
confirmation is always the complete workspace listing at
``GET /api/v1/models/list``. The export route is retained only for legacy
callers because it can return an empty list for API-key callers.

**F8 — ``WEBUI_NAME`` is transformed.** ``env.py:892-893`` appends the literal
``' (Open WebUI)'`` whenever the value differs from ``'Open WebUI'``. The
readback comparison normalizes for it (:func:`expected_site_name`), or it
reports drift on every single run.

**F9 — "the deployment declares no instructions" is a first-class honest
case.** ``okg-workspace`` declares no ``mcp_instructions``; the CMS
deployment declares four agent-surface keys. :func:`compose_system_prompt`
reports which sources contributed and which were absent, and REFUSES rather
than emitting an empty system prompt that looks configured.

**F10 — the admitted ``chat.ui`` knobs are exactly**
``{site_name, logo_ref, banner, features}``. ``theme`` is not drivable at
this version and ``SHIPPED_UI_KEYS`` already excludes it. The admitted set is
re-read from the recorded census artifact AT RUN TIME here, so a re-recorded
census that shrinks the set produces a refusal rather than silent no-ops.

**F11 — the ``query`` exclusion is permanent on masked deployments** until
``mcp-http-auth`` proves masking coverage of query output. It has measured
that Nomos masking does NOT cover query output in any cell. This module
excludes; it never claims the coverage.

**F12 — consume, do not reimplement.** The MCP round trip is
``chat.source_admin.read_live_tools``; URL construction is
``chat.deep_links``; the console base-awareness check is
``chat.sso.console_base_awareness``. None of them is restated here.

One knob is asserted, not written
----------------------------------
The census records a MECHANISM per knob. ``banner`` and every feature toggle
are ``api`` knobs: this verb writes them and reads them back. ``site_name``
is an ``env`` knob (``WEBUI_NAME``) and ``logo_ref`` is a ``file`` knob
(bytes under ``STATIC_DIR``). Neither can be applied through the API, and
this verb does not own instance restart or the container filesystem — so it
never writes them. ``logo_ref`` remains a blocking assertion because the
declared asset is part of the visual contract. ``site_name`` is different: it
is a cosmetic, environment-owned deviation. Sync reads it back, records the
normalized expected value, observed value, ownership mechanism, and restart
fix hint, then continues with API-owned surfaces when it differs. No value is
written blind, and the deviation is visible in both the surface diff and the
operator notes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace as _dc_replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Sequence

from okg.substrate.chat import health_banner
from okg.substrate.errors import SubstrateError
from okg.substrate.chat.live_tool_slot import (
    assert_transports_disjoint,
    resolve_live_tool_slot,
)
from okg.substrate.mcp.operators import CHAT_DEFAULT_TOOLS

__all__ = [
    "ADMIN_PRESET_GATING_CLAIM",
    "ADMIN_PRESET_REFUSED_FILTER_COLLISION",
    "ADMIN_PRESET_REFUSED_PROJECTION",
    "ADMIN_PRESET_REFUSED_READ_ONLY_ENDPOINT",
    "ADMIN_PRESET_REFUSED_UNRESOLVED_GROUP",
    "ADMIN_PRESET_REFUSED_UNSAFE_TRANSPORT",
    "ADMIN_PRESET_SUFFIX",
    "AdminPresetPlan",
    "CHAT_DEFAULT_TOOLS",
    "CONNECTION_KEY_ABSENT",
    "CONNECTION_KEY_MASKED",
    "CONNECTION_SERVING_DECLARED",
    "CONNECTION_SERVING_NOTHING",
    "DENY_ALL_SENTINEL",
    "ENV_ADMIN_TOKEN",
    "ENV_INSTANCE_URL",
    "GOVERNED_TOGGLES",
    "GUIDANCE_KEYS",
    "MCP_HTTP_AUTH_PRODUCER",
    "SURFACE_ADMIN_PRESET",
    "VERIFY_HEALTHY",
    "VERIFY_REJECTED",
    "VERIFY_UNAVAILABLE",
    "WRITE_AUTHORIZED",
    "WRITE_CONNECTION_SUFFIX",
    "WRITE_UNAUTHORIZED_NO_ADMIN_GROUP",
    "WRITE_UNAUTHORIZED_READ_ONLY_ENDPOINT",
    "ChatSyncError",
    "ComposedPrompt",
    "InstanceClient",
    "Mechanism",
    "Posture",
    "StandingGuidance",
    "SurfaceOutcome",
    "SyncReport",
    "ToolCuration",
    "ToolCurationPlan",
    "WriteAuthorization",
    "WriteConnection",
    "admin_group_of",
    "bind_tool_curation",
    "bind_write_connection",
    "census_admitted_knobs",
    "census_excluded_knobs",
    "compose_standing_guidance",
    "compose_system_prompt",
    "connection_key_digest",
    "deny_all_filter_list",
    "driven_knob_set",
    "effective_tool_binding",
    "expected_site_name",
    "is_string_allowed",
    "load_bundle_chat",
    "looks_masked",
    "payload_secrets",
    "plan_admin_preset",
    "plan_tool_curation",
    "plan_write_connection",
    "redact",
    "redact_connection",
    "resolve_posture",
    "site_hygiene_desired",
    "sync",
    "tool_filter_list",
    "write_authorization",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ChatSyncError(SubstrateError):
    """``okg chat sync`` refused, or a projected value did not read back.

    One code for every failure this verb can produce, because the operator
    action is always the same shape: read the named surface, the expected
    value and the observed value out of ``data`` and fix the disagreement.
    Partial application is reported here as a failure, never as a degraded
    success.
    """

    code = "chat_sync_failed"


# ---------------------------------------------------------------------------
# Operator-environment inputs. Named, and refused by name when unset.
# ---------------------------------------------------------------------------

#: The instance admin API credential. THE ONLY credential this module reads,
#: and the only one that appears in any rendered payload (as the request's
#: ``Authorization`` header). Manifest content is structurally incapable of
#: carrying one: ``chat_block.schema_field_census`` asserts no credential-
#: shaped key exists at any depth of the schema.
ENV_ADMIN_TOKEN = "OKG_CHAT_ADMIN_TOKEN"

#: The admin API base, e.g. ``http://127.0.0.1:3000``. Instance URL is
#: operator environment: two installs of one deployment run at two addresses,
#: so it is never manifest content.
ENV_INSTANCE_URL = "OKG_CHAT_INSTANCE_URL"


# ---------------------------------------------------------------------------
# Vendor constants, each traceable to a measurement
# ---------------------------------------------------------------------------

#: F8. ``env.py:892-893``: appended whenever ``WEBUI_NAME != 'Open WebUI'``.
SITE_NAME_SUFFIX = " (Open WebUI)"
SITE_NAME_VENDOR_DEFAULT = "Open WebUI"

#: F2/F5. The tool identity Open WebUI builds for an MCP connection.
MCP_TOOL_ID_PREFIX = "server:mcp:"

#: Sentinel for `_resolve_connection_credential`'s `env_name`. An UNPASSED
#: argument inherits `chat.mcp.auth_token_env` -- the graph connection's
#: variable. An EXPLICIT `None` means "this connection declares no
#: credential", which is a legal state for the live connection and is what
#: the plan already writes as `live_connection:auth_type: "none"`. `None`
#: cannot carry both meanings: reading it as "inherit" registered the live
#: endpoint with the GRAPH bearer token while the plan said it carried none.
_INHERIT: Any = object()

SURFACE_INSTANCE_UI = "instance_ui"
SURFACE_MODELS = "models"
SURFACE_PRESET = "preset"
SURFACE_PROMPTS = "prompts"
SURFACE_DEEP_LINKS = "deep_links"
SURFACE_MCP = "mcp"
#: The source-admin preset (PACT chat-source-admin-writer). Written only when
#: the deployment declares ``chat.sso.admin_group``, so it is NOT in the
#: literal :data:`WRITTEN_SURFACES`; a run that declares it appends it.
SURFACE_ADMIN_PRESET = "admin_preset"
#: Appended to the preset id and to the MCP server id for the admin preset
#: and its connection.
ADMIN_PRESET_SUFFIX = "-admin"

#: The five WRITTEN surfaces, in application order. ``mcp`` is deliberately
#: absent: it is a probe that reports reachable/unreachable, and counting it
#: among the written surfaces would let "the endpoint answered" read as
#: "something was configured".
WRITTEN_SURFACES: tuple[str, ...] = (
    SURFACE_INSTANCE_UI,
    SURFACE_MODELS,
    SURFACE_PRESET,
    SURFACE_PROMPTS,
    SURFACE_DEEP_LINKS,
)

#: The prompts-API write-plus-readback proof: a file in the chat data
#: directory (packaged ``okg.substrate.chat.data`` by default). Until it
#: exists for the RUNNING version the prompts surface is dropped with a named
#: refusal (req.chat-sync-projection.projection-surfaces surface 4). No such
#: proof has been recorded, so the packaged directory does not hold one.
PROMPTS_PROOF_FILE = "prompts-api-readback.json"


class Mechanism:
    """How a census-admitted knob reaches the instance.

    Read off the census artifact's ``mechanism`` field rather than decided
    here, so a re-measurement that moves a knob between mechanisms moves this
    verb with it.
    """

    API = "api"
    ENV = "env"
    FILE = "file"


#: Mechanisms this verb can WRITE. Everything else it asserts by readback and
#: refuses to write, because applying it would require an instance restart or
#: the container filesystem — neither of which this verb owns.
WRITABLE_MECHANISMS: frozenset[str] = frozenset({Mechanism.API})


def _default_chat_data_dir() -> Path:
    """The packaged directory of recorded Open WebUI measurements.

    Resolved only when a run actually consults a measurement, so a verb that
    reads none of them (``okg chat plan``) never depends on it.
    """
    from okg.substrate.chat.data import chat_data_dir

    return chat_data_dir()


def _census_artifact(data_dir: Path | None) -> Path:
    """The knob census file under ``data_dir`` (packaged by default)."""
    from okg.substrate.deployments.chat_block import ui_knob_census_path

    return ui_knob_census_path(data_dir)


def _census_report_path(data_dir: Path | None) -> str:
    """The knob census file as a report names it (see :func:`_report_path`)."""
    from okg.substrate.deployments.chat_block import UI_KNOB_CENSUS_FILE

    return _report_path(data_dir, UI_KNOB_CENSUS_FILE)


def _report_path(data_dir: Path | None, name: str) -> str:
    """How a report names a chat data file.

    The packaged copy is named by its package-relative path, so a report
    never carries this machine's install location. A directory a caller
    supplied is named as given. Error data keeps the absolute path, because
    that is what an operator needs to find a broken file.
    """
    from okg.substrate.chat.data import PACKAGE_RELATIVE_DIR

    if data_dir is None:
        return f"{PACKAGE_RELATIVE_DIR}/{name}"
    return str(Path(data_dir) / name)


# ---------------------------------------------------------------------------
# The instance admin API client
# ---------------------------------------------------------------------------

_TOKEN_SHAPED = re.compile(r"[A-Za-z0-9_\-\.]{20,}")

#: Substrings that mark a payload key as naming a credential. Matched
#: case-insensitively against the key, so the vendor's own
#: ``EXTERNAL_DOCUMENT_LOADER_API_KEY`` and the tool-server connection's bare
#: ``key`` are both covered. Deliberately over-wide: redacting one value too
#: many costs a word of an error message, and redacting one too few prints a
#: live credential.
_CREDENTIAL_KEY_FRAGMENTS: tuple[str, ...] = (
    "key",
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "auth",
    "bearer",
)


def _names_a_credential(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _CREDENTIAL_KEY_FRAGMENTS)


def payload_secrets(payload: Any) -> tuple[str, ...]:
    """Every non-empty string in ``payload`` filed under a credential-NAMED key.

    This is the FLOOR under :meth:`InstanceClient.call`'s ``secrets``
    argument, and the reason a caller cannot get the redaction wrong by
    omission. The document-loader projection posts its loader token as the
    body value of ``EXTERNAL_DOCUMENT_LOADER_API_KEY``
    (``document_loader_projection.py:108, 193``); a 422 from a drifted
    instance answers with ``detail[].input`` echoing that body. The token is
    OPERATOR-CHOSEN, so it can be short and punctuated — under 20 characters
    it slips past :data:`_TOKEN_SHAPED`, and it is not ``self._token``, so the
    identity pass missed it too. Keying on the vendor's own form-key name
    catches it whether or not the call site remembers to declare it.

    Recursive, because a payload nests (``config``, ``headers``, ``info``);
    list elements inherit the key they were filed under.
    """
    found: list[str] = []

    def walk(node: Any, key: str | None) -> None:
        if isinstance(node, Mapping):
            for raw_key, value in node.items():
                walk(value, str(raw_key))
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item, key)
        elif isinstance(node, str) and node and key and _names_a_credential(key):
            found.append(node)

    walk(payload, None)
    return tuple(found)


def redact(text: str, *secrets: str | None) -> str:
    """Scrub anything credential-shaped out of text bound for a log.

    Adversarial-review finding: an HTTP error body carried verbatim into
    ``data['detail']`` can echo a SUBMITTED token back into stderr and into
    the agent JSON. Two passes, because either alone is insufficient.

    Pass one replaces every SUPPLIED secret by identity — no length floor and
    no charset assumption, because these values are operator-chosen and a
    short or punctuated one is exactly the case the shape pass cannot see.
    Pass two replaces any remaining long token-shaped run by shape, which
    catches a credential nobody declared. Bounded afterwards, so a large HTML
    error page cannot bury the reason.

    Longest-first, so a secret that contains another secret as a substring
    cannot be partially rewritten into an unmatchable remnant.
    """
    out = text
    for secret in sorted({s for s in secrets if s}, key=len, reverse=True):
        out = out.replace(secret, "<redacted>")
    out = _TOKEN_SHAPED.sub("<redacted>", out)
    return out[:200]


#: What a tool-server credential is replaced by when it is ABSENT. A distinct
#: marker, not the empty string, so "no credential" and "a credential we did
#: not measure" cannot be confused in a diff.
CONNECTION_KEY_ABSENT = "<none>"

#: The three answers the instance's own verify route can give. Only
#: :data:`VERIFY_REJECTED` is evidence ABOUT THE CREDENTIAL; a version with no
#: such route measures nothing, and reporting that as a dead credential would
#: send an operator to rotate a token that was never the problem.
VERIFY_HEALTHY = "healthy"
VERIFY_REJECTED = "rejected"
VERIFY_UNAVAILABLE = "unavailable"
#: The instance was NOT asked: the connection forwards each signed-in
#: person's own token, which no operator run holds (PACT chat-oidc-login).
VERIFY_NOT_PROBED = "not_probed"

#: What a credential the INSTANCE already masked is replaced by. A masked
#: readback carries nothing about the value, so digesting it would produce a
#: stable-looking digest OF THE MASK — a comparison that can never converge
#: and that reads to an operator like a credential mismatch.
CONNECTION_KEY_MASKED = "<masked>"

#: A readback that is nothing but mask characters. v0.11.0 does NOT mask
#: (measured, #1738), but this module's own comparison contract must survive
#: a version that starts to, so the shape is recognized rather than assumed
#: away.
_MASK_SHAPED = re.compile(r"^[*\u2022\u00b7.\u2026x\s]{3,}$", re.IGNORECASE)

#: Per-RUN salt for the credential digest. An unsalted digest published in an
#: evidence artifact is a confirmation ORACLE: anyone who guesses the token
#: can check the guess offline against the artifact. Generated once per
#: process, so every comparison WITHIN a run still works and no comparison
#: ACROSS runs does — which is exactly the scope the reconcile loop needs.
_CONNECTION_DIGEST_SALT = secrets.token_bytes(32)


def looks_masked(value: Any) -> bool:
    """Whether a readback value is the instance's MASK rather than a secret.

    Shape, because there is no field saying so: a value made only of mask
    characters is not a credential anyone could have chosen, and treating it
    as one turns a version difference into a permanent refusal.
    """
    text = "" if value is None else str(value)
    return bool(text) and bool(_MASK_SHAPED.match(text))


def connection_key_digest(value: Any) -> str:
    """A stable, non-reversible, non-oracular stand-in for a credential.

    MEASURED (spike on #1738, open-webui v0.11.0): ``GET
    /api/v1/configs/tool_servers`` returns ``key`` and any custom
    ``headers`` IN CLEAR — no masking of any kind. So every comparison,
    deviation, note, log line and evidence artifact downstream of that
    readback would otherwise copy the deployment's shared MCP secret.

    Reconciliation therefore compares PRESENCE and DIGEST, never the value:
    equal digests mean the instance holds the credential this run resolved,
    and a mismatch says so without printing either side. The digest is an
    HMAC under a per-run salt, so a digest that reaches a report or an
    evidence file cannot be used to confirm a guessed token later.

    A value that is already MASKED gets :data:`CONNECTION_KEY_MASKED`, not a
    digest of the mask: see :class:`ConnectionKeyExpectation`.
    """
    text = "" if value is None else str(value)
    if not text:
        return CONNECTION_KEY_ABSENT
    if looks_masked(text):
        return CONNECTION_KEY_MASKED
    digest = hmac.new(
        _CONNECTION_DIGEST_SALT, text.encode("utf-8"), hashlib.sha256,
    ).hexdigest()
    return "hmac:" + digest[:16]


class ConnectionKeyExpectation(str):
    """The digest a connection's ``key`` must read back as — or its mask.

    A plain ``str`` carrying the digest, so it renders, serializes and
    compares exactly like one. The single added behavior: it also equals
    :data:`CONNECTION_KEY_MASKED`.

    Why the module needs it. This module's own comparison contract
    (:func:`_normalize`) says the server MASKS ``key`` — written before
    anyone measured it. The #1738 spike measured v0.11.0 returning it in
    clear. Both can be true of different versions, and only one of them
    permits a digest comparison. Against a masking version an unconditional
    digest assertion could NEVER converge: the run would rewrite the
    connection every time and then refuse, on a site whose credential is
    perfectly fine. So the digest is asserted when the readback carries one,
    and mere PRESENCE is asserted when it carries a mask — recorded as a
    note, never as silent agreement.
    """

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str) and str.__eq__(other, CONNECTION_KEY_MASKED):
            return True
        return str.__eq__(self, other)

    def __ne__(self, other: object) -> Any:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    __hash__ = str.__hash__


def redact_connection(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Digest a tool-server connection's credential fields.

    Applied the moment a connection leaves :meth:`InstanceClient.tool_servers`,
    so no caller can construct an unredacted observation by forgetting to.
    Both carriers are covered: the vendor builds the ``Authorization`` header
    from ``auth_type`` + ``key``, then merges ``headers`` on top, so a
    special case on ``key`` alone would leave the other door open.
    """
    out = dict(entry)
    if "key" in out:
        out["key"] = connection_key_digest(out.get("key"))
    headers = out.get("headers")
    if isinstance(headers, Mapping):
        out["headers"] = {
            str(name): (
                connection_key_digest(value)
                if _names_a_credential(str(name))
                else value
            )
            for name, value in headers.items()
        }
    return out


class InstanceClient:
    """Thin admin-API client. The ONLY holder of the admin credential.

    Deliberately not a general HTTP helper: every call targets the operator's
    own instance at :data:`ENV_INSTANCE_URL`, which is why this file carries a
    documented egress-coverage exception rather than routing through the
    public-URL validator (that validator would, correctly, reject the loopback
    address every one of these instances is reachable at).
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 60.0,
        opener: Callable[[urllib.request.Request, float], tuple[int, str]] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._opener = opener
        self.calls: list[tuple[str, str]] = []

    # -- transport ---------------------------------------------------------

    def _open(self, request: urllib.request.Request) -> tuple[int, str]:
        if self._opener is not None:
            return self._opener(request, self._timeout)
        with urllib.request.urlopen(request, timeout=self._timeout) as response:  # noqa: S310
            return response.status, response.read().decode("utf-8")

    def call(
        self,
        method: str,
        path: str,
        payload: Any = None,
        *,
        secrets: tuple[str, ...] = (),
    ) -> Any:
        """One admin API round trip. Raises :class:`ChatSyncError` on failure.

        ``secrets`` names credentials this particular call puts on the wire
        that are NOT the admin token — a document-loader API key, an upstream
        connection key — so they are scrubbed out of any error envelope this
        call raises. Every supplied value is replaced by identity regardless
        of its length or charset: these are operator-chosen, and the
        shape-based sweep only sees runs of 20+ token characters.

        Declaring them is the ceiling, not the floor. :func:`payload_secrets`
        already scrubs every string filed under a credential-NAMED key in
        ``payload``, so a call site that forgets ``secrets`` still does not
        leak the ordinary case. ``secrets`` covers the residual one: a
        credential the vendor files under a key whose name does not say so.
        """
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        outbound = payload_secrets(payload)

        def scrub(text: str) -> str:
            return redact(text, self._token, *outbound, *secrets)

        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={
                "Content-Type": "application/json",
                # The one credential in any rendered payload, resolved from
                # OKG_CHAT_ADMIN_TOKEN and never from manifest content.
                "Authorization": f"Bearer {self._token}",
            },
        )
        self.calls.append((method, path))
        try:
            status, body = self._open(request)
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace") if exc.fp else ""
            raise ChatSyncError(
                f"{method} {path} failed with HTTP {exc.code}",
                data={
                    "method": method,
                    "path": path,
                    "http_status": exc.code,
                    # Redacted: the body of a 401/422 can contain the
                    # submitted payload, and the payload can contain a token.
                    # FastAPI's 422 is the measured case — `detail[].input`
                    # echoes the submitted body verbatim.
                    "detail": scrub(raw),
                    "reason": "http_error",
                },
                fix_hint=(
                    f"check {ENV_INSTANCE_URL} and that {ENV_ADMIN_TOKEN} is "
                    "an admin credential for that instance"
                ),
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise ChatSyncError(
                f"{method} {path} could not reach the instance: "
                f"{scrub(str(exc))}",
                data={"method": method, "path": path, "reason": "unreachable"},
                fix_hint=f"check {ENV_INSTANCE_URL}",
            ) from exc
        if status >= 400:
            raise ChatSyncError(
                f"{method} {path} failed with HTTP {status}",
                data={
                    "method": method,
                    "path": path,
                    "http_status": status,
                    "detail": scrub(body),
                    "reason": "http_error",
                },
                fix_hint="see the instance log",
            )
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body

    def get_bytes(self, path: str) -> bytes | None:
        """Fetch a static asset. ``None`` when the instance does not serve it.

        Static assets are the ``file``-mechanism readback: byte identity is
        the only "which logo is configured" surface the vendor exposes.
        """
        request = urllib.request.Request(
            f"{self.base_url}{path}", method="GET",
        )
        self.calls.append(("GET", path))
        try:
            if self._opener is not None:
                status, body = self._opener(request, self._timeout)
                return body.encode("utf-8") if status < 400 else None
            with urllib.request.urlopen(request, timeout=self._timeout) as response:  # noqa: S310
                if response.status >= 400:
                    return None
                return response.read()
        except (urllib.error.HTTPError, urllib.error.URLError, OSError):
            return None

    # -- named reads -------------------------------------------------------

    def version(self) -> str:
        payload = self.call("GET", "/api/version") or {}
        version = payload.get("version") if isinstance(payload, Mapping) else None
        if not isinstance(version, str) or not version:
            raise ChatSyncError(
                "the instance did not report a version at GET /api/version. "
                "The census is keyed by version; with no version there is "
                "nothing to check the census against.",
                data={"reason": "version_unreadable", "observed": repr(payload)},
                fix_hint="check that the instance is Open WebUI and healthy",
            )
        return version

    def public_config(self) -> Mapping[str, Any]:
        payload = self.call("GET", "/api/config")
        return payload if isinstance(payload, Mapping) else {}

    def config_export(self) -> Mapping[str, Any]:
        payload = self.call("GET", "/api/v1/configs/export")
        if not isinstance(payload, Mapping):
            raise ChatSyncError(
                "GET /api/v1/configs/export did not return an object, so no "
                "governed toggle can be read back.",
                data={"reason": "export_unreadable"},
                fix_hint="check the admin credential",
            )
        return payload

    def config_import(self, values: Mapping[str, Any]) -> None:
        """Upsert FLAT DOTTED keys.

        Flat, never nested: Open WebUI's config table is one row per dotted
        key, and ``/configs/import`` upserts whatever it is given. A nested
        dict creates a row named ``chat`` that the application never reads,
        round-trips cleanly through ``/configs/export``, and looks exactly
        like a knob that worked. The program's own census hit that trap twice.
        """
        for key in values:
            if not isinstance(key, str) or "." not in key:
                raise ChatSyncError(
                    f"refusing to import the config key {key!r}: keys must be "
                    "flat dotted paths. A nested object writes a row the "
                    "application never reads and round-trips perfectly.",
                    data={"key": repr(key), "reason": "nested_config_key"},
                    fix_hint="use the flat dotted key the census recorded",
                )
        self.call("POST", "/api/v1/configs/import", {"config": dict(values)})

    def permissions(self) -> Mapping[str, Any]:
        payload = self.call("GET", "/api/v1/users/default/permissions")
        if not isinstance(payload, Mapping):
            raise ChatSyncError(
                "GET /api/v1/users/default/permissions did not return an "
                "object; the per-user permission toggles cannot be read back.",
                data={"reason": "permissions_unreadable"},
                fix_hint="check the admin credential",
            )
        return payload

    def set_permissions(self, permissions: Mapping[str, Any]) -> None:
        # Whole-object replacement (``routers/users.py:406`` re-validates the
        # full UserPermissions form), so this is always read-modify-write.
        self.call("POST", "/api/v1/users/default/permissions", dict(permissions))

    def connections_config(self) -> Mapping[str, Any]:
        payload = self.call("GET", "/api/v1/configs/connections")
        return payload if isinstance(payload, Mapping) else {}

    def set_connections_config(self, payload: Mapping[str, Any]) -> None:
        self.call("POST", "/api/v1/configs/connections", dict(payload))

    def banners(self) -> list[Any]:
        payload = self.call("GET", "/api/v1/configs/banners")
        return list(payload) if isinstance(payload, list) else []

    def set_banners(self, banners: Sequence[Mapping[str, Any]]) -> None:
        self.call("POST", "/api/v1/configs/banners", {"banners": list(banners)})

    def set_prompt_suggestions(
        self, suggestions: Sequence[Any], i18n: Mapping[str, Any],
    ) -> None:
        # routers/configs.py:817-840 at v0.11.4: upserts BOTH
        # `ui.prompt_suggestions` and `ui.prompt_suggestions_i18n`.
        self.call(
            "POST",
            "/api/v1/configs/suggestions",
            {"suggestions": list(suggestions), "i18n": dict(i18n)},
        )

    def user_settings(self) -> Mapping[str, Any]:
        """The CALLER's settings, merged with the site's interface defaults.

        ``routers/users.py:457-475`` at v0.11.4: the merged view is what the
        browser loads into its settings store, so it is the value that decides
        whether the What's New modal opens.
        """
        payload = self.call("GET", "/api/v1/users/user/settings")
        return payload if isinstance(payload, Mapping) else {}

    def update_user_ui_settings(self, ui: Mapping[str, Any]) -> None:
        # A field-level patch of `settings.ui` (models/users.py:832-842):
        # omitted fields are kept, so this cannot clear the admin's others.
        self.call("POST", "/api/v1/users/user/settings/update", {"ui": dict(ui)})

    def models_config(self) -> Mapping[str, Any]:
        payload = self.call("GET", "/api/v1/configs/models")
        return payload if isinstance(payload, Mapping) else {}

    def set_models_config(self, payload: Mapping[str, Any]) -> None:
        self.call("POST", "/api/v1/configs/models", dict(payload))

    def models_export(self) -> list[Mapping[str, Any]]:
        payload = self.call("GET", "/api/v1/models/export")
        return [row for row in payload if isinstance(row, Mapping)] if isinstance(payload, list) else []

    def models_list(self) -> list[Mapping[str, Any]]:
        """Read the complete workspace model inventory.

        ``/api/v1/models/export`` can return ``[]`` for an API-key caller even
        while the same instance lists models elsewhere.  The workspace route
        is the authoritative inventory for create-vs-update, but only when its
        envelope proves completeness.  A bare list, missing count, malformed
        row, duplicate id, changing total, or truncated page is a refusal —
        never evidence that a model is absent.
        """
        # Keep the route and measured page size aligned with the canonical
        # source-admin readback contract; this surface adds the stricter
        # create-vs-update id checks on top of the same `{items,total}` wire.
        from okg.substrate.chat.source_admin import (
            WORKSPACE_MODEL_LIST_PAGE_SIZE,
            WORKSPACE_MODEL_LIST_ROUTE,
        )

        route = WORKSPACE_MODEL_LIST_ROUTE
        page_size = WORKSPACE_MODEL_LIST_PAGE_SIZE
        rows: list[Mapping[str, Any]] = []
        total: int | None = None
        page = 1
        while True:
            payload = self.call("GET", f"{route}?page={page}")
            if not isinstance(payload, Mapping):
                raise ChatSyncError(
                    f"{route} returned a non-object payload, so the model "
                    "inventory cannot prove which ids already exist.",
                    data={"reason": "models_listing_malformed", "page": page},
                    fix_hint=f"check the authenticated {route} response",
                )
            items = payload.get("items")
            raw_total = payload.get("total")
            if (
                not isinstance(items, list)
                or isinstance(raw_total, bool)
                or not isinstance(raw_total, int)
                or raw_total < 0
            ):
                raise ChatSyncError(
                    f"{route} page {page} is missing a valid `items` list or "
                    "non-negative integer `total`; an incomplete inventory "
                    "is not evidence of absence.",
                    data={
                        "reason": "models_listing_malformed",
                        "page": page,
                        "payload_shape": type(payload).__name__,
                    },
                    fix_hint=f"read the complete workspace listing from {route}",
                )
            if total is None:
                total = raw_total
            elif total != raw_total:
                raise ChatSyncError(
                    f"{route} changed its total during reconciliation "
                    f"({total} then {raw_total}); refusing a mixed snapshot.",
                    data={
                        "reason": "models_listing_changed",
                        "page": page,
                        "totals": [total, raw_total],
                    },
                    fix_hint="retry sync after the instance model list is stable",
                )
            for index, row in enumerate(items):
                if not isinstance(row, Mapping):
                    raise ChatSyncError(
                        f"{route} page {page} item {index} is not an object; "
                        "refusing to infer model absence from malformed data.",
                        data={
                            "reason": "models_listing_malformed",
                            "page": page,
                            "item": index,
                        },
                        fix_hint=f"check the authenticated {route} response",
                    )
                model_id = row.get("id")
                if not isinstance(model_id, str) or not model_id.strip():
                    raise ChatSyncError(
                        f"{route} page {page} item {index} has no non-empty "
                        "model id; refusing to guess create-vs-update.",
                        data={
                            "reason": "models_listing_malformed",
                            "page": page,
                            "item": index,
                        },
                        fix_hint=f"check the authenticated {route} response",
                    )
                rows.append(row)
            if len(rows) > total:
                raise ChatSyncError(
                    f"{route} returned more rows than its declared total "
                    f"({len(rows)} > {total}); refusing a mixed snapshot.",
                    data={
                        "reason": "models_listing_malformed",
                        "page": page,
                        "seen": len(rows),
                        "total": total,
                    },
                    fix_hint=f"check the authenticated {route} response",
                )
            if len(rows) == total:
                break
            if not items or len(items) < page_size and len(rows) < total:
                raise ChatSyncError(
                    f"{route} returned only {len(rows)} of {total} rows; the "
                    "authoritative inventory is truncated.",
                    data={
                        "reason": "models_listing_truncated",
                        "page": page,
                        "seen": len(rows),
                        "total": total,
                    },
                    fix_hint=f"read every page from {route} without filtering by id",
                )
            page += 1
            if page > 1000:
                raise ChatSyncError(
                    f"{route} exceeded the safe pagination bound while "
                    "building the authoritative model inventory.",
                    data={"reason": "models_listing_unbounded"},
                    fix_hint="check the instance listing total and pagination",
                )
        ids = [str(row["id"]) for row in rows]
        if len(ids) != len(set(ids)):
            raise ChatSyncError(
                f"{route} returned duplicate model ids; refusing to select a "
                "create or update path from an ambiguous inventory.",
                data={"reason": "models_listing_duplicate_ids"},
                fix_hint=f"check the authenticated {route} response",
            )
        return rows

    def model_row(self, model_id: str) -> Mapping[str, Any] | None:
        """Single-row readback that can see rows the listings cannot.

        MEASURED at v0.11.0 (live okg-workspace bring-up, 2026-08-18): both
        workspace listing surfaces hide rows whose ``base_model_id`` is null
        — ``/list`` and ``/export`` run through ``Models.search_models`` /
        ``get_models``, and both filter ``Model.base_model_id != None``
        (vendor models/models.py:346 and :249). The builtin rows the models
        surface writes are exactly that shape, so a listing-based presence
        check reads them as absent forever and the create arm loops into
        "already registered" errors. The per-id route
        (vendor routers/models.py:515) reads the table unfiltered.
        """
        try:
            payload = self.call(
                "GET",
                f"/api/v1/models/model?id={urllib.parse.quote(model_id, safe='')}",
            )
        except ChatSyncError as err:
            # Row-absent must read as None, not as a failed run: the vendor
            # route answers 200/null for a missing row, but a 404-shaped
            # answer on any path would otherwise fire before create_model
            # and make a fresh instance unbootstrappable (seam recorded by
            # chat-tool-surface, 2026-08-18). Any other status stays an
            # error — absence is the only thing 404 is allowed to mean.
            if (err.data or {}).get("http_status") == 404:
                return None
            raise
        return payload if isinstance(payload, Mapping) else None

    def create_model(self, form: Mapping[str, Any]) -> None:
        self.call("POST", "/api/v1/models/create", dict(form))

    def update_model(self, form: Mapping[str, Any]) -> None:
        # F7: /api/v1/models/sync CANNOT update at v0.11.0 and fails silently
        # with HTTP 200 and a body of []. The single-row endpoint is the only
        # working path, and confirmation comes from models_list(), never this
        # write response.
        self.call(
            "POST", f"/api/v1/models/model/update?id={form['id']}", dict(form),
        )

    def tool_servers_raw(self) -> list[Mapping[str, Any]]:
        """The registered connections EXACTLY as the instance holds them.

        There is ONE legitimate consumer: the preserve-and-rewrite round trip
        in the preset surface. ``POST /api/v1/configs/tool_servers`` replaces
        the WHOLE list, so every entry this deployment does not own must be
        written back byte-identical — and handing that path the redacted view
        would overwrite a foreign tool server's credential with the literal
        digest string, silently and irreversibly.

        Nothing may OBSERVE these entries: no diff, note, report field, log
        line or evidence artifact. Anything that is looked at reads
        :meth:`tool_servers`.
        """
        payload = self.call("GET", "/api/v1/configs/tool_servers")
        entries = (payload or {}).get("TOOL_SERVER_CONNECTIONS") if isinstance(payload, Mapping) else None
        if not isinstance(entries, list):
            return []
        return [e for e in entries if isinstance(e, Mapping)]

    def tool_servers(self) -> list[Mapping[str, Any]]:
        """The registered connections, with every credential already digested.

        The redaction is HERE, at the boundary, and not at the call sites:
        the vendor returns the shared MCP secret in clear, and a caller that
        forgot would put it into a deviation, a note or an evidence file. No
        OBSERVATION downstream of this method can see the value, which is
        also why re-registration builds its payload from the resolved
        environment credential rather than from what it read back.

        The one path that must not go through here is the round trip that
        REWRITES the list, because a foreign entry has to survive it
        unchanged; that path, and only that path, uses
        :meth:`tool_servers_raw`.
        """
        return [redact_connection(e) for e in self.tool_servers_raw()]

    def verify_tool_server(self, connection: Mapping[str, Any]) -> tuple[str, Any]:
        """Ask the instance to dial one connection with the credential given.

        MEASURED (spike on #1738, v0.11.0): ``POST
        /api/v1/configs/tool_servers/verify`` answers ``200`` with
        ``status: true`` and the operator specs when the credential works,
        and ``400 Failed to create MCP client`` when ``mcp-serve`` answers
        ``401``. Vendor ``routers/configs.py:546`` and ``utils/tools.py:144``
        build the header from ``auth_type`` + ``key`` on BOTH the verify and
        the real tool-call path, so the verify route authenticates the way a
        chat turn does. It does NOT use the credential the instance STORED:
        it dials with the ``key`` in ``connection``, which callers build from
        this process's environment. A healthy answer therefore proves that
        credential works; after a token rotation that was never synced the
        site still holds the old one and every chat turn gets ``401``
        (#2287). Pinned 0.11.4 has no route that dials a stored connection
        by id, so readback compares digests of the stored and the resolved
        credential separately (``plan_apply._stored_credential_state``).

        Returns ``(state, detail)`` with state one of
        :data:`VERIFY_HEALTHY`, :data:`VERIFY_REJECTED` or
        :data:`VERIFY_UNAVAILABLE`. An HTTP failure is an answer here, not an
        exception: a dead credential is a measurement. The three states are
        distinct because only ONE of them is evidence about the credential —
        a version with no verify route says nothing about it at all.
        """
        try:
            payload = self.call(
                "POST",
                "/api/v1/configs/tool_servers/verify",
                dict(connection),
            )
        except ChatSyncError as err:
            status = err.data.get("http_status")
            # 400 is the MEASURED verdict for a connection the instance could
            # not use ("Failed to create MCP client").
            if status == 400:
                return VERIFY_REJECTED, "HTTP 400"
            # 404 is a version WITHOUT this route. The probe is unavailable,
            # which is not a finding about the credential and must not fail
            # a run whose every other surface reconciled.
            if status == 404:
                return VERIFY_UNAVAILABLE, "HTTP 404 (no verify route)"
            # Every OTHER status is this route failing, not the credential
            # failing — a 401 on the ADMIN token, a 500 — and answering
            # "credential dead" to those would blame the wrong thing, so they
            # propagate naming the route and the status.
            raise
        if not isinstance(payload, Mapping):
            # Not evidence about the credential either: a route that answers
            # a shape this client cannot read has not measured anything.
            return VERIFY_UNAVAILABLE, "non-object verify response"
        specs = payload.get("specs")
        if not payload.get("status"):
            return VERIFY_REJECTED, "verify answered status false"
        if not isinstance(specs, list) or not specs:
            # An empty spec list is the absence of a readback, not the
            # absence of tools: accepting it would make the probe pass on a
            # connection that offers a chat turn nothing.
            return VERIFY_REJECTED, "verify returned an empty spec list"
        return VERIFY_HEALTHY, len(specs)

    def set_tool_servers(self, connections: Sequence[Mapping[str, Any]]) -> None:
        self.call(
            "POST",
            "/api/v1/configs/tool_servers",
            {"TOOL_SERVER_CONNECTIONS": list(connections)},
        )

    def prompts(self) -> list[Mapping[str, Any]]:
        payload = self.call("GET", "/api/v1/prompts/")
        return [p for p in payload if isinstance(p, Mapping)] if isinstance(payload, list) else []

    def create_prompt(self, form: Mapping[str, Any]) -> None:
        self.call("POST", "/api/v1/prompts/create", dict(form))

    def update_prompt(self, prompt_id: str, form: Mapping[str, Any]) -> None:
        self.call("POST", f"/api/v1/prompts/id/{prompt_id}/update", dict(form))

    def functions(self) -> list[Mapping[str, Any]]:
        payload = self.call("GET", "/api/v1/functions/")
        return [f for f in payload if isinstance(f, Mapping)] if isinstance(payload, list) else []

    def function_valves_spec(self, function_id: str) -> Mapping[str, Any]:
        payload = self.call("GET", f"/api/v1/functions/id/{function_id}/valves/spec")
        return payload if isinstance(payload, Mapping) else {}

    def function_valves(self, function_id: str) -> Mapping[str, Any]:
        payload = self.call("GET", f"/api/v1/functions/id/{function_id}/valves")
        return payload if isinstance(payload, Mapping) else {}

    def set_function_valves(
        self, function_id: str, valves: Mapping[str, Any],
    ) -> None:
        self.call(
            "POST",
            f"/api/v1/functions/id/{function_id}/valves/update",
            dict(valves),
        )

    def groups(self) -> list[Mapping[str, Any]]:
        payload = self.call("GET", "/api/v1/groups/")
        return [g for g in payload if isinstance(g, Mapping)] if isinstance(payload, list) else []


# ---------------------------------------------------------------------------
# The census, consulted AT RUN TIME
# ---------------------------------------------------------------------------

def _census_payload(*, data_dir: Path | None) -> Mapping[str, Any]:
    from okg.substrate.deployments.chat_block import (
        ChatUiCensusUnavailable,
        ui_knob_census_path,
    )

    artifact = ui_knob_census_path(data_dir)
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census artifact is missing or unreadable: "
            f"{artifact} ({exc}). There is no admitted knob set, so no knob "
            "is driven — the run refuses rather than reusing a remembered set.",
            data={"artifact": str(artifact), "reason": "missing_or_unreadable"},
            fix_hint="record the census via the umbrella's read-handshake task",
        ) from exc
    if not isinstance(payload, Mapping):
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census artifact is not a JSON object: {artifact}",
            data={"artifact": str(artifact), "reason": "not_a_mapping"},
            fix_hint="re-record the census artifact",
        )
    return payload


def census_admitted_knobs(
    *, data_dir: Path | None = None, running_version: str | None = None,
) -> tuple[frozenset[str], frozenset[str], str]:
    """``(admitted chat.ui knobs, admitted feature leaves, census version)``.

    The census is consulted as the RECORDED ARTIFACT, never restated inline.
    When it is missing, unreadable, or records an ``openwebui_version`` other
    than the running instance's, this raises: a version bump that shrinks the
    drivable set has to show up as a refusal, not as silent no-ops.
    """
    from okg.substrate.deployments.chat_block import (
        ChatUiCensusUnavailable,
        load_ui_feature_census,
        load_ui_knob_census,
        ui_knob_census_path,
    )

    payload = _census_payload(data_dir=data_dir)
    census_version = payload.get("openwebui_version")
    if not isinstance(census_version, str) or not census_version:
        raise ChatUiCensusUnavailable(
            "the chat.ui knob census records no openwebui_version, so there "
            "is nothing to check the running instance against.",
            data={"reason": "census_version_absent"},
            fix_hint="re-record the census artifact",
        )
    if running_version is not None and running_version != census_version:
        raise ChatUiCensusUnavailable(
            f"the recorded chat.ui knob census was taken at Open WebUI "
            f"{census_version!r} but the instance is running "
            f"{running_version!r}. A census taken at another version proves "
            "nothing about the knobs this run would drive, so the run refuses "
            "rather than reusing the old census.",
            data={
                "reason": "census_version_mismatch",
                "census_version": census_version,
                "running_version": running_version,
                "artifact": str(ui_knob_census_path(data_dir)),
            },
            fix_hint=(
                "re-record the census at the running version, or run against "
                "an instance at the census version"
            ),
        )
    return (
        load_ui_knob_census(data_dir=data_dir),
        load_ui_feature_census(data_dir=data_dir),
        census_version,
    )


def census_mechanisms(*, data_dir: Path | None = None) -> dict[str, str]:
    """``chat.ui`` knob name -> the census-recorded mechanism.

    The mechanism decides whether this verb WRITES a knob or merely ASSERTS
    it. Reading it from the artifact rather than hard-coding it means a
    re-measurement that moves ``site_name`` onto an admin API automatically
    moves it into the written set.
    """
    payload = _census_payload(data_dir=data_dir)
    mechanisms: dict[str, str] = {}
    for entry in payload.get("knobs") or []:
        if not isinstance(entry, Mapping) or entry.get("drivable") is not True:
            continue
        manifest_field = entry.get("manifest_field")
        mechanism = entry.get("mechanism")
        if not isinstance(manifest_field, str) or not isinstance(mechanism, str):
            continue
        head, _, rest = manifest_field.partition(".")
        section, _, leaf = rest.partition(".")
        if head != "chat" or section != "ui" or not leaf:
            continue
        knob = leaf.partition(".")[0]
        # A container knob (``features``) takes the STRICTEST mechanism of its
        # leaves: if any leaf needs a restart the container cannot be claimed
        # API-writable. In practice every measured feature leaf is ``api``.
        if knob in mechanisms and mechanisms[knob] != mechanism:
            mechanisms[knob] = Mechanism.ENV
        else:
            mechanisms[knob] = mechanism
    return mechanisms


def census_excluded_knobs(
    *, data_dir: Path | None = None,
) -> list[dict[str, str]]:
    """Every knob the recorded census measured and did NOT admit, with its reason.

    Printed on every run. req.readback-no-partial-apply requires each
    census exclusion to be named with its reason ALONGSIDE the count of
    admitted knobs actually driven, so a driven set that shrinks toward empty
    is visible in the output instead of silently satisfying a self-scoped
    quantifier. Reading the exclusions off the artifact rather than listing
    them here means a re-recorded census changes the run's output by itself.
    """
    payload = _census_payload(data_dir=data_dir)
    excluded: list[dict[str, str]] = []
    for entry in payload.get("knobs") or []:
        if not isinstance(entry, Mapping) or entry.get("drivable") is True:
            continue
        field = entry.get("manifest_field")
        if not isinstance(field, str):
            continue
        excluded.append({
            "knob": field,
            "mechanism": str(entry.get("mechanism")),
            "reason": str(
                entry.get("notes")
                or entry.get("mechanism_detail")
                or "the census records it as not drivable-with-readback"
            )[:400],
        })
    return sorted(excluded, key=lambda item: item["knob"])


def driven_knob_set(*, data_dir: Path | None = None) -> frozenset[str]:
    """The ``chat.ui`` knobs this verb governs, stated as a fact about the code.

    Bound to the census-admitted set by
    ``test_driven_knob_set_equals_census``. It is derived from the artifact
    rather than listed here so the two cannot drift.
    """
    admitted, _features, _version = census_admitted_knobs(data_dir=data_dir)
    return admitted


def require_census_admits(
    requested: Iterable[str],
    admitted: frozenset[str],
    *,
    what: str,
    artifact: Path,
) -> None:
    """Refuse when the manifest asks for a knob the census does not admit."""
    from okg.substrate.deployments.chat_block import ChatUiCensusUnavailable

    outside = sorted(set(requested) - admitted)
    if not outside:
        return
    raise ChatUiCensusUnavailable(
        f"the manifest requests {what} {outside}, which the recorded knob "
        f"census does not admit as drivable-with-readback. The census at "
        f"{artifact} admits {sorted(admitted)}. An unadmitted knob is not "
        "driven best-effort — a write nobody can read back is a no-op that "
        "looks like configuration.",
        data={
            "reason": "knob_outside_census",
            "requested": outside,
            "admitted": sorted(admitted),
            "artifact": str(artifact),
            "what": what,
        },
        fix_hint=(
            f"remove {outside} from the manifest, or re-record the census "
            "with those knobs measured drivable"
        ),
    )


# ---------------------------------------------------------------------------
# F8: the site-name transform
# ---------------------------------------------------------------------------

def expected_site_name(declared: str) -> str:
    """The value ``GET /api/config -> .name`` will report for ``declared``.

    ``env.py:892-893`` appends ``' (Open WebUI)'`` to every value that is not
    the vendor default. Comparing the readback against the raw manifest value
    reports drift on every single run; this is the normalization that makes
    the comparison mean what it says.
    """
    if declared == SITE_NAME_VENDOR_DEFAULT:
        return declared
    if declared.endswith(SITE_NAME_SUFFIX):
        return declared
    return declared + SITE_NAME_SUFFIX


# ---------------------------------------------------------------------------
# The masking posture predicate (req.query-exclusion-on-masked-deployments)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Posture:
    """The deployment's masking posture, and why.

    ``indeterminate`` is a THIRD state that resolves to masked. A deployment
    whose catalog cannot be composed or whose nomos block cannot be read is
    not "probably unmasked"; it is unknown, and unknown excludes.
    """

    masked: bool
    indeterminate: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "masked": self.masked,
            "indeterminate": self.indeterminate,
            "reason": self.reason,
        }


def resolve_posture(
    deployment: str,
    raw_manifest: Mapping[str, Any],
    *,
    deployment_dir: Path | None = None,
    catalog_loader: Callable[[], Any] | None = None,
) -> Posture:
    """EXACTLY the predicate the requirement states, and nothing else.

    Masked when ``nomos.runtime_enforcement`` is any value other than ``off``,
    OR when any subtype in the composed catalog declares
    ``okg.subtypes.metadata.pii_classes``. There is no manifest field named
    "masking posture"; this predicate IS the definition.

    Indeterminate — the nomos block is unreadable, or the catalog cannot be
    composed — is treated as masked, never as unmasked.
    """
    nomos = raw_manifest.get("nomos")
    if nomos is not None and not isinstance(nomos, Mapping):
        return Posture(
            masked=True,
            indeterminate=True,
            reason=(
                f"deployment {deployment!r} has an unreadable nomos block "
                f"({type(nomos).__name__}), so the masking predicate cannot "
                "be evaluated; an indeterminate posture is treated as masked"
            ),
        )
    enforcement = (nomos or {}).get("runtime_enforcement")
    if enforcement is not None and enforcement != "off":
        return Posture(
            masked=True,
            indeterminate=False,
            reason=(
                f"nomos.runtime_enforcement is {enforcement!r} (any value "
                "other than 'off' is masked)"
            ),
        )

    def _default_loader() -> Any:
        from okg.substrate.catalog.modules_compose import compose_catalog

        return compose_catalog(deployment_dir or Path("."))

    loader = catalog_loader or _default_loader
    try:
        catalog = loader()
    except Exception as exc:  # noqa: BLE001 — every compose failure is UNKNOWN
        return Posture(
            masked=True,
            indeterminate=True,
            reason=(
                f"the composed catalog for {deployment!r} could not be read "
                f"({type(exc).__name__}: {exc}), so the pii_classes half of "
                "the predicate cannot be evaluated; an indeterminate posture "
                "is treated as masked"
            ),
        )
    metadata = getattr(catalog, "subtype_metadata", None) or {}
    declaring = sorted(
        subtype
        for subtype, meta in metadata.items()
        if isinstance(meta, Mapping) and "pii_classes" in meta
    )
    if declaring:
        return Posture(
            masked=True,
            indeterminate=False,
            reason=(
                f"{len(declaring)} composed subtype(s) declare "
                f"okg.subtypes.metadata.pii_classes (first: {declaring[0]})"
            ),
        )
    return Posture(
        masked=False,
        indeterminate=False,
        reason=(
            "nomos.runtime_enforcement is 'off' and no composed subtype "
            "declares pii_classes"
        ),
    )


#: The tool this verb removes from a masked deployment's preset. The exclusion
#: is lifted only by the mcp-http-auth child's proof that Nomos masking covers
#: the bounded query operator's output; that child has MEASURED that it does
#: not, in any cell. This module does not prove masking coverage and must not
#: claim it.
QUERY_TOOL = "query"


# ---------------------------------------------------------------------------
# F4: the connection tool filter, replicated so the wire can be ASSERTED
# ---------------------------------------------------------------------------

def is_string_allowed(name: str, filter_list: Sequence[str]) -> bool:
    """Open WebUI's own predicate (``utils/misc.py:56``), replicated exactly.

    Replicated rather than approximated because it has a trap: the match is
    ``endswith``, not equality, so an allow entry ``search`` also admits
    ``deep_search`` and a block entry ``!query`` blocks anything ending in
    ``query``. Replicating it lets this verb compute the EFFECTIVE bound set
    against the live ``tools/list`` and assert that ``query`` really is gone,
    instead of trusting that the filter list it wrote means what it looks like.
    """
    if not [e for e in filter_list if (e or "").strip()]:
        return True
    allow: list[str] = []
    block: list[str] = []
    for raw in filter_list:
        entry = _strip_filter_entry(raw)
        blocked = entry.startswith("!")
        if blocked:
            entry = _strip_filter_entry(entry[1:])
        if not entry:
            continue
        (block if blocked else allow).append(entry)
    if allow and not any(name.endswith(a) for a in allow):
        return False
    if any(name.endswith(b) for b in block):
        return False
    return True


def _strip_filter_entry(entry: str | None) -> str:
    """``utils/misc.py:32-34``. Surrounding quotes are stripped, then re-stripped.

    Immaterial for MCP tool names, which never carry quotes — but this
    function claims to replicate the vendor's predicate exactly, and a
    replica that quietly diverges is worse than no replica: it would compute
    an effective bound set the instance does not agree with, and this module
    ASSERTS on that set.
    """
    return (entry or "").strip().strip("\"'").strip()


def effective_tool_binding(
    live_tools: Sequence[str], filter_list: Sequence[str],
) -> tuple[str, ...]:
    """The tools the instance will actually offer, given a filter list."""
    return tuple(
        sorted(name for name in live_tools if is_string_allowed(name, filter_list))
    )


def tool_filter_list(
    requested: Sequence[str], *, excluded: Sequence[str] = (),
) -> tuple[str, ...]:
    """The ``config.function_name_filter_list`` for a bound tool set.

    Allow entries for what stays, plus an explicit ``!name`` block for every
    exclusion. The block arm is not redundant: allow matching is ``endswith``,
    so a permitted name could otherwise re-admit an excluded one as a suffix.
    Blocks are applied after allows and always win.
    """
    keep = [t for t in requested if t not in set(excluded)]
    return tuple(keep) + tuple(f"!{name}" for name in excluded)


# ---------------------------------------------------------------------------
# Tool curation: carrying the DECLARED set onto the one lever that works
# (PACT chat-tool-surface, req.curation-reaches-the-wire)
# ---------------------------------------------------------------------------
#
# Declaration and load-time validation are NOT here: a tool named outside the
# admitted surface raising, and an empty ADMITTED surface refusing every tool,
# are `chat_block.validate_requested_tools`. This section owns the step after
# — getting the declared set onto the wire — and the two vendor semantics that
# invert its meaning on the way.
#
# The lever is the per-CONNECTION `function_name_filter_list`
# (utils/middleware.py:2237-2243, re-verified against the running v0.11.0
# container). It is the ONLY per-tool lever. A PRESET cannot narrow an MCP
# server: :2740 matches the `server:mcp:` prefix, :2742 takes the WHOLE
# remainder as a server id, and :2757 iterates every spec the server
# advertises and registers each one — so dropping an operator from a preset's
# toolIds drops the entire server. (The `server_id|function_names` grammar at
# utils/tools.py:382-385 is parsed on the OpenAPI path only, and is dead even
# there: that path filters on the connection's own
# `function_name_filter_list` at :413 instead.)

#: The manifest named the set.
DECLARATION_MANIFEST = "manifest"
#: The deployment named nothing, so :data:`CHAT_DEFAULT_TOOLS` applies.
DECLARATION_SUBSTRATE_DEFAULT = "substrate_default"
#: The deployment declares `agent_surface.projections.chat.tools` — THE
#: statement of what this client is given. Named apart from
#: `DECLARATION_MANIFEST` so the run's readback says which of the two
#: declarations it read, rather than reporting one as the other.
DECLARATION_AGENT_SURFACE = "agent_surface"

#: The connection exists and serves the curated set.
CONNECTION_SERVING_DECLARED = "connection_present_serving_declared_tools"
#: The connection exists and serves NOTHING. Named separately from "absent"
#: because a readback that cannot tell those apart proves nothing: zero tools
#: read off a missing connection is the absence of a measurement.
CONNECTION_SERVING_NOTHING = "connection_present_serving_nothing"

#: An allow entry no tool name can end with, so the allow arm of
#: ``is_string_allowed`` (utils/misc.py:71) rejects every advertised name.
#: Chosen over an EMPTY list because empty means ALLOW ALL — see
#: :func:`deny_all_filter_list`.
DENY_ALL_SENTINEL = "__okg_admits_no_tool__"


def deny_all_filter_list(advertised: Sequence[str]) -> tuple[str, ...]:
    """A filter list that admits NOTHING. TRAP 2's only correct answer.

    Open WebUI reads an EMPTY filter list as ALLOW ALL, twice over:
    ``is_string_allowed`` returns True immediately on a falsy list
    (utils/misc.py:63-64), and even the string form ``''`` — which
    ``middleware.py:2239`` splits into the truthy ``['']`` and so does not
    skip at :2242 — composes to an empty allow list and an empty block list,
    which admits everything. Writing ``''`` for "no tools" would therefore
    hand the model the entire server.

    So the empty declared set is expressed POSITIVELY, in two arms: a
    sentinel ALLOW entry no advertised name can end with, plus an explicit
    ``!name`` BLOCK for every advertised name. One arm is a claim about the
    matcher, the other a claim about today's listing.

    Each arm is checked HERE against the composed list, on its own, and a
    leak refuses. Two claims of independence that nothing checks are one
    claim with a comment: an adversarial review deleted the block arm and
    changed the sentinel to a real tool name, and both survived, because the
    surviving arm covered for the broken one. The loop below is the check
    that makes each arm carry its own weight — it reads the arms back OUT of
    the returned list, so deleting either one is caught by the other's pass.
    """
    names = tuple(sorted(set(advertised)))
    filter_list = (DENY_ALL_SENTINEL,) + tuple(f"!{name}" for name in names)
    for arm_name, arm in (
        ("allow", tuple(e for e in filter_list if not e.startswith("!"))),
        ("block", tuple(e for e in filter_list if e.startswith("!"))),
    ):
        leaked = tuple(n for n in names if is_string_allowed(n, list(arm)))
        if leaked:
            raise ChatSyncError(
                f"the deny-everything filter's {arm_name} arm {list(arm)} "
                f"would admit {list(leaked)} on its own. Both arms of that "
                "filter must admit nothing independently: the allow arm is a "
                "claim about the ENDSWITH matcher (utils/misc.py:71) and the "
                "block arm is a claim about the advertised listing "
                "(utils/misc.py:75). An arm that leaks is one bug away from "
                "serving the whole MCP server to a deployment that declared "
                "no tools, and the other arm hiding it is exactly how that "
                "bug ships.",
                data={
                    "reason": "deny_all_arm_leaks",
                    "surface": SURFACE_PRESET,
                    "arm": arm_name,
                    "entries": list(arm),
                    "leaked": list(leaked),
                    "sentinel": DENY_ALL_SENTINEL,
                    "advertised": list(names),
                },
                fix_hint=(
                    "DENY_ALL_SENTINEL must be a string no advertised tool "
                    "name can END WITH, and every advertised name must get "
                    "its own '!name' block entry"
                ),
            )
    return filter_list


@dataclass(frozen=True)
class ToolCurationPlan:
    """What the deployment asked for, before the wire is consulted."""

    declared: tuple[str, ...]
    declaration_source: str
    excluded: tuple[str, ...]
    #: ``declared`` minus posture exclusions. The set the connection must
    #: serve, exactly — no more (a widening) and no fewer (a silent loss).
    intended: tuple[str, ...]
    audiences: tuple[tuple[str, tuple[str, ...]], ...] = ()


@dataclass(frozen=True)
class ToolCuration:
    """The plan, bound against what the server actually advertises."""

    plan: ToolCurationPlan
    advertised: tuple[str, ...]
    filter_list: tuple[str, ...]
    #: What the vendor's own matcher will admit, computed against
    #: ``advertised``. Never assumed equal to ``filter_list``.
    admitted: tuple[str, ...]
    connection_state: str

    @property
    def declared(self) -> tuple[str, ...]:
        return self.plan.declared

    @property
    def intended(self) -> tuple[str, ...]:
        return self.plan.intended

    def to_dict(self) -> dict[str, Any]:
        return {
            "declared": list(self.plan.declared),
            "declaration_source": self.plan.declaration_source,
            # Printed on EVERY run, declared or not, so a reader never has to
            # ask what the unstated case would have been.
            "substrate_default_set": list(CHAT_DEFAULT_TOOLS),
            "posture_excluded": list(self.plan.excluded),
            "intended": list(self.plan.intended),
            "advertised": list(self.advertised),
            "filter_list": list(self.filter_list),
            "admitted": list(self.admitted),
            "connection_state": self.connection_state,
            "audiences": [
                {"audience": name, "tools": list(tools)}
                for name, tools in self.plan.audiences
            ],
        }


def resolve_chat_prompt_path(inputs: SyncInputs) -> Path | None:
    """Where the chat system prompt lives, from whichever place declares it.

    ``agent_surface.projections.chat.system_prompt_ref`` first: the
    projection owns both halves of what the chat client is told — the tool
    set and the prose that describes it — because the deployment check scans
    the one against the other. ``chat.preset.system_prompt_ref`` is the
    frozen legacy spelling (issue #2067), and declaring both is refused at
    manifest load, so at most one of these can answer.
    """
    from okg.substrate.mcp.effective_surface import parse_agent_surface

    surface = parse_agent_surface(inputs.raw_manifest)
    projection = surface.projection("chat") if surface is not None else None
    if projection is not None and projection.system_prompt_ref:
        return inputs.deployment_dir / projection.system_prompt_ref
    chat = inputs.chat
    preset = getattr(chat, "preset", None) if chat is not None else None
    if preset is not None and getattr(preset, "system_prompt_ref", None):
        return inputs.deployment_dir / preset.system_prompt_ref
    return None


def plan_tool_curation(
    chat: Any,
    *,
    deployment: str,
    posture: Posture,
    raw_manifest: Mapping[str, Any] | None = None,
) -> tuple[ToolCurationPlan, list[dict[str, Any]]]:
    """Resolve the declared set and the posture exclusion. Stage one of two.

    Returns the plan plus the exclusion rows for the report. Nothing here
    touches the wire — the endswith and empty-list traps are properties of the
    live listing and are handled in :func:`bind_tool_curation`.
    """
    from okg.substrate.mcp.effective_surface import parse_agent_surface

    surface = (
        parse_agent_surface(raw_manifest) if raw_manifest is not None else None
    )
    projection = surface.projection("chat") if surface is not None else None

    mcp = getattr(chat, "mcp", None)
    if projection is not None:
        # THE declaration (PACT effective-surface-declaration). It is checked
        # first because `chat.mcp.tools` is REFUSED beside it at manifest
        # load, so reaching this branch means the projection is the only
        # statement there is. Falling through to the `mcp` branch would read
        # the empty tuple that a refused-and-absent key leaves behind and
        # project NOTHING, which is the failure mode this ordering exists to
        # prevent.
        declared = tuple(projection.tools)
        source = DECLARATION_AGENT_SURFACE
    elif mcp is None:
        # NO declaration. The literal default applies.
        declared = tuple(CHAT_DEFAULT_TOOLS)
        source = DECLARATION_SUBSTRATE_DEFAULT
    else:
        # A declaration EXISTS. Whatever it says is what it says — including
        # an empty set, which means NONE and must never fall through to the
        # default. Keyed on the presence of the block rather than on the
        # emptiness of the list, because "declared nothing" and "declared
        # nothing ADMITTED" arrive as the same empty tuple and reading one as
        # the other is how an empty set turns into six tools.
        declared = tuple(getattr(mcp, "tools", ()) or ())
        source = DECLARATION_MANIFEST

    audiences = tuple(
        (a.name, tuple(a.tools))
        for a in (getattr(mcp, "audiences", ()) or ())
    )
    if audiences:
        distinct = {frozenset(tools) for _, tools in audiences}
        distinct.add(frozenset(declared))
        if len(distinct) > 1:
            raise ChatSyncError(
                f"deployment {deployment!r} declares different tool sets for "
                f"different audiences over ONE MCP connection: "
                + "; ".join(
                    f"{name}={sorted(tools)}" for name, tools in audiences
                )
                + f"; chat.mcp.tools={sorted(declared)}. Open WebUI's per-tool "
                "filter (function_name_filter_list) is a property of the "
                "CONNECTION, not of a preset or a group — utils/middleware.py:"
                "2237 reads it off the connection and :2243 applies it to every "
                "spec that connection serves — and a preset cannot narrow an "
                "MCP server (:2742 takes the whole remainder after 'server:mcp:' "
                "as a server id, :2757 registers every advertised spec). One "
                "registration therefore serves exactly ONE tool set. Refused "
                "with nothing applied, rather than projecting one audience's "
                "set and leaving the other silently wrong.",
                data={
                    "reason": "two_audiences_one_connection",
                    "surface": SURFACE_PRESET,
                    "audiences": {name: sorted(tools) for name, tools in audiences},
                    "declared": sorted(declared),
                    "distinct_sets": [sorted(s) for s in sorted(distinct, key=sorted)],
                },
                fix_hint=(
                    "register two MCP connections (or run two mcp-serve "
                    "processes), one per audience, each with its own "
                    "function_name_filter_list"
                ),
            )

    exclusions: list[dict[str, Any]] = []
    excluded: list[str] = []
    if posture.masked and QUERY_TOOL in declared:
        excluded.append(QUERY_TOOL)
        exclusions.append({
            "tool": QUERY_TOOL,
            "surface": SURFACE_PRESET,
            "reason": (
                f"excluded from the preset's bound tools because deployment "
                f"{deployment!r} resolves MASKED ({posture.reason})"
                + (" [posture indeterminate]" if posture.indeterminate else "")
                + ". Whether Nomos masking covers the bounded query "
                "operator's output is proven by the mcp-http-auth child, not "
                "here; until that proof lands the tool is excluded. This run "
                "does not claim masking coverage."
            ),
        })
    intended = tuple(t for t in declared if t not in set(excluded))
    return (
        ToolCurationPlan(
            declared=declared,
            declaration_source=source,
            excluded=tuple(excluded),
            intended=intended,
            audiences=audiences,
        ),
        exclusions,
    )


def bind_tool_curation(
    plan: ToolCurationPlan,
    *,
    advertised: Sequence[str],
    deployment: str,
) -> ToolCuration:
    """Bind the plan to a filter list and PROVE the vendor will read it right.

    TRAP 1 — the matcher is ENDSWITH, not equality (utils/misc.py:71 for the
    allow arm, :75 for the block arm). A filter entry ``query`` also admits
    ``subquery``, ``raw_query``, a future ``graph_query``; a block entry
    ``!query`` also blocks them. So a filter entry is NOT a tool name, and
    this function never assumes it is: it computes what the vendor's own
    predicate will admit against the ADVERTISED names and refuses on ANY
    difference from the intended set — extra names (a widening, which
    integration_acceptance forbids) and missing ones (a silent loss) alike.

    TRAP 2 — an EMPTY filter list means ALLOW ALL. Handled by never writing
    one; see :func:`deny_all_filter_list`.
    """
    names = tuple(advertised)
    if not plan.intended:
        filter_list = deny_all_filter_list(names)
        state = CONNECTION_SERVING_NOTHING
    else:
        filter_list = tool_filter_list(plan.declared, excluded=plan.excluded)
        state = CONNECTION_SERVING_DECLARED

    # Structural guard on TRAP 2 that does not depend on the listing: if this
    # ever composed to something the vendor reads as empty, the connection
    # would serve EVERY tool. Asserted here so the failure is named rather
    # than discovered as an over-wide admitted set below.
    if not [entry for entry in filter_list if entry.strip()]:
        raise ChatSyncError(
            f"deployment {deployment!r} composed an EMPTY connection tool "
            "filter. Open WebUI reads an empty filter list as ALLOW ALL "
            "(utils/misc.py:63-64), so this would have served every tool the "
            "MCP server advertises while the run reported a curated surface.",
            data={
                "reason": "empty_filter_list_means_allow_all",
                "surface": SURFACE_PRESET,
                "declared": sorted(plan.declared),
                "intended": sorted(plan.intended),
            },
            fix_hint="report this: deny_all_filter_list must never return empty",
        )

    admitted = effective_tool_binding(names, filter_list)
    extra = sorted(set(admitted) - set(plan.intended))
    missing = sorted(set(plan.intended) - set(admitted))
    if extra or missing:
        raise ChatSyncError(
            f"the connection tool filter for {deployment!r} would not carry "
            f"the declared set. Declared {sorted(plan.declared)}"
            + (
                f" minus posture exclusions {sorted(plan.excluded)}"
                if plan.excluded else ""
            )
            + f" = intended {sorted(plan.intended)}; the filter "
            f"{list(filter_list)} against the advertised names "
            f"{sorted(names)} would actually admit {list(admitted)}"
            + (f"; EXTRA {extra}" if extra else "")
            + (f"; MISSING {missing}" if missing else "")
            + ". Open WebUI matches filter entries with endswith "
            "(utils/misc.py:71, 75), so an entry never names exactly one "
            "tool; this run computes the effective set against the live "
            "listing rather than trusting the list it wrote. Shipping a "
            "filter that admits more than was declared is a widening.",
            data={
                "reason": "filter_admits_a_different_set",
                "surface": SURFACE_PRESET,
                "declared": sorted(plan.declared),
                "excluded": sorted(plan.excluded),
                "intended": sorted(plan.intended),
                "admitted": list(admitted),
                "extra": extra,
                "missing": missing,
                "filter_list": list(filter_list),
                "advertised": sorted(names),
            },
            fix_hint=(
                "rename the colliding tool, or narrow chat.mcp.tools so no "
                "declared entry is a suffix of another advertised name"
            ),
        )
    if state == CONNECTION_SERVING_NOTHING and admitted:
        raise ChatSyncError(  # pragma: no cover - unreachable while `extra` fires first
            f"the deny-everything filter for {deployment!r} still admits "
            f"{list(admitted)}.",
            data={"reason": "deny_all_leaked", "admitted": list(admitted)},
            fix_hint="report this defect",
        )
    return ToolCuration(
        plan=plan,
        advertised=names,
        filter_list=filter_list,
        admitted=admitted,
        connection_state=state,
    )


# ---------------------------------------------------------------------------
# The WRITE connection: `record` and `propose`, bound to the admin group only
# (PACT chat-record-affordance, req.write-tools-bound /
# req.authenticated-transport-only)
# ---------------------------------------------------------------------------
#
# A preset CANNOT bind a subset of an MCP server's tools -- `middleware.py:2742`
# takes the whole remainder after `server:mcp:` as a server id and `:2757`
# registers every advertised spec -- so "the admin preset gets two more tools"
# is not expressible as a preset edit. The only per-tool lever is the
# per-CONNECTION `function_name_filter_list`, and one connection serves exactly
# one filter. The write surface is therefore a SECOND connection with its own
# filter and its own preset, granted to the admin group and to nobody else.
#
# What the run does NOT do is put `record` or `propose` anywhere near the
# general read connection. The two connections point at the same endpoint and
# differ only in what they admit.

#: Appended to the read connection's ids. The write connection is a distinct
#: registration, so it needs a distinct `info.id` -- two entries sharing one id
#: would have the reconciler overwrite one with the other on every run.
WRITE_CONNECTION_SUFFIX = "-write"

#: The child that supplies the authenticated transport this connection needs.
#: Named in the refusal because "refused" without the producer leaves an
#: operator with nothing to do about it.
MCP_HTTP_AUTH_PRODUCER = "mcp-http-auth"

#: The deployment named no admin group, so there is no audience to authorize.
WRITE_UNAUTHORIZED_NO_ADMIN_GROUP = "no_admin_group_declared"
#: The deployment named an admin group but its endpoint serves no write tool,
#: i.e. `mcp-serve` was launched read-only. NOT a defect: it is how a
#: deployment says it does not want chat writes.
WRITE_UNAUTHORIZED_READ_ONLY_ENDPOINT = "endpoint_advertises_no_write_tool"
#: Both halves present.
WRITE_AUTHORIZED = "admin_group_and_write_capable_endpoint"


@dataclass(frozen=True)
class WriteAuthorization:
    """Whether this deployment authorizes a chat write surface, and why.

    Two halves, both required, neither inferred from the other:

    * WHO -- ``chat.sso.admin_group``. design.md section 5 mechanism 4 says
      admins are distinguished by GROUP, not by URL, and this is the only
      group the chat block names for that purpose. Absent means nobody is
      authorized, so no write connection is projected anywhere.
    * WHAT -- the endpoint ADVERTISES a write-effect tool, observed in the
      same ``tools/list`` readback the read connection is bound against. That
      advertisement exists only because an operator launched ``mcp-serve``
      with ``--allow-writes`` (``tool_registration.py:179-181``), so it is a
      deliberate act rather than a default.

    Recording the reason on the negative arm is the point: "no write tools
    appear anywhere" has to be attributable to a decision, not to a projection
    that silently did nothing.
    """

    authorized: bool
    reason: str
    admin_group: str | None
    advertised_write_tools: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "authorized": self.authorized,
            "reason": self.reason,
            "admin_group": self.admin_group,
            "advertised_write_tools": list(self.advertised_write_tools),
        }


@dataclass(frozen=True)
class WriteConnection:
    """The second connection, bound and proven against the live listing."""

    server_id: str
    preset_id: str
    admin_group: str
    endpoint: str
    #: The pair this connection exists to carry.
    intended: tuple[str, ...]
    filter_list: tuple[str, ...]
    #: What the vendor's own matcher admits from ``filter_list`` against the
    #: advertised names. Never assumed equal to ``intended``; checked.
    admitted: tuple[str, ...]
    auth_posture: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "preset_id": self.preset_id,
            "admin_group": self.admin_group,
            "endpoint": self.endpoint,
            "intended": list(self.intended),
            "filter_list": list(self.filter_list),
            "admitted": list(self.admitted),
            "auth_posture": self.auth_posture,
        }


def _write_effect_tools() -> tuple[str, ...]:
    from okg.substrate.mcp.operators import WRITE_EFFECT_TOOLS

    return tuple(WRITE_EFFECT_TOOLS)


def write_authorization(
    chat: Any,
    *,
    advertised: Sequence[str],
    write_tools: Sequence[str] | None = None,
) -> WriteAuthorization:
    """Resolve :class:`WriteAuthorization` for one deployment. Nothing written."""
    tools = tuple(write_tools) if write_tools is not None else _write_effect_tools()
    live = set(advertised)
    advertised_writes = tuple(name for name in tools if name in live)

    sso = getattr(chat, "sso", None)
    admin_group = (getattr(sso, "admin_group", None) or "").strip() or None
    if admin_group is None:
        return WriteAuthorization(
            authorized=False,
            reason=WRITE_UNAUTHORIZED_NO_ADMIN_GROUP,
            admin_group=None,
            advertised_write_tools=advertised_writes,
        )
    if not advertised_writes:
        return WriteAuthorization(
            authorized=False,
            reason=WRITE_UNAUTHORIZED_READ_ONLY_ENDPOINT,
            admin_group=admin_group,
            advertised_write_tools=(),
        )
    return WriteAuthorization(
        authorized=True,
        reason=WRITE_AUTHORIZED,
        admin_group=admin_group,
        advertised_write_tools=advertised_writes,
    )


def bind_write_connection(
    *,
    deployment: str,
    admin_group: str,
    advertised: Sequence[str],
    auth_posture: str,
    endpoint: str,
    mcp_server_id: str,
    preset_id: str,
    write_tools: Sequence[str] | None = None,
) -> WriteConnection:
    """Bind the write pair to a filter list, or refuse. Three refusals.

    1. TRANSPORT. ``auth_posture`` must be the OBSERVED
       :data:`~okg.substrate.chat.source_admin.AUTH_REQUIRED` -- an anonymous
       ``tools/list`` was refused and a credentialed one succeeded. A
       configuration that merely NAMES a credential proves the operator holds
       one, not that the server demands one, so it is not accepted here; the
       caller passes what it measured. Until ``mcp-http-auth`` makes the
       endpoint enforce, this refuses by name rather than registering a
       connection whose tools would fail at call time -- a tool that is present
       and always fails is indistinguishable to a user from a broken graph.
    2. EMPTY. An empty intended set REFUSES and emits no filter list at all.
       Open WebUI reads an empty ``function_name_filter_list`` as ALLOW ALL
       (``utils/misc.py:63-64``), so the "obvious" encoding of "no write tools"
       would hand the write preset every tool the server advertises --
       including the nine source-admin ones. The guard is structural, not
       hypothetical: this function is where a future exclusion, policy filter
       or edit to ``WRITE_EFFECT_TOOLS`` would arrive, and it must not be
       reachable through it.
    3. COLLISION. Filter matching is ENDSWITH (``utils/misc.py:71``), so the
       entry ``record`` also admits ``prerecord`` and any future name ending in
       it. What the vendor's own predicate admits is computed against the
       ADVERTISED names and compared to the intended pair; ANY difference --
       extra (a widening) or missing (a silent loss) -- refuses.
    """
    from okg.substrate.chat.source_admin import AUTH_REQUIRED

    tools = tuple(write_tools) if write_tools is not None else _write_effect_tools()
    names = tuple(advertised)
    server_id = f"{mcp_server_id}{WRITE_CONNECTION_SUFFIX}"

    if auth_posture != AUTH_REQUIRED:
        raise ChatSyncError(
            f"deployment {deployment!r} authorizes chat writes, but the MCP "
            f"endpoint {endpoint!r} was not OBSERVED to require "
            f"authentication (posture {auth_posture!r}). The write connection "
            f"is produced by {MCP_HTTP_AUTH_PRODUCER}, which supplies the "
            "bearer credential at the transport boundary; until it is active "
            "on this endpoint no write connection is registered. It is not "
            "registered unauthenticated, not registered on loopback and "
            "described as final, and not registered to fail at call time -- a "
            "tool that is present and always fails is indistinguishable from "
            "a broken graph. The posture is what an anonymous read MEASURED, "
            "never what the configuration claims.",
            data={
                "reason": "write_transport_unauthenticated",
                "surface": SURFACE_PRESET,
                "deployment": deployment,
                "endpoint": endpoint,
                "auth_posture": auth_posture,
                "required_posture": AUTH_REQUIRED,
                "missing_producer": MCP_HTTP_AUTH_PRODUCER,
                "write_tools": list(tools),
            },
            fix_hint=(
                f"land {MCP_HTTP_AUTH_PRODUCER} and serve `okg mcp-serve "
                "--auth-token-env NAME` so an anonymous tools/list is refused"
            ),
        )

    intended = tuple(name for name in tools if name in set(names))
    if not intended:
        raise ChatSyncError(
            f"deployment {deployment!r} authorizes chat writes but the "
            f"computed write tool set is EMPTY (write tools {list(tools)}; "
            f"advertised {sorted(names)}). No filter list is emitted: Open "
            "WebUI reads an empty function_name_filter_list as ALLOW ALL "
            "(utils/misc.py:63-64), so writing one would serve the write "
            "preset every tool this server advertises. Refused with nothing "
            "registered.",
            data={
                "reason": "empty_write_tool_set",
                "surface": SURFACE_PRESET,
                "deployment": deployment,
                "write_tools": list(tools),
                "advertised": sorted(names),
                "filter_list_emitted": False,
            },
            fix_hint=(
                "serve `okg mcp-serve --allow-writes` so record and propose "
                "are advertised, or remove the deployment's write authorization"
            ),
        )

    filter_list = tuple(intended)
    admitted = effective_tool_binding(names, filter_list)
    extra = sorted(set(admitted) - set(intended))
    missing = sorted(set(intended) - set(admitted))
    if extra or missing:
        raise ChatSyncError(
            f"the write connection filter for {deployment!r} would not carry "
            f"exactly {sorted(intended)}: the filter {list(filter_list)} "
            f"against the advertised names {sorted(names)} would admit "
            f"{list(admitted)}"
            + (f"; EXTRA {extra}" if extra else "")
            + (f"; MISSING {missing}" if missing else "")
            + ". Open WebUI matches filter entries with endswith "
            "(utils/misc.py:71), so a filter entry never names exactly one "
            "tool; a write connection that admits more than the write pair is "
            "a widening of the write surface.",
            data={
                "reason": "write_filter_admits_a_different_set",
                "surface": SURFACE_PRESET,
                "deployment": deployment,
                "intended": sorted(intended),
                "admitted": list(admitted),
                "extra": extra,
                "missing": missing,
                "filter_list": list(filter_list),
                "advertised": sorted(names),
            },
            fix_hint=(
                "rename the colliding tool so no advertised name ends with a "
                "write tool name other than its own"
            ),
        )
    return WriteConnection(
        server_id=server_id,
        preset_id=f"{preset_id}{WRITE_CONNECTION_SUFFIX}",
        admin_group=admin_group,
        endpoint=endpoint,
        intended=intended,
        filter_list=filter_list,
        admitted=admitted,
        auth_posture=auth_posture,
    )


def plan_write_connection(
    chat: Any,
    *,
    deployment: str,
    advertised: Sequence[str],
    endpoint: str,
    mcp_server_id: str,
    preset_id: str,
    observe_auth: Callable[[str, str | None], str],
) -> tuple[WriteConnection | None, WriteAuthorization]:
    """Resolve the write connection for one run, or say why there is none.

    ``observe_auth`` is called ONLY once authorization holds: it dials the
    endpoint twice (anonymous, then credentialed), and a deployment that
    authorizes no writes should not pay for a measurement whose answer cannot
    change the outcome.
    """
    authorization = write_authorization(chat, advertised=advertised)
    if not authorization.authorized:
        return None, authorization
    auth_token_env = getattr(getattr(chat, "mcp", None), "auth_token_env", None)
    posture = observe_auth(endpoint, auth_token_env)
    connection = bind_write_connection(
        deployment=deployment,
        admin_group=str(authorization.admin_group),
        advertised=advertised,
        auth_posture=posture,
        endpoint=endpoint,
        mcp_server_id=mcp_server_id,
        preset_id=preset_id,
    )
    return connection, authorization


# ---------------------------------------------------------------------------
# The source-admin preset (PACT chat-source-admin-writer)
# ---------------------------------------------------------------------------
#
# `chat.source_admin.project_admin_preset` is the single authoring source of
# the admin preset; this section only decides whether this run writes it, and
# carries the projection to the two places that write it: its CONNECTION (in
# the preset surface, the one writer of `/api/v1/configs/tool_servers`) and its
# model ROW (the `admin_preset` surface).
#
# The switch is `chat.sso.admin_group`, the same one the write preset uses. A
# deployment without it gets no admin surface at all. A deployment WITH it
# whose admin preset cannot be projected safely gets a refusal on that one
# surface, by name, and nothing admin-bearing written; the rest of the run is
# unaffected, because the admin preset is additive and refusing it removes no
# capability the deployment had before.

#: The surface's status when the projection refused. Each names its cause.
ADMIN_PRESET_REFUSED_UNSAFE_TRANSPORT = "refused_unsafe_transport"
ADMIN_PRESET_REFUSED_UNRESOLVED_GROUP = "refused_unresolved_group"
ADMIN_PRESET_REFUSED_READ_ONLY_ENDPOINT = "refused_read_only_endpoint"
ADMIN_PRESET_REFUSED_FILTER_COLLISION = "refused_filter_collision"
ADMIN_PRESET_REFUSED_PROJECTION = "refused_projection"

#: The gating claim a written admin preset carries. Never "enforced": the
#: per-caller arm is blocked on mcp-principal-mapping, and a group grant is
#: visibility, which Open WebUI bypasses for every instance admin by role.
ADMIN_PRESET_GATING_CLAIM = "specified"


@dataclass(frozen=True)
class AdminPresetPlan:
    """What this run does about the admin preset, decided before any write.

    ``projection`` is set only when ``status`` is ``planned``; every refusal
    carries its reason and the refusal text, and writes nothing.
    """

    status: str
    admin_group: str
    preset_id: str
    server_id: str
    reason: str | None = None
    refusal: str | None = None
    projection: Any = None
    filter_list: tuple[str, ...] = ()
    group_ids: Mapping[str, str] = field(default_factory=dict)
    #: On a refused plan: whether an admin preset row from an earlier run is
    #: on the instance. A refusal removes the admin CONNECTION (the preset
    #: surface owns that list) but never deletes a model row, so a row left
    #: behind is reported rather than left to be discovered.
    existing_row: bool | None = None

    @property
    def projected(self) -> bool:
        return self.projection is not None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "declared": True,
            "status": self.status,
            "admin_group": self.admin_group,
            "preset_id": self.preset_id,
            "server_id": self.server_id,
            # Carried on EVERY arm, including the refusals: a refused run must
            # not read as one where the claim was ever stronger.
            "gating_claim": ADMIN_PRESET_GATING_CLAIM,
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.refusal:
            payload["refusal"] = self.refusal
        if self.existing_row is not None:
            payload["existing_row"] = self.existing_row
            if self.existing_row:
                payload["fix_hint"] = (
                    f"an admin preset row {self.preset_id!r} from an earlier "
                    "run is still on the instance; this run removed its "
                    f"connection {self.server_id!r}, so the row binds no "
                    "tools. Fix the refusal and re-run sync, or delete the "
                    "model in the Open WebUI workspace model list."
                )
        if self.projection is not None:
            payload["projection"] = self.projection.to_dict()
            payload["filter_list"] = list(self.filter_list)
        return payload


def _stale_admin_state(
    entries: Sequence[Mapping[str, Any]],
    rows: Iterable[str],
    *,
    server_id: str,
    preset_id: str,
) -> dict[str, Any]:
    """Admin-id leftovers on an instance whose deployment declares no switch.

    READ ONLY. Sync does not revoke what it no longer manages: removing
    ``chat.sso.admin_group`` leaves an earlier run's admin connection and row
    in place, and an entry carrying the id may not be ours at all (an
    operator's, or another deployment's whose id collides, e.g. deployments
    ``foo`` and ``foo-admin``). So both are NAMED here, with how to remove
    them, and nothing is written.

    Computed from the readback the preset surface already makes, so a
    deployment without the switch issues no request it did not issue before.
    """
    connection = any(
        (entry.get("info") or {}).get("id") == server_id for entry in entries
    )
    row = preset_id in set(rows)
    stale: dict[str, Any] = {
        "stale_connection": server_id if connection else None,
        "stale_row": preset_id if row else None,
    }
    if connection or row:
        named = [n for n in (stale["stale_connection"], stale["stale_row"]) if n]
        stale["fix_hint"] = (
            f"{named} carry this deployment's admin preset ids, but "
            "chat.sso.admin_group is not declared, so sync no longer manages "
            "them and has left them untouched. If they are leftovers of an "
            "earlier admin preset, remove the tool server in the Open WebUI "
            "admin settings and delete the model in the workspace model "
            "list; if another deployment or an operator owns the id, rename "
            "one side. Re-declaring chat.sso.admin_group puts them back under "
            "sync."
        )
    return stale


def admin_group_of(chat: Any) -> str | None:
    """``chat.sso.admin_group``, stripped, or None. The one switch."""
    sso = getattr(chat, "sso", None)
    return (getattr(sso, "admin_group", None) or "").strip() or None


def plan_admin_preset(
    chat: Any,
    client: "InstanceClient",
    *,
    deployment: str,
    advertised: Sequence[str],
    probe_endpoint: str,
    instance_endpoint: str,
    mcp_server_id: str,
    preset_id: str,
    observe_auth: Callable[[str, str | None], str],
    observed_posture: str | None = None,
) -> AdminPresetPlan | None:
    """Project the admin preset for one run, or say why this run will not.

    Returns None when the deployment declares no ``chat.sso.admin_group``:
    no dial, no read, nothing reported beyond the negative arm.

    The transport decision uses the host of the INSTANCE's URL for the
    endpoint, not the operator's probe URL. The instance is what dials the
    admin tools, so "loopback-local to the instance" is a property of that
    URL; a container reaching ``host.docker.internal`` is not on loopback, and
    reading the probe URL's ``127.0.0.1`` instead would grant the loopback
    interim to a connection that crosses a network boundary.
    """
    from okg.substrate.chat.source_admin import (
        ChatSourceAdminError,
        LiveToolSurface,
        _host_of,
        project_admin_preset,
    )

    admin_group = admin_group_of(chat)
    if admin_group is None:
        return None
    admin_preset_id = f"{preset_id}{ADMIN_PRESET_SUFFIX}"
    server_id = f"{mcp_server_id}{ADMIN_PRESET_SUFFIX}"

    def _refused(status: str, err: Exception, reason: str | None = None) -> AdminPresetPlan:
        data = getattr(err, "data", None) or {}
        return AdminPresetPlan(
            status=status,
            admin_group=admin_group,
            preset_id=admin_preset_id,
            server_id=server_id,
            reason=reason or str(data.get("reason") or status),
            refusal=str(err),
        )

    try:
        group_ids = _resolve_group_ids(client, [admin_group])
    except ChatSyncError as err:
        if err.data.get("reason") != "unknown_access_group":
            raise
        return _refused(
            ADMIN_PRESET_REFUSED_UNRESOLVED_GROUP, err, "unknown_access_group",
        )

    auth_token_env = getattr(getattr(chat, "mcp", None), "auth_token_env", None)
    posture = (
        observed_posture
        if observed_posture is not None
        else observe_auth(probe_endpoint, auth_token_env)
    )
    live = LiveToolSurface(
        endpoint=probe_endpoint,
        tool_names=tuple(advertised),
        auth_posture=posture,
    )
    try:
        projection = project_admin_preset(
            deployment=deployment,
            preset_id=admin_preset_id,
            live=live,
            access_group_names=[admin_group],
            auth_token_env=auth_token_env,
            host=_host_of(instance_endpoint),
            access_group_ids=group_ids,
            mcp_server_id=server_id,
        )
    except ChatSourceAdminError as err:
        data = err.data or {}
        if data.get("unmet_precondition") == "authenticated_or_loopback_transport":
            return _refused(
                ADMIN_PRESET_REFUSED_UNSAFE_TRANSPORT,
                err,
                "authenticated_or_loopback_transport",
            )
        if data.get("reason") == "read_only_endpoint":
            return _refused(ADMIN_PRESET_REFUSED_READ_ONLY_ENDPOINT, err)
        return _refused(ADMIN_PRESET_REFUSED_PROJECTION, err)

    # The same check the write connection makes: a filter entry matches by
    # ENDSWITH, so what the vendor's predicate admits from the bound names is
    # computed against the advertised names and must equal them exactly.
    filter_list = tuple(projection.bound_tools)
    admitted = effective_tool_binding(advertised, filter_list)
    if set(admitted) != set(projection.bound_tools):
        err = ChatSyncError(
            f"the admin connection filter {list(filter_list)} would admit "
            f"{list(admitted)} against the advertised names, not exactly "
            f"{list(projection.bound_tools)}. Open WebUI matches filter "
            "entries with endswith (utils/misc.py:71); an admin connection "
            "admitting a different set is refused rather than registered.",
            data={"reason": "admin_filter_admits_a_different_set"},
        )
        return _refused(ADMIN_PRESET_REFUSED_FILTER_COLLISION, err)

    # `_grant_set` is what the row is written with; the projection's own
    # grant set must be the same value or the readback could never converge.
    projected = projection.to_dict()
    if projected.get("access_grants") != _grant_set(group_ids):
        raise ChatSyncError(
            "the admin preset projection's access_grants disagree with the "
            "grant set sync writes; refusing rather than writing one and "
            "asserting the other.",
            data={
                "reason": "admin_grant_shape_mismatch",
                "surface": SURFACE_ADMIN_PRESET,
                "projected": projected.get("access_grants"),
                "written": _grant_set(group_ids),
            },
        )
    return AdminPresetPlan(
        status="planned",
        admin_group=admin_group,
        preset_id=admin_preset_id,
        server_id=server_id,
        projection=projection,
        filter_list=filter_list,
        group_ids=dict(group_ids),
    )


# ---------------------------------------------------------------------------
# F1/F9: the composed system prompt
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PromptSource:
    name: str
    path: str | None
    present: bool
    chars: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "present": self.present,
            "chars": self.chars,
        }


@dataclass(frozen=True)
class ComposedPrompt:
    """The preset's system prompt, plus which sources produced it."""

    text: str
    sources: tuple[PromptSource, ...]

    @property
    def contributing(self) -> tuple[str, ...]:
        return tuple(s.name for s in self.sources if s.present)

    @property
    def absent(self) -> tuple[str, ...]:
        return tuple(s.name for s in self.sources if not s.present)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chars": len(self.text),
            "contributing_sources": list(self.contributing),
            "absent_sources": list(self.absent),
            "sources": [s.to_dict() for s in self.sources],
        }


def compose_system_prompt(
    deployment: str,
    *,
    mcp_instructions_path: Path | None,
    system_prompt_path: Path | None,
) -> ComposedPrompt:
    """Compose the preset's system prompt. One authoring source, two consumers.

    F1 is why this exists at all: Open WebUI never shows the model the MCP
    server's ``instructions``, so the deployment's ``mcp_instructions``
    document reaches a chat user ONLY through the preset's ``params.system``.
    Restating the text in this verb would create a second authoring source
    that silently disagrees with the one the MCP server serves; the text is
    read from the deployment's own files instead, and this module contains no
    prompt text of its own.

    F9 is why the absent case is first-class: a deployment may legitimately
    declare neither key. What it may NOT do is end up with an empty system
    prompt that looks configured, so both-absent REFUSES.
    """
    sources: list[PromptSource] = []
    parts: list[str] = []
    for name, path in (
        ("mcp_instructions", mcp_instructions_path),
        ("system_prompt_file", system_prompt_path),
    ):
        if path is None:
            sources.append(PromptSource(name, None, False, 0))
            continue
        try:
            text = Path(path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            # A DECLARED but unreadable source is an error, not an absence:
            # the operator said the file exists.
            raise ChatSyncError(
                f"deployment {deployment!r} declares a {name} at {path} that "
                f"cannot be read ({exc}). A declared prompt source that is "
                "missing is a broken declaration, not an empty contribution.",
                data={"reason": f"{name}_unreadable", "path": str(path)},
                fix_hint=f"fix or remove the {name} declaration",
            ) from exc
        if not text:
            raise ChatSyncError(
                f"deployment {deployment!r} declares a {name} at {path} that "
                "is empty. An empty declared source would compose into a "
                "system prompt that looks configured and says nothing.",
                data={"reason": f"{name}_empty", "path": str(path)},
                fix_hint=f"write the {name} content, or remove the declaration",
            )
        sources.append(PromptSource(name, str(path), True, len(text)))
        parts.append(text)

    if not parts:
        raise ChatSyncError(
            f"deployment {deployment!r} declares neither mcp_instructions nor "
            "chat.preset.system_prompt_ref, so there is no operator "
            "instruction to compose. Open WebUI never surfaces the MCP "
            "server's own `instructions` to the model, so an empty system "
            "prompt would leave the model unable to find its tools while the "
            "preset looked configured. Refused.",
            data={
                "reason": "no_prompt_source",
                "sources": [s.to_dict() for s in sources],
            },
            fix_hint=(
                "declare mcp_instructions in deployment.yaml, or "
                "chat.preset.system_prompt_ref pointing at a prompt file"
            ),
        )
    return ComposedPrompt(text="\n\n".join(parts), sources=tuple(sources))


# ---------------------------------------------------------------------------
# Standing guidance: the remaining declared agent-surface keys
# (PACT chat-tool-surface, req.standing-guidance)
# ---------------------------------------------------------------------------
#
# design.md section 5 row 7 — "Skills, mcp_instructions, agent_tools,
# bootstrap packets" — is the program's only statement of the agent
# workflow-and-context surface, and this is where it is discharged for the
# chat plane. `mcp_instructions` is already carried by
# :func:`compose_system_prompt`. The other three are decided below, each
# either CARRIED with its rendering or NOT CARRIED with its reason recorded.
#
# The channel is forced. Open WebUI's MCP client reads a server's tool list
# and nothing else: `utils/mcp/client.py:88-107` keeps name, description and
# inputSchema per tool and drops the initialize result's `instructions`
# entirely. It does define `list_resources` / `read_resource` (:125, :138),
# but the chat path never calls either — so the substrate's own
# `okg://readside-metadata/<deployment>` resource, which already carries
# skills and skill_triggers to coding agents, is invisible to a chat
# assistant. The preset's `params.system` is the only door.

#: Cap on the whole rendered guidance block. The model pays for this text on
#: every turn; "without unbounded prompt growth" is a budget, not a wish.
GUIDANCE_TOTAL_BUDGET = 12000

#: Cap on how many deployment-declared agent tools are rendered. Bounded so a
#: manifest with hundreds cannot silently grow every turn's prompt.
GUIDANCE_AGENT_TOOL_LIMIT = 40

GUIDANCE_KEYS: tuple[str, ...] = (
    "tool_description_overrides",
    "agent_tools",
    "skills_dir",
    "skill_triggers",
)

#: The manifest keys whose PATHS are read off a resolved deployment. Named
#: here so :func:`load_inputs` can say exactly which facts an unresolvable
#: manifest leaves UNDETERMINED, rather than reporting all four as absent.
RESOLVED_AGENT_SURFACE_KEYS: tuple[str, ...] = (
    "mcp_instructions",
    "tool_description_overrides",
    "skills_dir",
    "skill_triggers",
)


@dataclass(frozen=True)
class GuidanceKey:
    """One declared agent-surface key, and what this verb did with it.

    ``carried`` and ``declared`` are separate booleans on purpose. "The
    deployment declared nothing" and "the deployment declared it and this verb
    deliberately does not carry it" are different facts, and a single flag
    would render them the same.
    """

    name: str
    declared: bool
    carried: bool
    reason: str
    rendered_chars: int = 0
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.name,
            "declared": self.declared,
            "carried": self.carried,
            "reason": self.reason,
            "rendered_chars": self.rendered_chars,
        }
        if self.detail:
            payload["detail"] = self.detail
        return payload


@dataclass(frozen=True)
class StandingGuidance:
    """The rendered guidance block plus a decision row for every key."""

    text: str
    keys: tuple[GuidanceKey, ...]
    tool_descriptions: tuple[Any, ...]

    @property
    def carried(self) -> tuple[str, ...]:
        return tuple(k.name for k in self.keys if k.carried)

    @property
    def declared_not_carried(self) -> tuple[str, ...]:
        return tuple(k.name for k in self.keys if k.declared and not k.carried)

    @property
    def absent(self) -> tuple[str, ...]:
        return tuple(k.name for k in self.keys if not k.declared)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chars": len(self.text),
            # Every key is named on every run under exactly one of the three
            # headings, so a shrinking carried set is legible rather than an
            # empty prompt that looks configured.
            "keys": list(GUIDANCE_KEYS),
            "carried": list(self.carried),
            "declared_not_carried": list(self.declared_not_carried),
            "absent": list(self.absent),
            "decisions": [k.to_dict() for k in self.keys],
            "tool_descriptions": [d.to_dict() for d in self.tool_descriptions],
        }


#: Why ``skills_dir`` is NOT carried into a chat assistant's standing
#: guidance. Recorded as a constant so the reason is one string, reported and
#: asserted, rather than prose that can drift from what the code does.
SKILLS_DIR_NOT_CARRIED = (
    "not carried: a skills bundle is an unbounded set of coding-agent "
    "procedure documents that assume a filesystem, a shell and write tools. A "
    "chat assistant has only the MCP read algebra, so the procedures are not "
    "executable for it, and inlining the bundle would grow every turn's system "
    "prompt without bound. It already reaches coding agents through the "
    "okg://readside-metadata/<deployment> MCP resource; Open WebUI's chat path "
    "never lists or reads MCP resources, which is why the question arises at "
    "all and why the answer is not 'inline it instead'."
)

SKILL_TRIGGERS_NOT_CARRIED = (
    "not carried: skill_triggers is an intent->skill index whose entries name "
    "skill documents that are not carried (see skills_dir). Carrying the "
    "trigger vocabulary alone would name procedures the assistant cannot read, "
    "which is worse than silence: the model would cite a skill it has no "
    "access to."
)

AGENT_TOOLS_CARRIED = (
    "carried: operator_macro entries name a canonical AKMON read operator, so "
    "a chat assistant holding the read algebra can act on them. Rendered as "
    "one bounded line per macro."
)


def _render_guidance_agent_tools(
    agent_tools: Mapping[str, Any],
    *,
    deployment: str,
    projected_tools: Sequence[str],
) -> tuple[str, dict[str, Any]]:
    """Render the operator-macro entries, and name what was left out and why.

    ``projected_tools`` is the set the connection will actually serve, and
    every rendered macro's ``operator`` is checked against it. The rendered
    line is an INSTRUCTION to the model — "use the `X` operator" — so a macro
    naming an operator this deployment does not bind tells the model to call
    a tool it does not have. The model cannot detect that; it can only fail
    the turn, or invent. Refused here, with the macro and the projected set
    named.
    """
    projected = set(projected_tools)
    carried_rows: list[str] = []
    carried_names: list[str] = []
    skipped: list[dict[str, str]] = []
    for name in sorted(agent_tools):
        body = agent_tools[name]
        if not isinstance(body, Mapping):
            skipped.append({"tool": str(name), "reason": "not a mapping"})
            continue
        if body.get("kind") != "operator_macro":
            skipped.append({
                "tool": str(name),
                "reason": (
                    f"kind={body.get('kind')!r} is not operator_macro; it "
                    "needs a host, a credential or a write path the chat "
                    "plane does not have"
                ),
            })
            continue
        if body.get("requires_env") or body.get("boundary") == "external_live":
            skipped.append({
                "tool": str(name),
                "reason": (
                    "external_live / requires_env: it reads outside the "
                    "pinned generation, so a chat answer citing it would not "
                    "be generation-pinned"
                ),
            })
            continue
        # Validated BEFORE the rendering limit, not after: an entry that
        # happens to fall past the limit today would otherwise carry an
        # unchecked operator name until the limit moved.
        operator = body.get("operator")
        if not isinstance(operator, str) or operator not in projected:
            raise ChatSyncError(
                f"deployment {deployment!r} declares the agent tool "
                f"{str(name)!r} as an operator_macro over "
                f"{operator!r}, which is not one of the operators this chat "
                f"surface projects ({sorted(projected)}). The macro renders "
                "into the preset's system prompt as an instruction to use "
                "that operator, and the preset's system prompt is the ONLY "
                "guidance channel Open WebUI leaves open, so the model would "
                "be told to call a tool its connection does not serve. It "
                "cannot check that; it can only fail the turn or invent an "
                "answer. Refused rather than rendered.",
                data={
                    "reason": "macro_names_unprojected_operator",
                    "surface": SURFACE_PRESET,
                    "deployment": deployment,
                    "agent_tool": str(name),
                    "operator": operator if isinstance(operator, str) else repr(operator),
                    "projected": sorted(projected),
                },
                fix_hint=(
                    f"point agent_tools.{name}.operator at one of "
                    f"{sorted(projected)}, or add that operator to "
                    "chat.mcp.tools — noting that a posture exclusion can "
                    "remove one after it is declared"
                ),
            )
        if len(carried_rows) >= GUIDANCE_AGENT_TOOL_LIMIT:
            skipped.append({
                "tool": str(name),
                "reason": (
                    f"over the {GUIDANCE_AGENT_TOOL_LIMIT}-entry rendering "
                    "limit"
                ),
            })
            continue
        summary = body.get("description") or body.get("summary") or ""
        line = f"- `{name}` — use the `{operator}` operator"
        if summary:
            line += f": {summary}"
        carried_rows.append(line)
        carried_names.append(str(name))
    text = ""
    if carried_rows:
        text = (
            "## Named reads this deployment defines\n\n"
            + "\n".join(carried_rows)
        )
    return text, {"carried": carried_names, "not_carried": skipped}


def _render_guidance_descriptions(tool_descriptions: Sequence[Any]) -> str:
    """Render only the OVERRIDDEN descriptions.

    A tool with no override is deliberately not restated here: the model
    already receives the substrate description through the MCP tool spec
    (``utils/mcp/client.py:105``), so repeating it would spend prompt budget
    to say the same thing twice. The report still states the substrate
    default per tool, which is where "an absent override is a stated default"
    is discharged.
    """
    from okg.substrate.mcp.operators import SOURCE_DEPLOYMENT_OVERRIDE

    rows = [d for d in tool_descriptions if d.source == SOURCE_DEPLOYMENT_OVERRIDE]
    if not rows:
        return ""
    return (
        "## What these tools mean in this deployment\n\n"
        + "\n\n".join(f"### `{d.name}`\n{d.text}" for d in rows)
    )


def compose_standing_guidance(
    deployment: str,
    *,
    projected_tools: Sequence[str],
    tool_description_overrides_path: Path | None,
    skills_dir: Path | None,
    skill_triggers_path: Path | None,
    agent_tools: Mapping[str, Any] | None,
) -> StandingGuidance:
    """Decide and render the remaining declared agent-surface keys.

    Every key in :data:`GUIDANCE_KEYS` gets a decision row whether or not the
    deployment declares it, so "this deployment declares no skills" is a
    stated fact. A key that is declared and deliberately not carried records
    WHY. Nothing is rendered for an absent key — no placeholder, because a
    placeholder implying guidance exists is the failure mode this requirement
    names.
    """
    from okg.substrate.mcp.operators import (
        compose_tool_descriptions,
        load_tool_description_overrides,
    )

    overrides = load_tool_description_overrides(tool_description_overrides_path)
    descriptions = compose_tool_descriptions(projected_tools, overrides)

    keys: list[GuidanceKey] = []
    sections: list[str] = []

    description_text = _render_guidance_descriptions(descriptions)
    if description_text:
        sections.append(description_text)
    keys.append(GuidanceKey(
        name="tool_description_overrides",
        declared=tool_description_overrides_path is not None,
        carried=bool(overrides),
        reason=(
            f"carried: {len(overrides)} declared override(s) projected into "
            "the preset's system prompt, which is the only channel Open WebUI "
            "leaves open"
            if overrides else
            # DECLARED-BUT-EMPTY is not UNDECLARED. `load_tool_description_
            # overrides` returns {} without raising for a file that parses to
            # nothing (operators.py:543-544), so both cases arrive here as a
            # falsy mapping. Reporting the declared one as "declares no
            # tool_description_overrides" would be the exact conflation this
            # key's two booleans exist to prevent (see GuidanceKey): an
            # operator who wrote a file and mistyped its contents would read a
            # report telling them they never wrote one, and would go looking
            # for a missing declaration instead of an empty file.
            (
                "declared, but the file is EMPTY: it parses to zero "
                "overrides, so every projected tool keeps its substrate "
                "description. This is NOT the same as declaring none -- the "
                "path is declared and readable and its contents are empty, "
                "which is usually a mistake in the file rather than a "
                "decision. The path is named in detail."
                if tool_description_overrides_path is not None else
                "the deployment declares no tool_description_overrides; every "
                "projected tool keeps its substrate description, named per "
                "tool in this report"
            )
        ),
        rendered_chars=len(description_text),
        detail={
            "overridden": sorted(overrides),
            "substrate_default": [
                d.name for d in descriptions if d.name not in overrides
            ],
            # Named whenever a path was declared, so the empty-file case above
            # points the operator at the file to fix rather than at a
            # declaration to add.
            **(
                {"path": str(tool_description_overrides_path)}
                if tool_description_overrides_path is not None else {}
            ),
        },
    ))

    raw_agent_tools = agent_tools if isinstance(agent_tools, Mapping) else {}
    agent_text, agent_detail = _render_guidance_agent_tools(
        raw_agent_tools, deployment=deployment, projected_tools=projected_tools,
    )
    if agent_text:
        sections.append(agent_text)
    keys.append(GuidanceKey(
        name="agent_tools",
        declared=bool(raw_agent_tools),
        carried=bool(agent_detail["carried"]),
        reason=(
            AGENT_TOOLS_CARRIED if agent_detail["carried"]
            else (
                "the deployment declares no agent_tools"
                if not raw_agent_tools else
                "declared, but no entry is a chat-actionable operator_macro; "
                "each skipped entry is named with its reason in detail"
            )
        ),
        rendered_chars=len(agent_text),
        detail=agent_detail,
    ))

    keys.append(GuidanceKey(
        name="skills_dir",
        declared=skills_dir is not None,
        carried=False,
        reason=(
            SKILLS_DIR_NOT_CARRIED if skills_dir is not None
            else "the deployment declares no skills_dir"
        ),
        detail=(
            {"path": str(skills_dir)} if skills_dir is not None else {}
        ),
    ))
    keys.append(GuidanceKey(
        name="skill_triggers",
        declared=skill_triggers_path is not None,
        carried=False,
        reason=(
            SKILL_TRIGGERS_NOT_CARRIED if skill_triggers_path is not None
            else "the deployment declares no skill_triggers"
        ),
        detail=(
            {"path": str(skill_triggers_path)}
            if skill_triggers_path is not None else {}
        ),
    ))

    text = "\n\n".join(sections)
    if len(text) > GUIDANCE_TOTAL_BUDGET:
        raise ChatSyncError(
            f"deployment {deployment!r} composes {len(text)} characters of "
            f"standing guidance, over the {GUIDANCE_TOTAL_BUDGET}-character "
            "ceiling. The model pays for this on every turn, so the run "
            "refuses rather than shipping unbounded prompt growth.",
            data={
                "reason": "guidance_over_budget",
                "chars": len(text),
                "budget": GUIDANCE_TOTAL_BUDGET,
                "sections": [len(s) for s in sections],
            },
            fix_hint="shorten the declared overrides or the agent_tools index",
        )
    return StandingGuidance(
        text=text, keys=tuple(keys), tool_descriptions=descriptions,
    )


# ---------------------------------------------------------------------------
# Governed feature toggles
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GovernedToggle:
    """One governed feature toggle, with every place it must be driven.

    ``config_keys`` are flat dotted config-table keys; ``permission_key`` is a
    ``features.*`` leaf on the per-user permission surface. The census
    measured several toggles as TWO INDEPENDENT SWITCHES — setting only one
    leaves the other at its default — so a toggle that has both is driven and
    read back on both.
    """

    name: str
    config_keys: tuple[str, ...] = ()
    #: A FULLY QUALIFIED permission leaf, e.g. ``features.memories`` or
    #: ``workspace.knowledge``. Fully qualified rather than a bare leaf under
    #: an assumed ``features.`` section, because the governed set spans two
    #: sections of the vendor's ``UserPermissions`` form and a hard-coded
    #: section would have silently written ``features.knowledge`` — a key the
    #: application never reads, which round-trips perfectly.
    permission_key: str | None = None
    connections_key: str | None = None
    #: Config keys this requirement names that the recorded census does NOT
    #: admit at the pinned version. Each is reported as a NAMED unsatisfiable
    #: clause: never driven, never counted verified, never silently dropped.
    #: Naming the KEY (not just the toggle) is the requirement — a clause that
    #: said "knowledge bases" without naming
    #: ``rag.bypass_embedding_and_retrieval`` could be read as covering a
    #: different control than the one that is actually unproved.
    unmeasured_config_keys: tuple[str, ...] = ()
    #: A policy constant is off regardless of what the manifest says
    #: (design.md section 2). None means "take the manifest's value".
    policy_value: bool | None = None
    policy_reason: str = ""
    #: The effective value when the manifest does NOT declare the toggle.
    #: ``None`` leaves an undeclared toggle undriven (the vendor default
    #: stands). Unlike ``policy_value`` a declared value still wins, so a
    #: deployment can opt back in by writing ``chat.ui.features.<name>: true``.
    default_value: bool | None = None


#: Why the site-hygiene cuts below are off. One sentence, printed with each.
SITE_HYGIENE_REASON = (
    "policy constant: the Open WebUI 0.11.4 cut list (decision 2026-09-29) -- "
    "vendor features this site does not offer are switched off by setting"
)


def _hygiene_cut(name: str, *keys: str, permission_key: str | None = None) -> "GovernedToggle":
    """A vendor feature the site switches off by setting, on every run.

    Each key was re-read in the v0.11.4 source (revision 8bd8b4fa):
    ``config.py:3089-3149`` registers every one of these dotted keys in the
    per-key config table, ``/api/v1/configs/import`` upserts it
    (``routers/configs.py:101``) and ``/api/v1/configs/export`` reads the
    stored row back (``models/config.py:186``), and ``main.py:2356-2372``
    reads the same key live on every ``GET /api/config`` -- so a write takes
    effect without a restart and the export readback is the value the
    application uses.
    """
    return GovernedToggle(
        name=name,
        config_keys=keys,
        permission_key=permission_key,
        policy_value=False,
        policy_reason=SITE_HYGIENE_REASON,
    )


#: Every toggle the requirement governs. The always-off three are policy
#: constants with no manifest field to compare against; the rest take the
#: manifest's declared value; compaction additionally passes the verdict gate.
GOVERNED_TOGGLES: tuple[GovernedToggle, ...] = (
    GovernedToggle(
        name="memory",
        permission_key="features.memories",
        # THREE switches, and the FIRST one is the reason this comment exists.
        #
        # The recorded census rejected `memory.enable` as "not a knob" and
        # concluded that the per-user permission `features.memories` is the
        # whole memory governance surface. That conclusion is WRONG, and it
        # was wrong because the census tested a key name the vendor does not
        # use. Measured first-hand in the running container:
        #
        #   config.py:2809           'memories.enable': ENABLE_MEMORIES
        #   routers/memories.py:34   if not config.get('memories.enable'):
        #                                raise HTTPException(404)
        #   routers/memories.py:41   if user.role != 'admin' and not
        #                                has_permission(user.id, 'features.memories', ...)
        #
        # `memories.enable` (PLURAL) is the master gate; the permission arm
        # EXPLICITLY EXEMPTS ADMINS. Driving only the permission therefore
        # reports "memory verified off" while every admin keeps memory — the
        # exact absence-dressed-as-agreement shape this verb exists to
        # prevent. The evidence for treating this as measured rather than
        # census-admitted is in
        # pact/changes/chat-sync-projection/evidence/vendor-findings-v0.11.0.md:
        # the key is in config.py's defaults table (the census's own
        # false-positive guard) AND its consuming code path was read, which is
        # strictly stronger than a round-trip.
        config_keys=("memories.enable", "memories.system_context.enable"),
        policy_value=False,
        policy_reason=(
            "policy constant: per-user memory is off (design.md section 2) — "
            "the MCP `record` operator is the governed write-back path and "
            "a second "
            "ungoverned memory store is not wanted"
        ),
    ),
    GovernedToggle(
        name="code_interpreter",
        config_keys=("code_interpreter.enable", "code_execution.enable"),
        permission_key="features.code_interpreter",
        policy_value=False,
        policy_reason="policy constant: code interpreter is off (design.md section 2)",
    ),
    GovernedToggle(
        name="image_generation",
        config_keys=("image_generation.enable",),
        permission_key="features.image_generation",
        policy_value=False,
        policy_reason="policy constant: image generation is off (design.md section 2)",
    ),
    GovernedToggle(
        name="knowledge_bases",
        # THREE controls, and only ONE of them is a capability toggle.
        #
        # The obvious lever — the `workspace.knowledge` user permission —
        # cannot carry the guarantee on its own: routers/knowledge.py:286
        # reads `if user.role != 'admin' and not await has_permission(...)`,
        # so setting it false reports "knowledge off" while every admin keeps
        # full access. That is the same admin-exemption trap the census hit on
        # memory. The requirement therefore names three controls:
        #
        #   (a) rag.bypass_embedding_and_retrieval TRUE — the storage-path
        #       switch (config.py:949, PersistentConfig config.py:2862) that
        #       stops the embedding path at routers/retrieval.py:1972, and the
        #       one control that DOES reach the admin-exempt path. The
        #       recorded census has no drive-plus-readback measurement for it
        #       at v0.11.0, and req.knob-census-bound forbids driving an
        #       unadmitted knob best-effort — so it is listed as an UNMEASURED
        #       key and reported as a named unsatisfiable clause.
        #   (b) workspace.knowledge false for non-admin users — DRIVEN and
        #       read back here, on the same permissions surface the census
        #       measured for the features.* leaves.
        #   (c) the preset's knowledge binding empty — asserted INSTANCE-SIDE
        #       in `_sync_preset` from the GET /api/v1/models/list row, not
        #       from the request body this verb composes.
        permission_key="workspace.knowledge",
        unmeasured_config_keys=("rag.bypass_embedding_and_retrieval",),
        policy_value=False,
        policy_reason=(
            "policy constant: built-in RAG and knowledge bases are off "
            "(design.md section 2)"
        ),
    ),
    GovernedToggle(name="context_compaction", config_keys=("chat.context_compaction.enable",)),
    GovernedToggle(
        name="web_search",
        config_keys=("web.search.enable",),
        permission_key="features.web_search",
    ),
    # Off unless the manifest opts in (cut list items 3 and 4). The vendor
    # default for the arena is ON (config.py:2054, ENABLE_EVALUATION_ARENA_
    # MODELS 'True'), so leaving an undeclared toggle undriven kept it.
    GovernedToggle(
        name="arena", config_keys=("evaluation.arena.enable",), default_value=False,
    ),
    GovernedToggle(
        name="channels",
        config_keys=("channels.enable",),
        permission_key="features.channels",
        default_value=False,
    ),
    GovernedToggle(name="byok", connections_key="ENABLE_DIRECT_CONNECTIONS"),
    # The rest of the cut list. None of these is a manifest field: each is a
    # policy constant, driven and read back on every run.
    #: Follow-up question chips under a reply (config.py:2295, default on).
    _hygiene_cut("follow_up_generation", "task.follow_up.enable"),
    #: Automatic chat tags (config.py:2297, default on).
    _hygiene_cut("tag_generation", "task.tags.enable"),
    #: Retrieval query generation (config.py:2304, default on).
    _hygiene_cut("retrieval_query_generation", "task.query.retrieval.enable"),
    #: Message-box autocomplete (config.py:2333, default OFF -- set anyway so
    #: an admin toggle is drift the readback reports).
    _hygiene_cut("autocomplete_generation", "task.autocomplete.enable"),
    #: "Share to community" (config.py:2096, default on).
    _hygiene_cut("community_sharing", "ui.enable_community_sharing"),
    #: User status (config.py:2052, default on).
    _hygiene_cut("user_status", "users.enable_status"),
    #: Calendar (config.py:2032, default on).
    _hygiene_cut("calendar", "calendar.enable"),
    #: Automations (config.py:2034, default on).
    _hygiene_cut("automations", "automations.enable"),
    #: Voice mode: the per-user permission ``chat.call`` (routers/users.py:253,
    #: default True). USERS ONLY: the message box shows the call button to
    #: every admin regardless (src/lib/components/chat/MessageInput.svelte:
    #: 2671, ``$_user?.role === 'admin' || ...``). There is no global key.
    _hygiene_cut("voice_call", permission_key="chat.call"),
)

_TOGGLE_BY_NAME = {t.name: t for t in GOVERNED_TOGGLES}


# ---------------------------------------------------------------------------
# Desired values and readback
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Desired:
    """One value the run intends the instance to hold."""

    surface: str
    key: str
    value: Any
    mechanism: str = Mechanism.API
    #: Set when the effective desired value DEVIATES from the raw manifest.
    #: Printed with the value, because "compaction is off" is only honest
    #: alongside "because the verdict is absent for this preset at v0.11.0".
    cause: str | None = None
    manifest_value: Any = None
    fix_hint: str | None = None
    #: The census `chat.ui` knob this value came from, when it came from one.
    #: Carried into the diff so the DRIVEN knob set can be read back off a
    #: real run and compared against the census. Without it the only
    #: available check was `driven_knob_set() == load_ui_knob_census()`,
    #: which is the same expression on both sides and stays green even if
    #: the code stops driving a knob entirely.
    knob: str | None = None
    #: An environment-owned cosmetic assertion may be reported without
    #: suppressing API-owned surfaces. It is never passed to ``write`` and a
    #: mismatch is recorded as a deviation rather than a run failure.
    non_blocking: bool = False


@dataclass(frozen=True)
class Observed:
    """One readback. ``readable`` is never inferred from the value."""

    readable: bool
    value: Any = None

    @classmethod
    def missing(cls) -> "Observed":
        return cls(readable=False)


@dataclass
class SurfaceOutcome:
    """What one surface intended, what it observed, and whether it agreed."""

    surface: str
    status: str = "pending"
    changes: int = 0
    diff: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    refusal: str | None = None
    ok: bool = False
    #: key -> expected value, recorded BEFORE the write. A write the endpoint
    #: rejects produces no diff, so without this the failure could only name
    #: the surface and not what it was reconciling toward.
    intended: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "surface": self.surface,
            "status": self.status,
            "ok": self.ok,
            "changes": self.changes,
            "diff": self.diff,
        }
        if self.notes:
            payload["notes"] = list(self.notes)
        if self.refusal:
            payload["refusal"] = self.refusal
        return payload


def _normalize(value: Any) -> Any:
    """A comparable projection of a value.

    Adversarial-review finding: comparing whole server rows for dict equality
    may NEVER converge, because the server rewrites ``updated_at``, may add
    fields between versions, and may mask a credential on the way out. Every
    comparison in this module is therefore over a DERIVED SUBSET — the keys
    the run itself asked for — and this function only makes those subsets
    order-insensitive where order is not part of the value.

    On masking specifically: v0.11.0 was MEASURED (#1738) returning a
    tool-server ``key`` IN CLEAR, so the sentence this docstring used to
    carry — "the server masks ``key``" — was a guess, and it is now known to
    be version-dependent. Neither reading may be assumed, which is why the
    credential comparison goes through
    :class:`ConnectionKeyExpectation` rather than through a fixed value.
    """
    if isinstance(value, Mapping):
        return {str(k): _normalize(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    return value


def _shown(observed: Observed) -> str:
    """How an observation is rendered in a refusal.

    Could-not-read gets its own words rather than ``None``: a response that
    omitted the field and a response that returned null are different facts,
    and a message that renders both as ``None`` sends the reader looking for
    the wrong bug.
    """
    if not observed.readable:
        return "NOTHING (the response omitted it)"
    return repr(observed.value)


def _matches(expected: Any, observed: Observed) -> bool:
    """Verified-equal, and only that.

    An unreadable value is NEVER a match. That is the whole rule: an omitted
    field in a readback response says nothing about what the instance holds,
    and counting it as agreement is how a half-applied configuration comes to
    look like a working site.
    """
    if not observed.readable:
        return False
    return _normalize(expected) == _normalize(observed.value)


def _reconcile(
    outcome: SurfaceOutcome,
    desired: Sequence[Desired],
    read: Callable[[], Mapping[str, Observed]],
    write: Callable[[Sequence[Desired]], None],
) -> None:
    """Read, diff, write only what differs, then read back and ASSERT.

    The change count comes from the FIRST readback, so it is a property of the
    instance rather than of a cached local state file. There is no local state
    file anywhere in this module — which is what makes "delete the cache and
    re-run" produce the same zero-change report by construction rather than by
    a code path that has to remember to ignore it.
    """
    before = read()
    stale = [d for d in desired if not _matches(d.value, before.get(d.key, Observed.missing()))]
    unwritable = [
        d for d in stale
        if d.mechanism not in WRITABLE_MECHANISMS and not d.non_blocking
    ]
    if unwritable:
        first = unwritable[0]
        observed = before.get(first.key, Observed.missing())
        raise ChatSyncError(
            f"surface {outcome.surface!r}: {first.key} does not match and "
            f"this verb cannot write it. The recorded census gives it the "
            f"{first.mechanism!r} mechanism, which needs an instance restart "
            "or the container filesystem — neither of which this verb owns. "
            f"Expected {first.value!r}; observed {_shown(observed)}.",
            data={
                "reason": "unwritable_mechanism_mismatch",
                "surface": outcome.surface,
                "key": first.key,
                "mechanism": first.mechanism,
                "expected": first.value,
                "observed": observed.value if observed.readable else None,
                "observed_readable": observed.readable,
            },
            fix_hint=first.fix_hint or "apply the value through its census mechanism",
        )
    # Recorded BEFORE the write, so a failure mid-write still carries what the
    # surface was reconciling toward. `scn.partial-apply-fails` requires the
    # failure to name the expected value, and a rejected write has no observed
    # one to pair it with.
    outcome.intended = {item.key: item.value for item in desired}
    writable_stale = [
        d for d in stale if d.mechanism in WRITABLE_MECHANISMS
    ]
    if writable_stale:
        write(writable_stale)
    after = read()
    diff: list[dict[str, Any]] = []
    failures: list[Desired] = []
    for item in desired:
        observation = after.get(item.key, Observed.missing())
        matched = _matches(item.value, observation)
        entry: dict[str, Any] = {
            "key": item.key,
            "expected": item.value,
            "observed": observation.value if observation.readable else None,
            "readable": observation.readable,
            "verified": matched,
            "changed": any(s.key == item.key for s in writable_stale),
            "mechanism": item.mechanism,
            "knob": item.knob,
            "non_blocking": item.non_blocking,
        }
        if item.fix_hint:
            entry["fix_hint"] = item.fix_hint
        if item.cause:
            entry["cause"] = item.cause
            entry["manifest_value"] = item.manifest_value
        diff.append(entry)
        if not matched and item.non_blocking:
            outcome.notes.append(
                f"non-blocking deviation for {item.key}: expected "
                f"{item.value!r}, observed {_shown(observation)}; ownership "
                f"remains with the {item.mechanism} mechanism; "
                f"fix_hint: {item.fix_hint or 'apply the value through its census mechanism'}"
            )
        if not matched and not item.non_blocking:
            failures.append(item)
    outcome.diff = diff
    outcome.changes = len(writable_stale)
    if failures:
        first = failures[0]
        observation = after.get(first.key, Observed.missing())
        raise ChatSyncError(
            f"surface {outcome.surface!r}: {first.key} did not read back. "
            f"Expected {first.value!r}; observed {_shown(observation)}. "
            "An unreadable value is never counted as a match.",
            data={
                "reason": "readback_mismatch" if observation.readable else "readback_unreadable",
                "surface": outcome.surface,
                "key": first.key,
                "expected": first.value,
                "observed": observation.value if observation.readable else None,
                "observed_readable": observation.readable,
            },
            fix_hint=first.fix_hint or "check the instance log for the failed write",
        )
    outcome.status = "verified"
    outcome.ok = True


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

@dataclass
class SyncReport:
    deployment: str
    openwebui_version: str = ""
    census_version: str = ""
    #: The digest of the manifest-derived desired state this run applied
    #: (PACT chat-plan-apply-readback). Recorded on EVERY run, whether or
    #: not the operator passed a plan: a report that carried it only when
    #: a plan was supplied could not be compared against a plan written
    #: later, which is exactly the after-the-fact audit it exists for.
    plan_digest: str = ""
    ok: bool = False
    surfaces: dict[str, SurfaceOutcome] = field(default_factory=dict)
    posture: Posture | None = None
    prompt: ComposedPrompt | None = None
    #: The curated tool set, the filter that carries it, and what that filter
    #: will ACTUALLY admit against the advertised names.
    curation: ToolCuration | None = None
    #: Per-key decisions for the remaining declared agent-surface keys.
    guidance: StandingGuidance | None = None
    #: The second, admin-only connection carrying ``record`` and ``propose``,
    #: or None when the deployment authorizes no chat writes.
    write_connection: WriteConnection | None = None
    #: Why there is or is not one. Recorded on BOTH arms so "no write tools
    #: anywhere" is attributable to a decision rather than to silence.
    write_authorization: WriteAuthorization | None = None
    #: The source-admin preset plan (PACT chat-source-admin-writer), or None
    #: when the deployment declares no ``chat.sso.admin_group``. When set, the
    #: ``admin_preset`` surface is part of this run's written surfaces.
    admin_preset: "AdminPresetPlan | None" = None
    #: Without ``chat.sso.admin_group``: any entry or row on the instance that
    #: carries this deployment's admin ids. Read only; never removed.
    admin_preset_stale: dict[str, Any] = field(default_factory=dict)
    mcp: dict[str, Any] = field(default_factory=dict)
    #: Deployment health as this run read it, and what the owned banner became
    #: (PACT chat-health-banner-projection). Present on EVERY run, including
    #: the runs that never probed and the runs whose probe broke: a health
    #: block that is absent when the check fails is indistinguishable from a
    #: healthy deployment.
    health: dict[str, Any] = field(default_factory=dict)
    #: The byte-delivery outcome. Present on EVERY run, including the runs
    #: where the deployment declares no bundle — a surface that simply
    #: vanishes from the report when it has nothing to do is indistinguishable
    #: from one that was never consulted.
    css_bundle: Any = None
    deviations: list[dict[str, Any]] = field(default_factory=list)
    exclusions: list[dict[str, Any]] = field(default_factory=list)
    unsatisfiable: list[dict[str, Any]] = field(default_factory=list)
    #: Read off the recorded census at run time, not listed in code.
    census_excluded_knobs: list[dict[str, str]] = field(default_factory=list)
    census_admitted_knobs: list[str] = field(default_factory=list)

    @property
    def changes(self) -> int:
        return sum(o.changes for o in self.surfaces.values())

    @property
    def written_surface_names(self) -> tuple[str, ...]:
        """The literal five, plus ``admin_preset`` when the manifest declares it.

        Decided by ``chat.sso.admin_group`` alone, never by how far the run
        got, so no run can quantify over a smaller set of its own choosing.
        """
        if self.admin_preset is None:
            return WRITTEN_SURFACES
        return (*WRITTEN_SURFACES, SURFACE_ADMIN_PRESET)

    @property
    def written_surfaces_verified(self) -> int:
        """How many of the five written surfaces this run actually verified.

        The number req.readback-no-partial-apply requires printed: 5 when
        prompts is written, 4 when prompts is dropped. A DROPPED surface is
        deliberately not counted — dropping is the specified outcome for an
        unproven vendor surface, but counting it verified would make "we did
        not write it" indistinguishable from "we wrote it and checked it".
        Nothing is counted on a failed run, because `_demote_on_failure`
        withdraws every surface's success claim.
        """
        return sum(
            1
            for outcome in self.surfaces.values()
            if outcome.ok and outcome.status == "verified"
        )

    @property
    def written_surfaces_dropped(self) -> list[dict[str, Any]]:
        """Every written surface this run did NOT verify, with its named refusal.

        req.readback-no-partial-apply makes this accounting TOTAL: a written
        surface may be absent from the verified set ONLY when the run reports
        it dropped with its named refusal, so on a successful run
        ``written_surfaces_verified + len(written_surfaces_dropped)`` must
        equal ``written_surfaces_total``.

        LIVE FINDING 2026-08-18. This property previously filtered on
        ``status == "dropped"`` alone, and ``dropped`` is not the only way a
        written surface goes unwritten: ``_sync_deep_links`` reports
        ``no_consumer`` when nothing installed reads the declared base, and
        ``not_declared`` when the manifest carries no block. On the real
        okg-workspace instance — where the ``okg_review_queue`` function is
        not installed — that put ``deep_links`` in NEITHER count, and the run
        printed "written surfaces verified: 3 of 5 (dropped: prompts)". Three
        plus one is four, so the fifth surface vanished from the output
        entirely: exactly the self-scoped quantifier shrinking toward empty
        that this requirement forbids. The hermetic fixture installs the
        consuming function, so it verified four of five and could never see
        it.
        """
        return [
            {
                "surface": outcome.surface,
                "status": outcome.status,
                "refusal": outcome.refusal,
            }
            for outcome in self.surfaces.values()
            if outcome.ok and outcome.status != "verified"
        ]

    @property
    def written_surfaces_accounted(self) -> int:
        """Verified plus dropped. Printed so the total is checkable.

        On a successful run this MUST equal ``written_surfaces_total``. It is
        reported as a number rather than asserted as a property so a reader
        can do the arithmetic instead of trusting that someone did.
        """
        return self.written_surfaces_verified + len(self.written_surfaces_dropped)

    @property
    def surfaces_with_detected_changes(self) -> int:
        """How many written surfaces this run found drifted and corrected.

        req.idempotent requires the drift proof to print how many surfaces it
        perturbed and detected. Computed from the readback comparison — the
        same place `changes` comes from — so it is a property of the instance
        and not of a cached local state file.
        """
        return sum(1 for outcome in self.surfaces.values() if outcome.changes > 0)

    @property
    def driven_knobs(self) -> list[str]:
        """The census `chat.ui` knobs this run actually drove or asserted.

        Read off the diff entries a real run produced, never off the census.
        `driven_knob_set()` derives the intended set FROM the artifact, so
        comparing it against the artifact is the same expression on both
        sides; this is the observed side of that comparison.
        """
        return sorted({
            entry["knob"]
            for outcome in self.surfaces.values()
            for entry in outcome.diff
            if entry.get("knob")
        })

    @property
    def status(self) -> str:
        """``failed``, ``verified``, or ``verified_with_unsatisfiable_clauses``.

        The third state exists because a run can apply and verify every
        surface it is ALLOWED to drive while a clause the requirement names is
        still unproved — today, "built-in RAG and knowledge bases off", which
        the recorded census never measured and which
        req.chat-sync-projection.knob-census-bound therefore forbids driving.
        Reporting that as plain ``verified`` would be the vacuous pass this
        program keeps finding.
        """
        if not self.ok:
            return "failed"
        if self.unsatisfiable:
            return "verified_with_unsatisfiable_clauses"
        return "verified"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.chat.sync.v1",
            "surface": "sync",
            "deployment": self.deployment,
            "ok": self.ok,
            # A bare "verified" over a non-empty unsatisfiable list would read
            # as "everything the requirement asks for is proved", which is
            # exactly what it is not. The top line carries the qualification
            # so a reader who never scrolls to `unsatisfiable_clauses` still
            # sees that something the requirement names is unproved.
            "status": self.status,
            "openwebui_version": self.openwebui_version,
            "census_version": self.census_version,
            "plan_digest": self.plan_digest,
            "changes": self.changes,
            # The surface set is LITERAL. All six are named on every run: the
            # five written ones here and `mcp` in its own key below, so no run
            # can quantify over a smaller set of its own choosing.
            "written_surfaces": list(self.written_surface_names),
            "probe_surfaces": [SURFACE_MCP],
            # The checkable numbers, rather than an asserted "everything
            # verified": 5 when prompts is written, 4 when it is dropped.
            "written_surfaces_verified": self.written_surfaces_verified,
            "written_surfaces_total": len(self.written_surface_names),
            "written_surfaces_dropped": self.written_surfaces_dropped,
            # verified + dropped. On a successful run this equals
            # `written_surfaces_total`; printing it is what makes a surface
            # that fell out of both counts visible.
            "written_surfaces_accounted": self.written_surfaces_accounted,
            "surfaces_with_detected_changes": self.surfaces_with_detected_changes,
            # The driven-knob count, so a driven set that shrinks toward empty
            # is visible in the output instead of passing silently.
            "driven_knobs": self.driven_knobs,
            "driven_knob_count": len(self.driven_knobs),
            "census_admitted_knobs": list(self.census_admitted_knobs),
            "census_admitted_knob_count": len(self.census_admitted_knobs),
            "census_excluded_knobs": list(self.census_excluded_knobs),
            "surfaces": {
                name: outcome.to_dict() for name, outcome in self.surfaces.items()
            },
            "mcp": self.mcp,
            # Named on EVERY run. `not_probed`, `probe_failed`, `healthy`,
            # `degraded` and `unavailable` are five distinguishable answers,
            # and only one of them is `healthy`.
            "health": self.health,
            # Its own key, outside `written_surfaces`: this one delivers BYTES
            # and can never claim a value read back, so folding it into the
            # verified-surface count would overstate what was proved.
            "css_bundle": (
                self.css_bundle.to_dict() if self.css_bundle else None
            ),
            "posture": self.posture.to_dict() if self.posture else None,
            "system_prompt": self.prompt.to_dict() if self.prompt else None,
            # Named on EVERY run, declared or not: the literal default set
            # lives in `curation.substrate_default_set` so an unstated default
            # is never left to the reader to guess.
            "tool_curation": self.curation.to_dict() if self.curation else None,
            "standing_guidance": (
                self.guidance.to_dict() if self.guidance else None
            ),
            # Printed on EVERY run. The negative arm carries its REASON, so a
            # deployment with no write tools anywhere says which of the two
            # halves was missing rather than leaving the reader to infer that
            # the projection worked.
            "write_authorization": (
                self.write_authorization.to_dict()
                if self.write_authorization
                else None
            ),
            "write_connection": (
                self.write_connection.to_dict() if self.write_connection else None
            ),
            # Printed on EVERY run. The negative arm names its reason, so "no
            # admin preset" is attributable to the manifest, not to silence.
            "admin_preset": (
                {
                    **self.admin_preset.to_dict(),
                    # The SURFACE's outcome, so a planned preset that then
                    # failed to read back never reads as written.
                    "status": (
                        self.surfaces[SURFACE_ADMIN_PRESET].status
                        if SURFACE_ADMIN_PRESET in self.surfaces
                        else self.admin_preset.status
                    ),
                }
                if self.admin_preset is not None
                else {
                    "declared": False,
                    "status": "not_declared",
                    "reason": WRITE_UNAUTHORIZED_NO_ADMIN_GROUP,
                    "stale_connection": None,
                    "stale_row": None,
                    **self.admin_preset_stale,
                }
            ),
            "deviations_from_manifest": self.deviations,
            "exclusions": self.exclusions,
            "unsatisfiable_clauses": self.unsatisfiable,
        }


# ---------------------------------------------------------------------------
# Deep-link valve targets (surface 5)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ValveTarget:
    """One consuming component and the valve a declared base fills.

    Membership here means the component ACTUALLY READS the value. That bar is
    deliberately high: a valve written into a component that ignores it is
    configuration in name only, and it would make this surface report a
    carried base that changes nothing.
    """

    function_id: str
    valve: str
    base: str


#: Today there is exactly one such component. The review-queue Pipe needs this
#: instance's own base URL for its admin-group membership lookup
#: (``review_queue_pipe.Valves.openwebui_api_base``), and today an operator
#: types it by hand — a second source of truth that can disagree with
#: ``chat.deep_links.chat_site_base`` and, when it does, disagrees silently.
#:
#: ``operator_ui_base`` has NO installed consumer at this version. The audit
#: filter's link rendering lives in the substrate library
#: (``audit_filter.FilterSettings.operator_ui_base``); the only Open WebUI
#: filter file in the tree is the audit child's proof harness, which renders
#: no links and therefore declares no such valve. Rather than write the base
#: into a component that ignores it, this surface REPORTS that the base was
#: carried nowhere and why. When a link-rendering filter ships with an
#: ``operator_ui_base`` valve, it joins this table and the report changes by
#: itself.
DEEP_LINK_VALVE_TARGETS: tuple[ValveTarget, ...] = (
    ValveTarget("okg_review_queue", "openwebui_api_base", "chat_site_base"),
)

#: Declared bases with no consuming component at this version. Named here so
#: the run can say which base went nowhere, instead of leaving the operator to
#: notice that a value they declared never appears in any diff.
UNCONSUMED_BASES: tuple[tuple[str, str], ...] = (
    (
        "operator_ui_base",
        "no installed Open WebUI component declares a valve that reads it; "
        "the audit filter's link rendering is substrate-library code and the "
        "in-tree filter function renders no links",
    ),
)


# ---------------------------------------------------------------------------
# The verb
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SyncInputs:
    """Everything the run needs, resolved once so it cannot drift mid-run."""

    deployment: str
    deployment_dir: Path
    raw_manifest: Mapping[str, Any]
    chat: Any
    mcp_instructions_path: Path | None
    #: The directory holding the recorded Open WebUI measurements this run
    #: consults: the knob census and, once recorded, the prompts-API proof.
    #: ``None`` means the packaged ``okg.substrate.chat.data`` directory,
    #: resolved only when a measurement is read; a test supplies its own.
    chat_data_dir: Path | None
    #: Where THIS PROCESS dials mcp-serve. Operator-state topology.
    mcp_probe_url: str
    #: What the INSTANCE is registered with. Often a different hostname than
    #: the probe URL (a container reaches a host loopback bind through
    #: ``host.docker.internal``), which is why they are two inputs.
    mcp_instance_url: str
    preset_id: str
    mcp_server_id: str
    state_home: str | os.PathLike[str] | None = None
    #: Override for the catalog half of the masking predicate. Only the tests
    #: pass one; the run composes the deployment's real catalog. It exists so
    #: a test can exercise the UNMASKED arm — which needs a catalog that
    #: composes and declares no pii_classes — without a real ontology on disk.
    catalog_loader: Callable[[], Any] | None = None
    #: The remaining declared agent-surface keys, resolved once here so the
    #: guidance decision reads the same paths the deployment model resolved
    #: rather than re-deriving them. ``None`` means the deployment declared
    #: the key nowhere — which is a first-class case, not a defect.
    tool_description_overrides_path: Path | None = None
    skills_dir: Path | None = None
    skill_triggers_path: Path | None = None
    #: The raw ``agent_tools:`` mapping. Read from the manifest rather than
    #: the chat block: it is a deployment-level declaration that predates the
    #: chat block and is shared with the coding-agent surface.
    agent_tools: Mapping[str, Any] = field(default_factory=dict)
    #: How ``chat`` above was composed with the selected bundle's chat
    #: contribution (#1183 S3): the record ``compose_bundle_chat`` returned,
    #: which the plan carries verbatim. ``chat`` is ALREADY the composed
    #: block, so this is provenance, not a second input. ``None`` means the
    #: deployment was not installed from a bundle -- the manifest-only case
    #: S2 shipped, in which the plan is byte-identical to S2's.
    bundle_chat: Mapping[str, Any] | None = None


def load_inputs(
    deployment: str,
    *,
    chat_data_dir: Path | None = None,
    mcp_probe_url: str | None = None,
    mcp_instance_url: str | None = None,
    mcp_host: str = "127.0.0.1",
    state_home: str | os.PathLike[str] | None = None,
    install_plan_dir: Path | str | None = None,
) -> SyncInputs:
    from okg.substrate import fastyaml
    from okg.substrate.deployments import find_deployment_dir, resolve_deployment
    from okg.substrate.deployments.chat_block import parse_chat_block

    dep_dir = find_deployment_dir(deployment)
    if dep_dir is None or not (dep_dir / "deployment.yaml").is_file():
        raise ChatSyncError(
            f"deployment {deployment!r} has no deployment.yaml, so there is no "
            "chat block to project.",
            data={"deployment": deployment, "reason": "manifest_absent"},
            fix_hint="check the deployment name",
        )
    manifest_path = dep_dir / "deployment.yaml"
    raw = fastyaml.safe_load(manifest_path.read_text()) or {}
    chat = parse_chat_block(raw, manifest_path=manifest_path)
    if chat is None or not chat.enabled:
        raise ChatSyncError(
            f"deployment {deployment!r} declares no enabled chat: block. "
            "There is no declared site to render, and this verb does not "
            "invent one.",
            data={"deployment": deployment, "reason": "chat_not_enabled"},
            fix_hint=(
                "write the block with `okg chat setup --deployment "
                f"{deployment} --provider <openai|ollama|openai-compatible> "
                "--model <id> --instance-port <port> --configure-only`; a "
                "declared `enabled: false` is a decision to change by hand"
            ),
        )
    # The four agent-surface paths are read off the RESOLVED manifest, and
    # every consumer downstream reads `None` as "the deployment declares this
    # nowhere" — a first-class, reported fact. So an unresolvable manifest
    # must NOT arrive here as four Nones: that is a declared-but-unreadable
    # path presented as an absent one, which is the default-when-undetermined
    # rule this file refuses everywhere else. `load_tool_description_overrides`
    # exists precisely to refuse a declared path it cannot read; absorbing the
    # resolution failure would mean that refusal never gets the path to refuse.
    try:
        resolved = resolve_deployment(deployment)
    except Exception as exc:  # noqa: BLE001 — every failure is refused, not absorbed
        raise ChatSyncError(
            f"deployment {deployment!r} has a chat block, but its manifest "
            f"could not be resolved: {type(exc).__name__}: {exc}. The "
            f"agent-surface paths {list(RESOLVED_AGENT_SURFACE_KEYS)} are read "
            "off the resolved manifest, so continuing would report every one "
            "of them as UNDECLARED when the truth is UNKNOWN — a declared "
            "path this run could not read, rendered as a deployment that "
            "declares none. Refused instead of guessed.",
            data={
                "deployment": deployment,
                "reason": "deployment_unresolvable",
                "manifest": str(manifest_path),
                "error": f"{type(exc).__name__}: {redact(str(exc))}",
                "undetermined_keys": list(RESOLVED_AGENT_SURFACE_KEYS),
            },
            fix_hint=(
                f"fix {manifest_path} so the deployment resolves, then re-run; "
                "this verb does not treat an unreadable manifest as an empty one"
            ),
        ) from exc
    if resolved is None:
        raise ChatSyncError(
            f"resolving deployment {deployment!r} returned nothing. Same rule "
            "as an outright failure: the agent-surface paths "
            f"{list(RESOLVED_AGENT_SURFACE_KEYS)} are UNDETERMINED, not absent.",
            data={
                "deployment": deployment,
                "reason": "deployment_unresolvable",
                "manifest": str(manifest_path),
                "error": "resolve_deployment returned None",
                "undetermined_keys": list(RESOLVED_AGENT_SURFACE_KEYS),
            },
            fix_hint=f"fix {manifest_path} so the deployment resolves",
        )
    mcp_instructions = resolved.mcp_instructions
    overrides_path = resolved.tool_description_overrides
    skills_dir = resolved.skills_dir
    skill_triggers = resolved.skill_triggers

    # S3 of #1183. The bundle's contribution is composed INTO the block
    # every consumer below reads, so the plan, the apply and the readback
    # cannot hold three different opinions of the same site. A manifest
    # that widens the bundle's connections or tools refuses here, before
    # anything is dialled or written.
    bundle_record = None
    if install_plan_dir is not None:
        from okg.substrate.chat.bundle_composition import compose_bundle_chat

        selection = load_bundle_chat(install_plan_dir)
        if selection is not None:
            chat, bundle_record = compose_bundle_chat(chat, selection)

    port = chat.mcp.port if chat.mcp else None
    probe = mcp_probe_url or (f"http://{mcp_host}:{port}/mcp" if port else None)
    if not probe:
        raise ChatSyncError(
            f"deployment {deployment!r} declares no chat.mcp.port and no "
            "--mcp-probe-url was given, so the /mcp answer check has no "
            "endpoint. The endpoint is never guessed.",
            data={"deployment": deployment, "reason": "no_mcp_endpoint"},
            fix_hint="declare chat.mcp.port, or pass --mcp-probe-url",
        )
    return SyncInputs(
        deployment=deployment,
        deployment_dir=dep_dir,
        raw_manifest=raw,
        chat=chat,
        mcp_instructions_path=mcp_instructions,
        chat_data_dir=chat_data_dir,
        mcp_probe_url=probe,
        mcp_instance_url=mcp_instance_url or probe,
        preset_id=f"okg-{deployment}",
        mcp_server_id=f"okg-{deployment}",
        state_home=state_home,
        tool_description_overrides_path=overrides_path,
        skills_dir=skills_dir,
        skill_triggers_path=skill_triggers,
        agent_tools=(
            raw["agent_tools"] if isinstance(raw.get("agent_tools"), Mapping) else {}
        ),
        bundle_chat=bundle_record,
    )


def load_bundle_chat(install_plan_dir: Path | str) -> Any:
    """The chat contribution of the bundle an install plan resolved.

    ``install_plan_dir`` is the directory ``okg install`` wrote, holding
    ``plan.json`` next to the package the plan names by its
    transport-bound filename. The package is opened, VERIFIED, read, and
    closed here: the contribution's bytes are checked against the same
    object digests the plan recorded, so a package edited after the plan
    was written cannot contribute anything.
    """
    from okg.substrate.chat.bundle_composition import bundle_chat_selection
    from okg.substrate.distributions.install_plan import PLAN_FILENAME
    from okg.substrate.distributions.package import verify_distribution_package

    directory = Path(install_plan_dir)
    plan_path = directory / PLAN_FILENAME
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ChatSyncError(
            f"the install plan at {plan_path} could not be read ({exc}). An "
            "unreadable plan is not an absent one: composing the deployment's "
            "chat block alone would silently drop the bundle's contribution.",
            data={"reason": "install_plan_unreadable", "path": str(plan_path)},
            fix_hint="re-run the install verb, or drop --install-plan",
        ) from exc
    package_path = directory / str((plan.get("package") or {}).get("filename") or "")
    package = verify_distribution_package(package_path)
    try:
        return bundle_chat_selection(package, plan)
    finally:
        package.close_runtime_authority()


def resolve_operator_environment(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """``(instance base URL, admin token)``, or a refusal naming the variable."""
    env = environ if environ is not None else os.environ
    values: dict[str, str] = {}
    for name in (ENV_INSTANCE_URL, ENV_ADMIN_TOKEN):
        value = (env.get(name) or "").strip()
        if not value:
            raise ChatSyncError(
                f"{name} is unset or empty. The instance URL and its admin "
                "credential are operator environment: they differ per install "
                "and are never manifest content, so the run refuses rather "
                "than guessing either.",
                data={"reason": "operator_env_missing", "variable": name},
                fix_hint=f"export {name}=...",
            )
        values[name] = value
    return values[ENV_INSTANCE_URL], values[ENV_ADMIN_TOKEN]


def _compaction_gate(
    inputs: SyncInputs, *, declared: bool, openwebui_version: str,
) -> tuple[bool, str | None]:
    """``(effective compaction value, cause when it deviates)``.

    Compaction may be ON for a preset only when the chat-context-audit-filter
    child's verdict artifact reads verified for THIS preset at THIS version.
    An absent record, an unrecognized schema version and an explicit
    other-configuration record are all unverified — and unverified means the
    effective desired value is OFF, which is a pass with a stated cause rather
    than a mismatch.
    """
    from okg.substrate.chat.observability_verdict import (
        Configuration,
        VerdictKey,
        is_verified,
        resolve_verdict_path,
    )

    if not declared:
        return False, None
    path = resolve_verdict_path(inputs.deployment, state_home=inputs.state_home)
    key = VerdictKey(
        deployment=inputs.deployment,
        preset=inputs.preset_id,
        openwebui_version=openwebui_version,
    )
    if is_verified(path, key, Configuration.COMPACTION_ON):
        return True, None
    return False, (
        f"compaction off: the observability verdict at {path} does not read "
        f"verified_compaction_on for preset {inputs.preset_id!r} at Open WebUI "
        f"{openwebui_version}. An absent verdict is treated as unverified, "
        "never as verified."
    )


def _feature_desired(
    inputs: SyncInputs,
    *,
    openwebui_version: str,
    admitted_features: frozenset[str],
) -> tuple[dict[str, bool], list[dict[str, Any]], list[dict[str, Any]]]:
    """``(toggle name -> effective value, deviations, unsatisfiable clauses)``."""
    declared = dict((inputs.chat.ui.features or {}) if inputs.chat.ui else {})
    # FIRST, before any value is computed. A feature the manifest declares that
    # the census does not admit refuses the run; computing an effective value
    # for it first would mean the refusal came from a plan that already
    # contained the thing being refused.
    require_census_admits(
        declared.keys(),
        admitted_features,
        what="chat.ui.features toggle(s)",
        artifact=_census_artifact(inputs.chat_data_dir),
    )
    effective: dict[str, bool] = {}
    deviations: list[dict[str, Any]] = []
    unsatisfiable: list[dict[str, Any]] = []

    for toggle in GOVERNED_TOGGLES:
        for key in toggle.unmeasured_config_keys:
            # NOT driven and NOT counted as verified. The recorded census has
            # no drive-plus-readback measurement for this KEY at the pinned
            # version, and req.knob-census-bound forbids driving an unmeasured
            # knob best-effort. Reported on every run, naming the key, so it
            # cannot pass silently in either direction. The toggle's OTHER
            # controls are still driven below: an unsatisfiable clause
            # withdraws one control, not the whole governed toggle.
            unsatisfiable.append({
                "clause": (
                    "req.chat-sync-projection.toggle-verification control (a): "
                    f"{key} TRUE ({toggle.name} off)"
                ),
                "key": key,
                "reason": (
                    f"the recorded knob census contains no drive-plus-readback "
                    f"measurement for {key} at Open WebUI "
                    f"{openwebui_version}, and req.chat-sync-projection."
                    "knob-census-bound forbids driving an unadmitted knob "
                    "best-effort. It is NOT driven and NOT reported verified. "
                    "Its POSITIVE CONTROL — the same vector-row query "
                    "returning rows with the bypass OFF — is therefore also "
                    "unrun, so no zero-row result from this run may be read "
                    "as evidence."
                ),
                "candidate_mechanism": (
                    f"POST /api/v1/configs/import {{{key!r}: true}} plus a "
                    "vector-row readback (config.py:949, PersistentConfig "
                    "config.py:2862; the storage-path switch consumed at "
                    "routers/retrieval.py:1972) — unmeasured, so not driven"
                ),
                "residual_limits": (
                    "an admin can flip the bypass back through the same admin "
                    "API, and TWO write paths reach the vector store with no "
                    "bypass check: process_text (routers/retrieval.py:2109) "
                    "and process_files_batch (routers/retrieval.py:3011, whose "
                    "save_docs_to_vector_db call at :3094 is unguarded), the "
                    "latter reached by the FOURTH knowledge-ingest site at "
                    "routers/knowledge.py:2066 — so the multi-file admin add "
                    "does not inherit the bypass the three process_file sites "
                    "(routers/knowledge.py:379, 1425, 1519) do"
                ),
                "artifact": _census_report_path(inputs.chat_data_dir),
            })
        if toggle.policy_value is not None:
            value = toggle.policy_value
            if toggle.name in declared and declared[toggle.name] != value:
                deviations.append({
                    "key": f"chat.ui.features.{toggle.name}",
                    "manifest_value": declared[toggle.name],
                    "effective_value": value,
                    "cause": toggle.policy_reason,
                })
            effective[toggle.name] = value
            continue
        if toggle.name == "context_compaction":
            value, cause = _compaction_gate(
                inputs,
                declared=bool(declared.get(toggle.name, False)),
                openwebui_version=openwebui_version,
            )
            if cause:
                deviations.append({
                    "key": "chat.ui.features.context_compaction",
                    "manifest_value": declared.get(toggle.name),
                    "effective_value": value,
                    "cause": cause,
                })
            effective[toggle.name] = value
            continue
        if toggle.name in declared:
            effective[toggle.name] = bool(declared[toggle.name])
        elif toggle.default_value is not None:
            effective[toggle.name] = toggle.default_value

    return effective, deviations, unsatisfiable


def _toggle_desired(
    surface: str, effective: Mapping[str, bool],
) -> list[Desired]:
    """One :class:`Desired` per switch each governed toggle actually has."""
    out: list[Desired] = []
    for name, value in sorted(effective.items()):
        toggle = _TOGGLE_BY_NAME[name]
        # Every switch is tagged with the census container knob `features`,
        # so the driven-knob set can be read off a real run's diff.
        for key in toggle.config_keys:
            out.append(Desired(surface, f"config:{key}", value, knob="features"))
        if toggle.permission_key:
            out.append(Desired(
                surface,
                # The key is already fully qualified with its UserPermissions
                # section (`features.` or `workspace.`); prefixing a section
                # here would write a leaf the application never reads.
                f"permission:{toggle.permission_key}",
                value,
                knob="features",
            ))
        if toggle.connections_key:
            out.append(Desired(
                surface, f"connections:{toggle.connections_key}", value, knob="features",
            ))
    return out


#: The cut-list settings that are not on/off config keys. Each has its own
#: write route, and each is read back from the surface the site itself uses.
SUGGESTIONS_KEY = "suggestions:ui.prompt_suggestions"
SUGGESTIONS_I18N_KEY = "suggestions:ui.prompt_suggestions_i18n"
CHANGELOG_DEFAULT_KEY = "interface_default:showChangelog"
CHANGELOG_ADMIN_KEY = "user_setting:showChangelog"
#: ``ENABLE_VERSION_UPDATE_CHECK`` and ``ENABLE_EASTER_EGGS`` are read once
#: at import (env.py:1202, env.py:968) and served, to a signed-in caller,
#: as ``GET /api/config`` ``features.*`` (main.py:2347, :2350). Only a
#: container restart changes them; `okg chat-instance up` sets both.
ENV_UPDATE_CHECK_KEY = "env_feature:enable_version_update_check"
ENV_EASTER_EGGS_KEY = "env_feature:enable_easter_eggs"
#: The config key the interface defaults live under (config.py:3089).
INTERFACE_DEFAULTS_CONFIG_KEY = "ui.default_interface_settings"


def site_hygiene_desired(surface: str, *, deployment: str = "<deployment>") -> list[Desired]:
    """The cut-list settings that are not governed toggles.

    Shared by ``sync`` and ``chat plan`` so a readback compares the same keys
    the run writes.
    """
    recreate = (
        f"recreate the instance (okg chat-instance down --deployment "
        f"{deployment}, then up): the variable is read once at start and "
        "`chat-instance up` sets it; this verb does not own instance restart"
    )
    return [
        # Vendor starter prompts (cut 5). An EMPTY list disables them
        # (config.py:1678, "an empty list disables suggestions"); the default
        # null makes the browser show its own localized set, and a
        # per-language entry in the i18n map would win over the list
        # (src/lib/utils/localizedContent.ts:131-141), so both are driven.
        Desired(surface, SUGGESTIONS_KEY, []),
        Desired(surface, SUGGESTIONS_I18N_KEY, {}),
        # What's New (cut 1), two ways. The site-wide interface default
        # covers every admin who has not chosen otherwise
        # (routers/users.py:457-475 merges it under each user's own
        # settings); the calling admin's own merged setting is what the
        # browser actually reads (src/routes/(app)/+layout.svelte:355).
        Desired(surface, CHANGELOG_DEFAULT_KEY, False),
        Desired(surface, CHANGELOG_ADMIN_KEY, False),
        # Update toast (cut 2) and easter eggs (cut 15): environment-owned.
        # Non-blocking: a container started before this change reports the
        # deviation with the recreate hint, and the API-owned cuts on this
        # surface still apply rather than waiting on a restart.
        Desired(
            surface, ENV_UPDATE_CHECK_KEY, False,
            mechanism=Mechanism.ENV, fix_hint=recreate, non_blocking=True,
        ),
        Desired(
            surface, ENV_EASTER_EGGS_KEY, False,
            mechanism=Mechanism.ENV, fix_hint=recreate, non_blocking=True,
        ),
    ]


def _read_toggles(client: InstanceClient, desired: Sequence[Desired]) -> dict[str, Observed]:
    """Read every governed switch back from the surface that owns it.

    ``dict.get`` with a MISSING sentinel, never ``.get(key, requested_value)``:
    an omitted field must be distinguishable from agreement, because a
    response that omits a toggle says nothing at all about the instance.
    """
    wants = {d.key for d in desired}
    out: dict[str, Observed] = {}
    export = (
        client.config_export()
        if any(k.startswith(("config:", "suggestions:", "interface_default:")) for k in wants)
        else {}
    )
    perms = client.permissions() if any(k.startswith("permission:") for k in wants) else {}
    conns = client.connections_config() if any(k.startswith("connections:") for k in wants) else {}
    settings = (
        client.user_settings() if any(k.startswith("user_setting:") for k in wants) else {}
    )
    features = (
        (client.public_config().get("features") or {})
        if any(k.startswith("env_feature:") for k in wants)
        else {}
    )
    for key in wants:
        kind, _, leaf = key.partition(":")
        if kind in ("config", "suggestions"):
            # A key the site never stored is ABSENT from the export
            # (models/config.py:186-196 returns stored rows only), which is
            # unreadable, not the vendor default.
            if leaf in export:
                out[key] = Observed(True, export[leaf])
            else:
                out[key] = Observed.missing()
        elif kind == "interface_default":
            defaults = export.get(INTERFACE_DEFAULTS_CONFIG_KEY)
            if isinstance(defaults, Mapping) and leaf in defaults:
                out[key] = Observed(True, defaults[leaf])
            else:
                out[key] = Observed.missing()
        elif kind == "user_setting":
            ui = settings.get("ui") if isinstance(settings, Mapping) else None
            if isinstance(ui, Mapping) and leaf in ui:
                out[key] = Observed(True, ui[leaf])
            else:
                out[key] = Observed.missing()
        elif kind == "env_feature":
            if isinstance(features, Mapping) and leaf in features:
                out[key] = Observed(True, features[leaf])
            else:
                out[key] = Observed.missing()
        elif kind == "permission":
            section, _, name = leaf.partition(".")
            block = perms.get(section)
            if isinstance(block, Mapping) and name in block:
                out[key] = Observed(True, block[name])
            else:
                out[key] = Observed.missing()
        elif kind == "connections":
            if leaf in conns:
                out[key] = Observed(True, conns[leaf])
            else:
                out[key] = Observed.missing()
    return out


def _write_toggles(client: InstanceClient, stale: Sequence[Desired]) -> None:
    config_updates = {
        d.key.split(":", 1)[1]: d.value for d in stale if d.key.startswith("config:")
    }
    if config_updates:
        client.config_import(config_updates)
    permission_updates = [d for d in stale if d.key.startswith("permission:")]
    if permission_updates:
        current = json.loads(json.dumps(client.permissions()))
        for item in permission_updates:
            section, _, name = item.key.split(":", 1)[1].partition(".")
            current.setdefault(section, {})[name] = item.value
        client.set_permissions(current)
    connection_updates = [d for d in stale if d.key.startswith("connections:")]
    if connection_updates:
        current = json.loads(json.dumps(client.connections_config()))
        for item in connection_updates:
            current[item.key.split(":", 1)[1]] = item.value
        client.set_connections_config(current)
    if any(d.key.startswith("suggestions:") for d in stale):
        # One route writes both keys, so both are always sent at their
        # desired value (site_hygiene_desired: [] and {}) -- never one stale
        # half carried forward.
        client.set_prompt_suggestions([], {})
    default_updates = [d for d in stale if d.key.startswith("interface_default:")]
    if default_updates:
        # Read-modify-write of ONE config value holding a whole object:
        # every other interface default an admin set is carried forward.
        export = client.config_export()
        current = export.get(INTERFACE_DEFAULTS_CONFIG_KEY)
        merged = dict(current) if isinstance(current, Mapping) else {}
        for item in default_updates:
            merged[item.key.split(":", 1)[1]] = item.value
        client.config_import({INTERFACE_DEFAULTS_CONFIG_KEY: merged})
    setting_updates = [d for d in stale if d.key.startswith("user_setting:")]
    if setting_updates:
        client.update_user_ui_settings({
            item.key.split(":", 1)[1]: item.value for item in setting_updates
        })


def _banner_payload(text: str) -> list[dict[str, Any]]:
    """The deployment's DECLARED banner, as one deterministic banner object.

    ``id`` is the reserved marker :data:`health_banner.DECLARED_BANNER_ID` so a
    re-run produces the identical object and the composition can tell the
    manifest's banner from an operator's without matching text; a random id
    would make every run report a change. ``timestamp`` is fixed at 0 for the
    same reason — it is vendor bookkeeping, not part of what the deployment
    declares.

    NOT the whole desired list. ``POST /api/v1/configs/banners`` replaces the
    entire ``ui.banners`` list, so the value this surface reconciles is the
    EFFECTIVE state composed by
    :func:`~okg.substrate.chat.health_banner.compose_banner_list` — this
    declared banner, plus every banner neither this verb nor the health
    projection owns carried forward untouched, plus at most one owned health
    banner. A banner an operator added by hand is preserved, not deleted:
    ``chat-health-banner-projection`` req.lifecycle requires that banners
    without an ownership marker are never modified or removed.
    """
    return [{
        "id": health_banner.DECLARED_BANNER_ID,
        "type": "info",
        "title": None,
        "content": text,
        "dismissible": True,
        "timestamp": 0,
    }]


def _resolve_group_ids(
    client: InstanceClient, names: Sequence[str],
) -> dict[str, str]:
    """Group NAME -> group id, refusing on a name the instance does not have.

    A grant against a group that does not exist reads as an ACL and grants
    nothing, so an unresolvable name is a refusal rather than a dropped grant.
    """
    if not names:
        return {}
    by_name = {
        str(g.get("name")): str(g.get("id"))
        for g in client.groups()
        if g.get("name") and g.get("id")
    }
    missing = sorted(set(names) - set(by_name))
    if missing:
        raise ChatSyncError(
            f"chat.access.groups names {missing}, which do not exist on the "
            "instance. A grant against a nonexistent group reads as an access "
            "control and grants nothing, so it is refused rather than written.",
            data={
                "reason": "unknown_access_group",
                "missing": missing,
                "known": sorted(by_name),
            },
            fix_hint=(
                "create the group on the instance (SSO projection creates it "
                "in multi-user mode), or fix chat.access.groups"
            ),
        )
    return {name: by_name[name] for name in names}


#: Read for every signed-in person, in the exact shape Open WebUI's model
#: filter consults (``principal_type: user``, ``principal_id: *``). Its
#: ``anyone`` principal type is accepted on write but not checked when models
#: are filtered, so a grant written that way leaves the site empty.
_SIGNED_IN_READ_GRANT: Mapping[str, str] = {
    "principal_type": "user",
    "principal_id": "*",
    "permission": "read",
}


def _signed_in_grants(chat: Any) -> list[dict[str, str]]:
    """Grants for a deployment that names no ``chat.access.groups``.

    With ``chat.oidc_login`` (PACT chat-oidc-login) the identity provider has
    already decided who may sign in -- its allowed roles -- and ``mcp-serve``
    decides what each person may read, so every signed-in person sees the
    models. Without it the empty set stands: a site visible to admins only is
    what a single-operator deployment wants.
    """
    if getattr(chat, "oidc_login", None) is None:
        return []
    return [dict(_SIGNED_IN_READ_GRANT)]


def _grant_set(group_ids: Mapping[str, str]) -> list[dict[str, str]]:
    """The grant set for a resource, as a comparable sorted list.

    Derived and normalized on both sides: the server assigns each grant a
    random ``id`` and a ``created_at``, so comparing raw grant rows would
    never converge. The comparison is over
    ``(principal_type, principal_id, permission)`` alone.
    """
    return sorted(
        (
            {
                "principal_type": "group",
                "principal_id": group_id,
                "permission": "read",
            }
            for group_id in group_ids.values()
        ),
        key=lambda g: g["principal_id"],
    )


def _observed_grants(row: Mapping[str, Any]) -> Observed:
    """The row's grant set, or NOT-READ when the response omits the field.

    ``row.get("access_grants") or []`` would collapse "the response has no
    such field" into "the resource has no grants" — and since an empty grant
    set is a legitimate desired state (a deployment with no
    ``chat.access.groups``), that collapse would make an omitted field read as
    verified agreement. Every other readback in this module distinguishes the
    two; this one has to as well.
    """
    if "access_grants" not in row:
        return Observed.missing()
    return Observed(True, sorted(
        (
            {
                "principal_type": str(g.get("principal_type")),
                "principal_id": str(g.get("principal_id")),
                "permission": str(g.get("permission")),
            }
            for g in (row.get("access_grants") or [])
            if isinstance(g, Mapping)
        ),
        key=lambda g: g["principal_id"],
    ))


#: The one built-in tool category the preset turns off (cut 10). Every other
#: category keeps whatever the site or an admin chose: see
#: :func:`_preset_builtin_tools`.
PRESET_BUILTIN_TOOLS: Mapping[str, bool] = MappingProxyType({"knowledge": False})


def _preset_builtin_tools(client: "InstanceClient", model_id: str) -> dict[str, Any]:
    """The ``meta.builtinTools`` to write on a preset: current choices + knowledge off.

    Writing ``{"knowledge": false}`` alone would be wrong twice over at
    v0.11.4. The site's model defaults (``DEFAULT_MODEL_METADATA``, config
    key ``models.default_metadata``) fill a model's ``builtinTools`` ONLY
    when the model has none (``utils/models.py:339-356``: capabilities are
    merged, every other key is taken whole or not at all), so a preset that
    carries its own ``builtinTools`` stops inheriting an operator's site-wide
    ``chats: false``; and a category an admin set on the preset itself would
    be dropped. So the value is the site default, overlaid with the preset's
    current categories, overlaid with :data:`PRESET_BUILTIN_TOOLS`. Readback
    checks ``knowledge`` only; the rest is carried, not governed. An absent
    category still means the vendor default, on (``utils/tools.py:540``).
    After the first write the preset keeps a frozen copy of the site
    defaults and no longer follows later site-wide changes (the same vendor
    rule). A later sync does not refresh it either: the preset's own copy
    overlays the site default, so a changed site-wide category must be
    changed on the preset too.

    ``DEFAULT_MODEL_METADATA`` must be PRESENT in ``GET
    /api/v1/configs/models`` (``null`` when unset, ``routers/configs.py:66``):
    an answer without the field is a vendor this was not measured on, and
    guessing "no site defaults" would re-enable whatever the operator turned
    off site-wide.
    """
    config = client.models_config()
    if "DEFAULT_MODEL_METADATA" not in config:
        raise ChatSyncError(
            "GET /api/v1/configs/models did not carry DEFAULT_MODEL_METADATA, "
            "so the site's default built-in tool choices cannot be carried "
            "onto the preset; writing builtinTools without them would turn "
            "back on any category the operator switched off site-wide.",
            data={"reason": "model_defaults_unreadable", "surface": SURFACE_PRESET},
            fix_hint="check the instance version against the pinned 0.11.4",
        )
    defaults = config.get("DEFAULT_MODEL_METADATA") or {}
    site = defaults.get("builtinTools") if isinstance(defaults, Mapping) else None
    merged: dict[str, Any] = dict(site) if isinstance(site, Mapping) else {}
    row = next(
        (r for r in client.models_list() if str(r.get("id")) == model_id), None,
    )
    current = ((row or {}).get("meta") or {}).get("builtinTools")
    if isinstance(current, Mapping):
        merged.update(current)
    merged.update(PRESET_BUILTIN_TOOLS)
    return merged


def _observed_builtin_knowledge(meta: Mapping[str, Any]) -> Observed:
    """``meta.builtinTools.knowledge`` off a stored model row.

    Absent is UNREADABLE, not "on" and not "off": a row this verb never
    wrote carries no ``builtinTools`` at all, and the vendor reads that as
    the tools being offered.
    """
    block = meta.get("builtinTools")
    if isinstance(block, Mapping) and "knowledge" in block:
        return Observed(True, block["knowledge"])
    return Observed.missing()


def _model_form(
    *,
    model_id: str,
    name: str,
    base_model_id: str | None,
    meta: Mapping[str, Any],
    params: Mapping[str, Any],
    grants: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    """A ``ModelForm`` body.

    Deliberately carries NO server-owned field: ``user_id``, ``created_at``
    and ``updated_at`` are the server's, and F7's silent failure is exactly
    what happens when a caller sends them. ``access_grants`` is ALWAYS present
    — including as an empty list — because ``set_access_grants`` replaces the
    whole set and skips the replacement entirely when the field is ``None``.
    Omitting it on a run that removed ``chat.access.groups`` would leave the
    old grants in place while the run reported an ACL it no longer enforces.
    """
    return {
        "id": model_id,
        "name": name,
        "base_model_id": base_model_id,
        "meta": dict(meta),
        "params": dict(params),
        "access_grants": [dict(g) for g in grants],
        "is_active": True,
    }


def _prompts_proof(
    *, data_dir: Path | None, running_version: str,
) -> tuple[bool, str]:
    """``(driven, reason)`` for the prompts surface.

    The prompts API is an UNPROVEN vendor surface: it has plausible CRUD
    (``/create``, ``/id/{id}/update``, ``GET /id/{id}``) but nobody has
    recorded a write-plus-readback against it at the pinned version. Until
    that proof exists the surface is DROPPED with this reason, never written
    blind.
    """
    artifact = (
        Path(data_dir) if data_dir is not None else _default_chat_data_dir()
    ) / PROMPTS_PROOF_FILE
    # Reports name the file, never this machine's install location.
    shown = _report_path(data_dir, PROMPTS_PROOF_FILE)
    if not artifact.is_file():
        return False, (
            f"the prompts surface is DROPPED: no write-plus-readback proof "
            f"exists at {shown}. The prompts API is an unproven vendor "
            "surface at Open WebUI "
            f"{running_version}; writing to it without a recorded readback "
            "proof would be a blind write, which this verb does not do."
        )
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return False, (
            f"the prompts surface is DROPPED: the proof at {shown} is "
            f"unreadable ({exc}). An unreadable proof is no proof."
        )
    if not isinstance(payload, Mapping) or payload.get("proved") is not True:
        return False, (
            f"the prompts surface is DROPPED: the proof at {shown} does "
            "not record proved: true."
        )
    proved_version = payload.get("openwebui_version")
    if proved_version != running_version:
        return False, (
            f"the prompts surface is DROPPED: the readback proof at "
            f"{shown} was recorded at Open WebUI {proved_version!r}, not "
            f"the running {running_version!r}. A proof at another version "
            "proves nothing about this one."
        )
    return True, f"prompts readback proof recorded at Open WebUI {proved_version}"


def _default_observe_auth(endpoint: str, token_env: str | None) -> str:
    """Measure whether ``endpoint`` DEMANDS a credential.

    Delegates to ``chat.source_admin.observe_auth_posture`` -- the one place in
    the tree that decides this, by dialing anonymously and then with the
    credential. Never derived from a flag, an environment variable, or the fact
    that our own configuration names a token.
    """
    from okg.substrate.chat.source_admin import observe_auth_posture

    return observe_auth_posture(endpoint, auth_token_env=token_env)


def sync(
    inputs: SyncInputs,
    client: InstanceClient,
    *,
    probe_tools: Callable[[str, str | None], Sequence[str]] | None = None,
    observe_auth: Callable[[str, str | None], str] | None = None,
    probe_health: health_banner.HealthProbe | None = None,
    now: Callable[[], int] | None = None,
    container_runtime: Any = None,
    container: str | None = None,
    expected_plan: Mapping[str, Any] | None = None,
) -> SyncReport:
    """Render the deployment's chat block into its instance, and prove it.

    Fails fast on the first surface that cannot be verified: the remaining
    surfaces are reported ``not_attempted`` rather than written, because
    continuing to write after a failure deepens the partial application the
    requirement forbids. No surface reports ``ok`` on a failed run.

    ``probe_health`` is the deployment-health reading (PACT
    chat-health-banner-projection). It is INJECTED rather than defaulted here:
    ``sync`` is a library entry point with callers that hold no deployment
    runtime, and reaching for a subprocess on their behalf would make every
    such call spawn a probe it never asked for. ``okg chat sync`` passes
    :func:`~okg.substrate.chat.health_banner.cli_health_probe`. A run given
    none reports ``health.state = not_probed`` with its reason and touches no
    banner — which is not the same claim as ``healthy``, and is printed rather
    than left silent.

    ``container_runtime`` and ``container`` reach the byte-delivery surface
    (``chat.css_bundle``). They are optional because the five API surfaces
    need neither, and a caller that omits them while the manifest declares a
    bundle is REFUSED rather than quietly served a run with the delivery
    skipped — a skipped delivery reads identically to a delivered one in every
    other line of the report.
    """
    report = SyncReport(deployment=inputs.deployment)
    chat = inputs.chat

    # FIRST, before the instance is dialled at all. A plan that describes a
    # different manifest must apply NOTHING -- refusing after the version
    # call would still be safe, but refusing before it makes "nothing was
    # applied" checkable from the request log rather than believed.
    from okg.substrate.chat.plan_apply import (
        check_plan_matches_manifest,
        resolve_chat_plan,
    )

    if expected_plan is not None:
        report.plan_digest = check_plan_matches_manifest(inputs, expected_plan)
    else:
        report.plan_digest = resolve_chat_plan(
            inputs, require_credential=False,
        )["plan_digest"]

    running_version = client.version()
    report.openwebui_version = running_version
    admitted_knobs, admitted_features, census_version = census_admitted_knobs(
        data_dir=inputs.chat_data_dir, running_version=running_version,
    )
    report.census_version = census_version
    census_artifact = _census_artifact(inputs.chat_data_dir)
    declared_knobs = [
        name
        for name, value in (
            ("site_name", chat.ui.site_name if chat.ui else None),
            ("logo_ref", chat.ui.logo_ref if chat.ui else None),
            ("banner", chat.ui.banner if chat.ui else None),
            ("features", chat.ui.features if chat.ui else None),
        )
        if value is not None
    ]
    require_census_admits(
        declared_knobs, admitted_knobs, what="chat.ui knob(s)", artifact=census_artifact,
    )
    mechanisms = census_mechanisms(data_dir=inputs.chat_data_dir)
    report.census_admitted_knobs = sorted(admitted_knobs)
    report.census_excluded_knobs = census_excluded_knobs(data_dir=inputs.chat_data_dir)

    report.posture = resolve_posture(
        inputs.deployment,
        inputs.raw_manifest,
        deployment_dir=inputs.deployment_dir,
        catalog_loader=inputs.catalog_loader,
    )
    report.prompt = compose_system_prompt(
        inputs.deployment,
        mcp_instructions_path=inputs.mcp_instructions_path,
        system_prompt_path=resolve_chat_prompt_path(inputs),
    )

    # Stage one of curation: what the deployment asked for, and the posture
    # exclusion. The two-audience refusal fires here, before anything is
    # written, so a refused manifest applies nothing.
    plan, exclusions = plan_tool_curation(
        chat, deployment=inputs.deployment, posture=report.posture,
        raw_manifest=inputs.raw_manifest,
    )
    report.exclusions.extend(exclusions)

    # PACT chat-oidc-login: the chat's login and mcp-serve's verification
    # must describe one provider and one client, checked before any write.
    from okg.substrate.mcp.oidc_auth import (
        oidc_auth_from_raw_manifest,
        require_login_blocks_agree,
    )

    require_login_blocks_agree(
        oidc_auth_from_raw_manifest(inputs.raw_manifest),
        getattr(chat, "oidc_login", None),
        deployment=inputs.deployment,
    )
    # PACT chat-oidc-login: before anything is written, the instance must
    # actually offer the login the manifest declares. Otherwise forwarding each
    # person's token and opening the models to every signed-in person would
    # apply to an instance still on local accounts: every model visible, every
    # tool call refused.
    _require_instance_oidc_login(inputs, client)
    # The MCP credential, resolved ONCE and here — before the probe, and so
    # before any surface runs. It has to be first: the probe resolves the SAME
    # variable inside `read_live_tools` and turns every failure into
    # `mcp_unreachable`, so a declared-but-unset variable would be reported as
    # an endpoint that did not answer, sending the operator to restart a
    # server that is running fine. Refusing here names the actual defect, with
    # nothing applied anywhere.
    connection_auth_type, auth_token = _connection_credentials(chat)
    # S4. Resolved next to the graph credential and for the same reason: a
    # live slot naming an unset variable must refuse with nothing applied on
    # any surface, not halfway through the preset surface.
    live_slot = resolve_live_tool_slot(
        chat, deployment=inputs.deployment, mcp_server_id=inputs.mcp_server_id,
    )
    live_auth_token = (
        _resolve_connection_credential(chat, env_name=live_slot.auth_token_env)
        if live_slot is not None and live_slot.registered
        else None
    )

    # -- surface 6: the /mcp answer check. A probe, run first because the
    # preset's bound tools are asserted against what it observes.
    live_tools = _probe_mcp(
        inputs, report, bound_tools=plan.intended, probe_tools=probe_tools,
    )
    # Stage two: bind the plan to a filter list, with both vendor traps
    # handled against the ADVERTISED names rather than assumed away.
    curation = bind_tool_curation(
        plan, advertised=live_tools, deployment=inputs.deployment,
    )
    report.curation = curation
    filter_list = curation.filter_list
    effective_bound = curation.admitted
    report.mcp["effective_bound_tools"] = list(effective_bound)
    report.mcp["connection_state"] = curation.connection_state

    # The write half (PACT chat-record-affordance). Decided here, BEFORE any
    # surface is written, so a deployment that authorizes writes against an
    # endpoint with no observed authentication refuses with nothing applied
    # rather than half a site plus a refusal.
    write_connection, write_authorization_decision = plan_write_connection(
        chat,
        deployment=inputs.deployment,
        advertised=live_tools,
        endpoint=inputs.mcp_probe_url,
        mcp_server_id=inputs.mcp_server_id,
        preset_id=inputs.preset_id,
        observe_auth=observe_auth or _default_observe_auth,
    )
    report.write_connection = write_connection
    report.write_authorization = write_authorization_decision

    # The source-admin preset (PACT chat-source-admin-writer). Planned here,
    # before any surface is written, for the same reason as the write half;
    # unlike it, a refusal stays on its own surface (see plan_admin_preset).
    admin_plan = plan_admin_preset(
        chat,
        client,
        deployment=inputs.deployment,
        advertised=live_tools,
        probe_endpoint=inputs.mcp_probe_url,
        instance_endpoint=inputs.mcp_instance_url,
        mcp_server_id=inputs.mcp_server_id,
        preset_id=inputs.preset_id,
        observe_auth=observe_auth or _default_observe_auth,
        observed_posture=(
            write_connection.auth_posture if write_connection is not None else None
        ),
    )
    if admin_plan is not None and not admin_plan.projected:
        admin_plan = _dc_replace(
            admin_plan,
            existing_row=any(
                str(r.get("id")) == admin_plan.preset_id
                for r in client.models_list()
            ),
        )
    report.admin_preset = admin_plan

    report.guidance = compose_standing_guidance(
        inputs.deployment,
        projected_tools=effective_bound,
        tool_description_overrides_path=inputs.tool_description_overrides_path,
        skills_dir=inputs.skills_dir,
        skill_triggers_path=inputs.skill_triggers_path,
        agent_tools=inputs.agent_tools,
    )
    system_text = report.prompt.text
    if report.guidance.text:
        system_text = system_text + "\n\n" + report.guidance.text

    features, deviations, unsatisfiable = _feature_desired(
        inputs,
        openwebui_version=running_version,
        admitted_features=admitted_features,
    )
    report.deviations.extend(deviations)
    report.unsatisfiable.extend(unsatisfiable)

    # -- deployment health -> one owned banner (PACT
    # chat-health-banner-projection). Read BEFORE any surface is written, so
    # the banner surface reconciles once against a settled desired state.
    health_reading: health_banner.HealthReading | None = None
    health_failure: health_banner.HealthProbeFailure | None = None
    if probe_health is not None:
        health_reading, health_failure = health_banner.observe(
            inputs.deployment, probe=probe_health,
        )
    if health_failure is not None:
        # Loud by construction: an unsatisfiable clause qualifies the run's
        # top-line status, so `verified` can never be printed over a health
        # check that did not answer.
        report.unsatisfiable.append({
            "clause": "chat.health_banner",
            "reason": (
                "the deployment health probe did not answer "
                f"({health_failure.reason}), so this run could not decide "
                "whether the chat banner should be raised or cleared. An "
                "existing owned banner is HELD; none is written. A failed "
                "probe is never read as a recovery."
            ),
            "detail": health_failure.detail,
        })
    if (
        health_reading is not None
        and health_reading.degraded
        and "banner" not in admitted_knobs
    ):
        # F10, applied to the projection rather than to the manifest: the
        # health banner reaches the instance through the same `banner` knob,
        # so a re-recorded census that stops admitting it must refuse here too
        # rather than write a value nobody can read back.
        require_census_admits(
            ["banner"],
            admitted_knobs,
            what="the health banner's chat.ui knob",
            artifact=census_artifact,
        )
    now_epoch = int(now()) if now is not None else int(time.time())
    # Recorded BEFORE the surfaces run, so a run that fails before the banner
    # surface still reports what health said. `_sync_instance_ui` replaces it
    # with the same block plus the composed banner plan.
    report.health = health_banner.health_report_block(
        reading=health_reading,
        failure=health_failure,
        plan=None,
        probed=probe_health is not None,
    )

    for name in report.written_surface_names:
        report.surfaces[name] = SurfaceOutcome(surface=name, status="not_attempted")

    runners: tuple[tuple[str, Callable[[SurfaceOutcome], None]], ...] = (
        (
            SURFACE_INSTANCE_UI,
            lambda outcome: _sync_instance_ui(
                inputs,
                client,
                outcome,
                features=features,
                mechanisms=mechanisms,
                report=report,
                health_reading=health_reading,
                health_failure=health_failure,
                health_probed=probe_health is not None,
                now_epoch=now_epoch,
            ),
        ),
        (SURFACE_MODELS, lambda outcome: _sync_models(inputs, client, outcome)),
        (
            SURFACE_PRESET,
            lambda outcome: _sync_preset(
                inputs,
                client,
                outcome,
                system_text=system_text,
                filter_list=filter_list,
                bound_tools=effective_bound,
                live_tools=live_tools,
                curation=curation,
                guidance=report.guidance,
                write_connection=write_connection,
                report=report,
                auth_token=auth_token,
                live_slot=live_slot,
                live_auth_token=live_auth_token,
                admin_plan=admin_plan,
            ),
        ),
        (
            SURFACE_PROMPTS,
            lambda outcome: _sync_prompts(
                inputs,
                client,
                outcome,
                prompt=report.prompt,
                running_version=running_version,
            ),
        ),
        (SURFACE_DEEP_LINKS, lambda outcome: _sync_deep_links(inputs, client, outcome)),
    )
    if admin_plan is not None:
        # After the preset surface, which registers the admin connection this
        # row binds; the row's readback checks the connection it points at.
        runners = (
            *runners,
            (
                SURFACE_ADMIN_PRESET,
                lambda outcome: _sync_admin_preset(
                    inputs,
                    client,
                    outcome,
                    plan=admin_plan,
                    system_text=system_text,
                    live_tools=live_tools,
                ),
            ),
        )
    try:
        for name, runner in runners:
            outcome = report.surfaces[name]
            outcome.status = "pending"
            try:
                runner(outcome)
            except ChatSyncError as err:
                # A failure raised from the HTTP client knows the path it
                # called but not the surface it was reconciling, and the
                # surface is what an operator acts on. Enriched here rather
                # than threaded through every call site.
                outcome.status = "failed"
                outcome.ok = False
                outcome.refusal = str(err)
                if "surface" not in err.data:
                    err.data["surface"] = name
                if "expected" not in err.data:
                    # A write the endpoint REJECTED has no observed value —
                    # saying so is the honest rendering, and it is different
                    # from a value that read back wrong.
                    err.data["expected"] = _expected_summary(outcome)
                    err.data["observed"] = "(the write was rejected)"
                raise
        _verify_initial_selection(inputs, client, report)
        _run_css_bundle(
            inputs,
            client,
            report,
            container_runtime=container_runtime,
            container=container,
            running_version=running_version,
        )
    except ChatSyncError as err:
        # Partial application is a FAILURE, not a degraded success. Surfaces
        # that individually verified before this one keep their diff (the
        # operator needs to know what was already written) but lose their
        # `ok`, because the run must not report success for anything once a
        # surface could not be applied.
        _demote_on_failure(report)
        # Carried on the exception so the CLI can print what was applied. A
        # report that only the raising frame can see is a report nobody reads.
        err.partial_report = report  # type: ignore[attr-defined]
        raise

    report.ok = all(o.ok for o in report.surfaces.values())
    if not report.ok:
        _demote_on_failure(report)
    return report


def _vendor_initial_selection(value: Any) -> str:
    """The id the browser opens on, by the VENDOR's rule, not by ours.

    Open WebUI's built frontend does
    ``selectedModelId = $config?.default_models.split(',')[0]``
    (``_app/immutable/chunks/BLLL3FN7.js`` at v0.11.0), fed by
    ``ui.default_models`` (``backend/open_webui/config.py:3047``). The field
    is a LIST and the browser takes its head, so an exact-string readback of
    the whole field is not the same question as "what will be selected".
    Applied to the value read back off the instance, so an out-of-band edit
    that prepends another id is caught for the reason it matters.

    Deliberately does NOT trim the entry. MEASURED in the image at v0.11.0:
    the frontend splits and nothing trims (five call sites, all bare
    ``split(",")``), the write route stores the string verbatim
    (``routers/configs.py:751``) and the read route returns it verbatim
    (``:764``). So a leading space on the first entry is part of the id the
    browser looks up, it matches no model row, and the site does not open on
    the preset. Trimming here would read that as a match and report green on
    exactly the out-of-band edit this check exists to catch: a check may not
    be more forgiving than the consumer whose rule it claims to apply.
    """
    return str(value or "").split(",")[0]


def _verify_initial_selection(
    inputs: SyncInputs, client: InstanceClient, report: SyncReport,
) -> None:
    """The last thing sync checks: what a reader's FIRST question reaches.

    Surfaces 2 and 3 each verify their own writes and neither can answer
    this. The models surface writes ``ui.default_models`` and reads it back
    BEFORE the preset row exists, so it cannot know the id it just wrote
    resolves to anything; the preset surface writes the row and never looks
    at which model the browser selects. The gap between them is exactly
    #1183 finding 1: a run in which both surfaces verify clean and the site
    still opens on a model with no graph tools.

    So this runs once, after every written surface, against the instance's
    final state, and refuses on the two ways the intent can be lost: the
    selection is not the id this run resolved (something else owns the
    field, or an admin edited it mid-run), or it names no model row at all
    (the preset was never created, or was created under another id). A
    deployment that opted out with ``chat.models.ui_default`` is checked the
    same way -- against ITS declared id -- because "we pointed at a raw
    model on purpose" still has to point at a model that exists.
    """
    from okg.substrate.deployments.chat_block import ui_default_model

    intended = ui_default_model(
        inputs.chat.models, preset_id=inputs.preset_id,
    )
    outcome = report.surfaces[SURFACE_MODELS]
    config = client.models_config()
    if "DEFAULT_MODELS" not in config:
        raise ChatSyncError(
            "the instance did not report DEFAULT_MODELS, so what the browser "
            "opens on could not be read. An unreadable value is never counted "
            f"as a match; this run intended {intended!r}.",
            data={
                "reason": "initial_selection_unreadable",
                "surface": SURFACE_MODELS,
                "expected": intended,
                "observed": None,
                "observed_readable": False,
            },
            fix_hint=(
                "check the admin credential and GET "
                "/api/v1/configs/models on the instance"
            ),
        )
    raw = config["DEFAULT_MODELS"]
    selected = _vendor_initial_selection(raw)
    if selected != intended:
        raise ChatSyncError(
            f"the browser will open on {selected!r}, not on {intended!r}. "
            "Open WebUI selects the FIRST entry of ui.default_models, and "
            f"this run resolved that to {intended!r}. The observed field is "
            f"{raw!r}. A site that opens on a model other than the "
            "deployment's preset answers a first question with no graph "
            "tools and no system prompt, and says nothing about it.",
            data={
                "reason": "initial_selection_not_intended",
                "surface": SURFACE_MODELS,
                "expected": intended,
                "observed": selected,
                "default_models": raw,
                "preset_id": inputs.preset_id,
            },
            fix_hint=(
                "re-run `okg chat sync`; if it recurs, something else is "
                "writing ui.default_models on this instance"
            ),
        )
    rows = {str(row.get("id")) for row in client.models_list()}
    # Same v0.11.0 listing blindness the models surface works around: a
    # null-base row is invisible to the listing, so absence there is not
    # absence.
    if selected not in rows and client.model_row(selected) is None:
        raise ChatSyncError(
            f"the browser will open on {selected!r}, which is not a model "
            "row on this instance. The selection reads correctly and "
            "resolves to nothing, so the site opens on an empty chat.",
            data={
                "reason": "initial_selection_row_missing",
                "surface": SURFACE_MODELS,
                "expected": intended,
                "observed": selected,
                "preset_id": inputs.preset_id,
            },
            fix_hint=(
                "the preset row is written by the preset surface; re-run "
                "`okg chat sync` and read that surface's diff"
            ),
        )
    outcome.notes.append(
        f"initial selection verified: the browser opens on {selected!r}, "
        f"which is {'the deployment preset' if selected == inputs.preset_id else 'a declared built-in'} "
        "and is present on the instance"
    )


def _run_css_bundle(
    inputs: SyncInputs,
    client: InstanceClient,
    report: SyncReport,
    *,
    container_runtime: Any,
    container: str | None,
    running_version: str,
) -> None:
    """Run the byte-delivery surface and attach its outcome to the report.

    Attached BEFORE the refusal is raised, on purpose: the pin-bump and
    retraction arms REMOVE bytes as part of their outcome, and an exception
    that carried only a message would report the refusal while losing the fact
    that the instance's styling surface changed. The raise then follows the
    same rule as every other surface — a declared surface that could not be
    applied fails the run rather than degrading it.
    """
    from okg.substrate.chat.css_bundle import (
        SERVED_CSS_PATH,
        SURFACE_CSS_BUNDLE,
        ChatCssBundleError,
        sync_css_bundle,
    )
    from okg.substrate.chat.instance import container_name_for

    chat = inputs.chat
    declared = getattr(chat, "css_bundle", None)
    target = container or container_name_for(inputs.deployment)
    try:
        outcome = sync_css_bundle(
            deployment=inputs.deployment,
            deployment_dir=inputs.deployment_dir,
            css_bundle=declared,
            running_version=running_version,
            container=target,
            runtime=container_runtime,
            read_served=lambda: client.get_bytes(SERVED_CSS_PATH),
            state_root=inputs.state_home,
        )
    except ChatCssBundleError as err:
        raise ChatSyncError(
            str(err),
            data={**dict(err.data), "surface": SURFACE_CSS_BUNDLE},
            fix_hint=err.fix_hint,
        ) from err
    report.css_bundle = outcome
    if outcome.ok:
        return
    raise ChatSyncError(
        f"surface {SURFACE_CSS_BUNDLE!r}: {outcome.refusal}",
        data={
            "reason": outcome.refusal_code,
            "surface": SURFACE_CSS_BUNDLE,
            "expected": outcome.declared_sha256,
            "observed": outcome.observed_sha256,
            "removed": outcome.removed,
            "vendor_default_restored": outcome.vendor_default_restored,
        },
        fix_hint=outcome.fix_hint,
    )


def _expected_summary(outcome: SurfaceOutcome) -> Any:
    """What the surface was reconciling toward when a write was rejected.

    Read off the values recorded before the write, NOT off the diff: the diff
    is only assigned after the second readback, so a rejected write would
    otherwise report ``expected: null`` — which reads as "it wanted nothing"
    rather than "the write never landed".
    """
    return dict(outcome.intended) or {
        entry["key"]: entry["expected"] for entry in outcome.diff
    } or None


def _demote_on_failure(report: SyncReport) -> None:
    """Strip every surface's success claim once the run has failed."""
    report.ok = False
    for outcome in report.surfaces.values():
        if outcome.ok:
            outcome.ok = False
            outcome.status = "applied_but_run_failed"
            outcome.notes.append(
                "this surface verified, but the run failed on another "
                "surface; the configuration is PARTIALLY applied and no "
                "surface is reported successful"
            )


def _require_instance_oidc_login(inputs: SyncInputs, client: Any) -> None:
    """Refuse when ``chat.oidc_login`` is declared but the instance lacks it.

    Read from the instance's public config, the same place its login page
    reads which providers to offer, so the check is an observation of the
    running instance rather than of the manifest.
    """
    if getattr(inputs.chat, "oidc_login", None) is None:
        return
    providers = (client.public_config().get("oauth") or {}).get("providers") or {}
    if "oidc" in providers:
        return
    raise ChatSyncError(
        f"deployment {inputs.deployment!r} declares chat.oidc_login, but the "
        "running instance offers no OIDC login. It was started before the "
        "block was declared, or without it. Nothing was written: forwarding "
        "each person's token and opening the models to every signed-in person "
        "would apply to an instance still on local accounts.",
        data={
            "reason": "instance_oidc_login_absent",
            "deployment": inputs.deployment,
            "offered_providers": sorted(providers),
        },
        fix_hint=(
            f"recreate the instance: okg chat-instance down --deployment "
            f"{inputs.deployment}, then okg chat-instance up, then sync again"
        ),
    )


#: The variable NAME the service token rides under for the one readback that
#: carries it. Never exported: the value lives only in a private mapping.
SERVICE_TOKEN_ENV = "OKG_CHAT_OIDC_SERVICE_TOKEN"


def _service_token_environ(
    oidc_login: Any,
    secrets_out: list[str],
    *,
    environ: Mapping[str, str] | None = None,
    fetch: Callable[[str, str, str], str] | None = None,
) -> tuple[str, dict[str, str]]:
    """The chat client's service token, packaged for ``read_live_tools``.

    An OIDC-mode ``mcp-serve`` (PACT mcp-oidc-auth) accepts only tokens its
    identity provider issued, so the static credential cannot prove it
    answers. The chat client's own client-credentials token can: it lists
    tools and can read nothing as anyone. Both the client secret and the token
    are appended to ``secrets_out`` BEFORE any dial, so every failure path can
    redact them.
    """
    env = os.environ if environ is None else environ
    secret = (env.get(oidc_login.client_secret_env) or "").strip()
    if not secret:
        raise ChatSyncError(
            "the chat OIDC client secret is not set: environment variable "
            f"{oidc_login.client_secret_env} is empty or absent, so sync cannot "
            "obtain the service token that proves the OIDC-mode mcp-serve answers",
            data={
                "reason": "oidc_client_secret_env_unset",
                "env_var": oidc_login.client_secret_env,
            },
            fix_hint=f"export {oidc_login.client_secret_env}",
        )
    secrets_out.append(secret)
    if fetch is None:
        from okg.substrate.mcp.oidc_auth import fetch_service_token as fetch
    token = fetch(oidc_login.issuer, oidc_login.client_id, secret)
    secrets_out.append(token)
    return SERVICE_TOKEN_ENV, {SERVICE_TOKEN_ENV: token}


def _probe_mcp(
    inputs: SyncInputs,
    report: SyncReport,
    *,
    bound_tools: Sequence[str],
    probe_tools: Callable[[str, str | None], Sequence[str]] | None,
) -> tuple[str, ...]:
    """Surface 6: an initialize plus tools/list, with the registered credential.

    Reachability is DEFINED here, not assumed: a bare TCP connect, an HTTP 401
    and an empty listing are each a FAILURE. The round trip goes through
    ``chat.source_admin.read_live_tools`` — the one MCP client in the tree —
    rather than a second dialer written here.
    """
    auth_env = inputs.chat.mcp.auth_token_env if inputs.chat.mcp else None
    oidc_login = getattr(inputs.chat, "oidc_login", None)
    probe_secrets: list[str] = []
    service: tuple[str, dict[str, str]] | None = None
    if oidc_login is not None and probe_tools is None:
        # Obtained BEFORE the probe and outside its catch-all. A missing secret
        # or a provider refusing the token is a LOGIN problem; reported as
        # `mcp_unreachable` it would send the operator to a healthy server.
        try:
            service = _service_token_environ(oidc_login, probe_secrets)
        except ChatSyncError:
            raise
        except SubstrateError as exc:
            reason = redact(str(exc), *probe_secrets)
            provider_data = getattr(exc, "data", None) or {}
            report.mcp = {
                "endpoint": inputs.mcp_probe_url,
                "reachable": None,
                "reason": reason,
            }
            raise ChatSyncError(
                "sync could not obtain the chat client's service token, which it "
                f"needs to prove the OIDC-mode mcp-serve answers: {reason}",
                data={
                    "reason": "oidc_service_token_unavailable",
                    "provider_reason": provider_data.get("reason"),
                    "provider_error": provider_data.get("error"),
                    "endpoint": inputs.mcp_probe_url,
                    "surface": SURFACE_MCP,
                },
                fix_hint=getattr(exc, "fix_hint", None)
                or "check chat.oidc_login and the identity provider",
            ) from None

    def _default(endpoint: str, token_env: str | None) -> Sequence[str]:
        from okg.substrate.chat.source_admin import read_live_tools

        if service is None:
            return read_live_tools(endpoint, auth_token_env=token_env).tool_names
        service_env, environ = service
        return read_live_tools(
            endpoint, auth_token_env=service_env, environ=environ,
        ).tool_names

    probe = probe_tools or _default
    try:
        observed = tuple(probe(inputs.mcp_probe_url, auth_env))
    except Exception as exc:  # noqa: BLE001 — every transport failure is UNREACHABLE
        # This is the other error path carrying a second credential: the
        # value NAMED by chat.mcp.auth_token_env. The comment here used to
        # say exactly that while redacting by SHAPE only — and shape misses
        # an operator-chosen token under 20 characters or carrying a
        # character outside `[A-Za-z0-9_.-]`, which is the same hole the
        # document-loader token went through. Resolve the value and pass it
        # by identity.
        probe_secret = os.environ.get(auth_env) if auth_env else None
        reason = redact(f"{type(exc).__name__}: {exc}", probe_secret, *probe_secrets)
        report.mcp = {
            "endpoint": inputs.mcp_probe_url,
            "reachable": False,
            "reason": reason,
        }
        raise ChatSyncError(
            f"the deployment's mcp-serve endpoint {inputs.mcp_probe_url!r} did "
            f"not answer an initialize plus tools/list: {reason}. A 401, an empty "
            "listing and a bare TCP connect are failures, not answers.",
            data={
                "reason": "mcp_unreachable",
                "endpoint": inputs.mcp_probe_url,
                "surface": SURFACE_MCP,
            },
            fix_hint=(
                "start `okg mcp-serve --deployment "
                f"{inputs.deployment} --transport streamable-http`, and check "
                "the credential named by chat.mcp.auth_token_env"
            ),
        ) from exc
    if not observed:
        report.mcp = {
            "endpoint": inputs.mcp_probe_url,
            "reachable": False,
            "reason": "empty tools/list",
        }
        raise ChatSyncError(
            f"the mcp-serve endpoint {inputs.mcp_probe_url!r} answered with an "
            "EMPTY tools/list. An empty reference set is the absence of a "
            "readback, not the absence of tools: it would make every "
            "membership test below pass while proving nothing.",
            data={
                "reason": "mcp_empty_listing",
                "endpoint": inputs.mcp_probe_url,
                "surface": SURFACE_MCP,
            },
            fix_hint="check that mcp-serve registered its read operators",
        )
    missing = sorted(set(bound_tools) - set(observed))
    if missing:
        report.mcp = {
            "endpoint": inputs.mcp_probe_url,
            "reachable": True,
            "tools": list(observed),
            "reason": f"missing bound tools {missing}",
        }
        raise ChatSyncError(
            f"the mcp-serve endpoint {inputs.mcp_probe_url!r} does not carry "
            f"the preset's bound tools {missing}; it registers "
            f"{sorted(observed)}. The live listing must be a SUPERSET of what "
            "the preset binds, or the preset points at tools that do not exist.",
            data={
                "reason": "mcp_missing_bound_tools",
                "endpoint": inputs.mcp_probe_url,
                "missing": missing,
                "observed": sorted(observed),
                "surface": SURFACE_MCP,
            },
            fix_hint="narrow chat.mcp.tools, or serve the missing operators",
        )
    # EQUALITY against the declared served set, where the deployment declares
    # one (PACT effective-surface-declaration, req.esd.drift-refused).
    #
    # The superset check above answers a different question: it catches a
    # preset pointing at a tool that does not exist. It cannot catch the
    # defect this block exists for. okg-workspace declared seven chat tools,
    # sync removed `query` under Nomos enforcement, and the server registered
    # all seven — a superset, so the check above passed, and every session
    # paid for a schema the chat client was forbidden to call.
    #
    # Scoped to deployments that declare the block. A block-less deployment
    # keeps the frozen legacy path (issue #2067) and has no declared set to
    # compare against; asserting equality against the preset's bound tools
    # instead would refuse every read-only launch that also serves `query`.
    from okg.substrate.mcp.effective_surface import (
        assert_served_sets_equal,
        assert_text_serves_only,
        parse_agent_surface,
    )

    surface = parse_agent_surface(inputs.raw_manifest)
    if surface is not None:
        _projection = surface.projection("chat")
        assert_served_sets_equal(
            declared=surface.served,
            observed=observed,
            deployment=inputs.deployment,
            endpoint=inputs.mcp_probe_url,
            # The chat endpoint is a READ binding. Stated here rather than
            # inferred from the listing: an endpoint mistakenly launched
            # --allow-writes lists the write set, and inferring the mode from
            # what it listed would approve exactly that mistake. The chat
            # write path is a separate connection with its own binding.
            allow_writes=False,
            projected=_projection.tools if _projection is not None else None,
        )
        # The chat system prompt, scanned against what this client is
        # actually given. It is scanned HERE and in `okg deployments lint`,
        # not in `build_server`: an edit to chat prose must not refuse an
        # unrelated stdio launch that never sends this prompt to anyone.
        _prompt_path = resolve_chat_prompt_path(inputs)
        if _projection is not None and _prompt_path is not None and _prompt_path.is_file():
            assert_text_serves_only(
                _prompt_path.read_text(encoding="utf-8"),
                served=_projection.tools,
                source=f"the chat system prompt {_prompt_path.name}",
                deployment=inputs.deployment,
                channel_note=(
                    "Open WebUI never delivers the MCP server's initialize "
                    "instructions to the model, so this prompt is the "
                    "model's only instruction channel."
                ),
            )

    report.mcp = {
        "endpoint": inputs.mcp_probe_url,
        "instance_endpoint": inputs.mcp_instance_url,
        "reachable": True,
        "checked": "initialize + tools/list",
        "tools": list(observed),
        "covers_bound_tools": True,
        "equals_declared_served_set": surface is not None,
        "credentialed": bool(auth_env),
    }
    return observed


def _sync_instance_ui(
    inputs: SyncInputs,
    client: InstanceClient,
    outcome: SurfaceOutcome,
    *,
    features: Mapping[str, bool],
    mechanisms: Mapping[str, str],
    report: SyncReport | None = None,
    health_reading: health_banner.HealthReading | None = None,
    health_failure: health_banner.HealthProbeFailure | None = None,
    health_probed: bool = False,
    now_epoch: int = 0,
) -> None:
    chat = inputs.chat
    ui = chat.ui
    desired: list[Desired] = []

    if ui is not None and ui.site_name:
        desired.append(Desired(
            outcome.surface,
            "site_name",
            # F8: the vendor appends ' (Open WebUI)'. Comparing the raw
            # manifest value here reports drift on every run forever.
            expected_site_name(ui.site_name),
            mechanism=mechanisms.get("site_name", Mechanism.ENV),
            manifest_value=ui.site_name,
            cause=(
                "site name readback normalizes for the vendor suffix "
                "env.py:892-893 appends"
            ) if expected_site_name(ui.site_name) != ui.site_name else None,
            fix_hint=(
                f"recreate the instance (okg chat-instance down --deployment "
                f"{inputs.deployment}, then up), which sets WEBUI_NAME="
                f"{ui.site_name!r} from chat.ui.site_name; this verb does not "
                "own instance restart"
            ),
            knob="site_name",
            non_blocking=True,
        ))
    if ui is not None and ui.logo_ref:
        logo = inputs.deployment_dir / ui.logo_ref
        try:
            expected_bytes = logo.read_bytes()
        except OSError as exc:
            raise ChatSyncError(
                f"chat.ui.logo_ref points at {logo}, which cannot be read "
                f"({exc}). A declared asset that is missing is a broken "
                "declaration, not an absent one.",
                data={"reason": "logo_unreadable", "path": str(logo)},
                fix_hint="add the file, or drop chat.ui.logo_ref",
            ) from exc
        desired.append(Desired(
            outcome.surface,
            "logo_bytes_sha256",
            _sha256(expected_bytes),
            mechanism=mechanisms.get("logo_ref", Mechanism.FILE),
            fix_hint=(
                f"mount {logo} over STATIC_DIR/logo.png in the instance "
                "(backend/open_webui/static, NOT the frontend build's "
                "build/static); this verb does not own the container filesystem"
            ),
            knob="logo_ref",
        ))
    # ONE list, two owners. `ui.banners` carries the deployment's declared
    # banner AND the health projection's owned banner, and the vendor's POST
    # replaces the whole list — so the value reconciled here is the EFFECTIVE
    # desired state (chat-health-banner-projection's seam with this PACT):
    # declared banner + every unowned banner carried forward + at most one
    # owned health banner. Composed from the observed list, because a desired
    # value that did not carry the unowned banners forward would delete an
    # operator's banner on every run.
    declared_banner = (
        _banner_payload(ui.banner) if ui is not None and ui.banner else []
    )
    if declared_banner or health_reading is not None or health_failure is not None:
        plan = health_banner.compose_banner_list(
            client.banners(),
            declared=declared_banner,
            reading=health_reading,
            failure=health_failure,
            now=now_epoch,
            operator_ui_base=(
                chat.deep_links.operator_ui_base if chat.deep_links else None
            ),
        )
        if report is not None:
            report.health = health_banner.health_report_block(
                reading=health_reading,
                failure=health_failure,
                plan=plan,
                probed=health_probed,
            )
        outcome.notes.append(
            f"banner list: health banner {plan.owned_action}, "
            f"{plan.foreign_preserved} unowned banner(s) preserved"
        )
        desired.append(Desired(
            outcome.surface,
            "banner",
            list(plan.banners),
            mechanism=mechanisms.get("banner", Mechanism.API),
            # The census knob is claimed only when the MANIFEST declared it.
            # The health banner is a projection, not a declared knob, and
            # tagging it would report a driven knob the deployment never
            # declared.
            knob="banner" if declared_banner else None,
        ))
    desired.extend(_toggle_desired(outcome.surface, features))
    desired.extend(site_hygiene_desired(outcome.surface, deployment=inputs.deployment))

    def read() -> dict[str, Observed]:
        observations: dict[str, Observed] = {}
        keys = {d.key for d in desired}
        if "site_name" in keys:
            config = client.public_config()
            observations["site_name"] = (
                Observed(True, config["name"]) if "name" in config else Observed.missing()
            )
        if "logo_bytes_sha256" in keys:
            served = client.get_bytes("/static/logo.png")
            observations["logo_bytes_sha256"] = (
                Observed(True, _sha256(served)) if served is not None else Observed.missing()
            )
        if "banner" in keys:
            observations["banner"] = Observed(True, [
                {
                    "id": b.get("id"),
                    "type": b.get("type"),
                    "title": b.get("title"),
                    "content": b.get("content"),
                    "dismissible": b.get("dismissible"),
                    "timestamp": b.get("timestamp"),
                }
                for b in client.banners()
                if isinstance(b, Mapping)
            ])
        observations.update(
            _read_toggles(client, [d for d in desired if ":" in d.key])
        )
        return observations

    def write(stale: Sequence[Desired]) -> None:
        for item in stale:
            if item.key == "banner":
                client.set_banners(item.value)
        _write_toggles(client, [d for d in stale if ":" in d.key])

    _reconcile(outcome, desired, read, write)


def _sha256(payload: bytes | None) -> str | None:
    import hashlib

    if payload is None:
        return None
    return hashlib.sha256(payload).hexdigest()


def _sync_models(
    inputs: SyncInputs, client: InstanceClient, outcome: SurfaceOutcome,
) -> None:
    """Surface 2: the curated list, its grants, and the default.

    Curation is not a list, it is a GRANT SET: ``utils/models.py`` admits a
    model with no DB row only for admins ("no DB entry means no access control
    configured yet"), so a deployment that ships models without grant rows is
    admin-only and silently empty for everyone else. Each declared built-in
    therefore gets a row with the deployment's group grants.

    ADDITIVE ONLY, deliberately. A model dropped from ``chat.models.builtin``
    keeps its row and its grants: this verb owns the models the manifest
    NAMES, and deleting every other row would destroy presets and grants that
    belong to other deployments or to the operator sharing the instance.
    ``ui.model_order_list`` and ``ui.default_models`` are reconciled exactly,
    so a dropped model leaves the curated list immediately — it just is not
    garbage-collected. Stated because "sync removed it from the list but the
    row is still there" is otherwise a surprise.
    """
    from okg.substrate.deployments.chat_block import ui_default_model

    chat = inputs.chat
    builtin = list(chat.models.builtin) if chat.models else []
    # #1183 finding 1. ``chat.models.default`` is the preset's
    # ``base_model_id`` and therefore has to be a RAW provider model -- the
    # one configuration on the instance with no tools and no system prompt.
    # Writing it here made it the id the browser pre-selects, so the site
    # opened on the one model that cannot reach the graph. The preset is what
    # this field points at now; ``chat.models.ui_default`` opts out.
    ui_default = ui_default_model(chat.models, preset_id=inputs.preset_id)
    declared_ui_default = chat.models.ui_default if chat.models else None
    if declared_ui_default is not None:
        outcome.notes.append(
            f"chat.models.ui_default declares {declared_ui_default!r}, so the "
            f"browser opens on that provider model rather than on the preset "
            f"{inputs.preset_id!r}. A provider model carries no MCP tools and "
            "no system prompt, so a first question on this site is answered "
            "from the model's general knowledge and the graph is not "
            "consulted. Remove the key to open on the preset."
        )
    group_names = list(chat.access.groups) if chat.access else []
    group_ids = _resolve_group_ids(client, group_names)
    grants = _grant_set(group_ids) or _signed_in_grants(chat)
    if not grants:
        # The consequence, said out loud. `utils/models.py:514-524` admits a
        # model with no DB row only for admins; once this verb CREATES rows,
        # a row with no grants is visible to admins and to nobody else. So a
        # manifest with no chat.access.groups produces a site that verifies
        # clean and is empty for every ordinary user. That is a legitimate
        # configuration (single-operator deployments want exactly it) but it
        # is never what someone intended by accident, so it is stated rather
        # than left for them to discover in the UI.
        outcome.notes.append(
            "the manifest declares no chat.access.groups, so every model row "
            "and the preset are created with an EMPTY grant set. Open WebUI "
            "shows an ungranted model to admins only, so this site will be "
            "empty for every non-admin user. Declare chat.access.groups to "
            "grant read to a group."
        )

    desired: list[Desired] = [
        Desired(outcome.surface, "ui.default_models", ui_default),
        Desired(outcome.surface, "ui.model_order_list", builtin),
    ]
    for model_id in builtin:
        desired.append(Desired(outcome.surface, f"model:{model_id}:present", True))
        # The inventory decides whether this row is an insert or update, but
        # presence alone is not reconciliation. These are the fields this
        # surface owns in the ModelForm; a hand-edited name, active flag,
        # base-model link, or description must take the update path too.
        # ``params`` is intentionally not included: it is not declared by the
        # manifest, so the reconciler must not infer ownership of arbitrary
        # vendor/operator settings from a model row.
        desired.extend((
            Desired(outcome.surface, f"model:{model_id}:name", model_id),
            Desired(outcome.surface, f"model:{model_id}:base_model_id", None),
            Desired(outcome.surface, f"model:{model_id}:is_active", True),
            Desired(outcome.surface, f"model:{model_id}:meta.description", None),
        ))
        # Compared in BOTH directions on every run: an empty desired set is a
        # revocation, and a run that removed chat.access.groups must actually
        # revoke rather than print an ACL claim it no longer enforces.
        desired.append(Desired(outcome.surface, f"model:{model_id}:grants", grants))

    def read() -> dict[str, Observed]:
        config = client.models_config()
        rows = {str(r.get("id")): r for r in client.models_list()}
        # The builtin rows this surface writes carry base_model_id=None, and
        # at v0.11.0 every LISTING surface hides that shape (see
        # InstanceClient.model_row). The per-id route is the only truthful
        # presence readback for them.
        for model_id in builtin:
            if model_id not in rows:
                row = client.model_row(model_id)
                if row is not None:
                    rows[model_id] = row
        observations: dict[str, Observed] = {}
        observations["ui.default_models"] = (
            Observed(True, config["DEFAULT_MODELS"])
            if "DEFAULT_MODELS" in config
            else Observed.missing()
        )
        observations["ui.model_order_list"] = (
            Observed(True, list(config["MODEL_ORDER_LIST"] or []))
            if "MODEL_ORDER_LIST" in config
            else Observed.missing()
        )
        for model_id in builtin:
            row = rows.get(model_id)
            observations[f"model:{model_id}:present"] = Observed(True, row is not None)
            observations[f"model:{model_id}:name"] = (
                Observed(True, row["name"])
                if row is not None and "name" in row
                else Observed.missing()
            )
            observations[f"model:{model_id}:base_model_id"] = (
                Observed(True, row["base_model_id"])
                if row is not None and "base_model_id" in row
                else Observed.missing()
            )
            observations[f"model:{model_id}:is_active"] = (
                Observed(True, row["is_active"])
                if row is not None and "is_active" in row
                else Observed.missing()
            )
            meta = row.get("meta") if row is not None else None
            observations[f"model:{model_id}:meta.description"] = (
                Observed(True, meta["description"])
                if isinstance(meta, Mapping) and "description" in meta
                else Observed.missing()
            )
            observations[f"model:{model_id}:grants"] = (
                _observed_grants(row) if row is not None else Observed.missing()
            )
        return observations

    def write(stale: Sequence[Desired]) -> None:
        keys = {d.key for d in stale}
        if {"ui.default_models", "ui.model_order_list"} & keys:
            config = json.loads(json.dumps(client.models_config()))
            config["DEFAULT_MODELS"] = ui_default
            config["MODEL_ORDER_LIST"] = builtin
            client.set_models_config(config)
        existing = {str(r.get("id")) for r in client.models_list()}
        # Same v0.11.0 listing blindness as in read(): a null-base row the
        # listing cannot show may still exist, and creating over it is an
        # "already registered" error — so every id the listing did not show
        # is checked on the per-id route before choosing the create arm.
        existing.update(
            model_id
            for model_id in builtin
            if model_id not in existing and client.model_row(model_id) is not None
        )
        for model_id in builtin:
            model_keys = {
                key for key in keys if key.startswith(f"model:{model_id}:")
            }
            if not model_keys:
                continue
            form = _model_form(
                model_id=model_id,
                name=model_id,
                base_model_id=None,
                meta={"description": None},
                params={},
                grants=grants,
            )
            if model_id in existing:
                client.update_model(form)
            else:
                client.create_model(form)

    _reconcile(outcome, desired, read, write)


def _sync_preset(
    inputs: SyncInputs,
    client: InstanceClient,
    outcome: SurfaceOutcome,
    *,
    system_text: str,
    filter_list: Sequence[str],
    bound_tools: Sequence[str],
    live_tools: Sequence[str],
    curation: "ToolCuration | None" = None,
    guidance: "StandingGuidance | None" = None,
    write_connection: "WriteConnection | None" = None,
    report: "SyncReport | None" = None,
    auth_token: str | None = None,
    live_slot: Any = None,
    live_auth_token: str | None = None,
    admin_plan: "AdminPresetPlan | None" = None,
) -> None:
    """Surface 3: the MCP registration and the preset row.

    Both halves live here because they are one thing. F2: the registration's
    ``info.id`` IS the preset's tool id (``server:mcp:<id>``); a registration
    without one produces ``server:mcp:None``, which looks configured and fails
    at call time. F5: the preset row carries ``params.system`` and
    ``meta.toolIds``, and neither alone is a working preset.

    ``live_slot`` adds a THIRD registration (#1183 S4) for the same mechanical
    reason as the second: all three live in the one
    ``/api/v1/configs/tool_servers`` list, so a surface of its own would drop
    the other two. It is written only where an audience is granted a live
    tool, and its id is dropped from the preserved entries on EVERY run, so
    withdrawing the grant withdraws the connection.

    ``write_connection`` adds the SECOND registration and its admin-only preset
    (PACT chat-record-affordance). It shares this surface rather than getting
    one of its own for a mechanical reason: both connections live in the single
    ``/api/v1/configs/tool_servers`` list, so two surfaces writing that list
    would each drop the other's entry. The write id is excluded from the
    preserved entries on EVERY run, so withdrawing the authorization withdraws
    the connection.

    ``admin_plan`` adds the source-admin CONNECTION (PACT
    chat-source-admin-writer) for the same reason: it lives in the same list.
    Its preset ROW is the ``admin_preset`` surface's. When the deployment
    declares the admin switch, the admin id is dropped from the preserved
    entries on every rewrite and re-appended only when the plan projected, so
    a refused plan leaves no admin connection. Without the switch the id is
    preserved like any foreign entry: sync does not revoke what it no longer
    manages; the report names it as stale instead.
    """
    chat = inputs.chat
    preset = chat.preset
    # Resolved by `sync` BEFORE the MCP probe and before any surface runs, so
    # a declared-but-unset variable refuses with nothing applied anywhere —
    # not "nothing applied on this surface". Held in a local and never put on
    # a report, a note or the inputs dataclass.
    # The expectation must be computed exactly as the payload is, or the
    # readback never matches and every run reports drift and rewrites the
    # connection: with chat.oidc_login the entry forwards each person's own
    # token (`system_oauth`) and stores no key.
    auth_type = _mcp_connection_auth_type(chat) or ("bearer" if auth_token else "none")
    auth_digest = ConnectionKeyExpectation(connection_key_digest(auth_token))
    tool_id = f"{MCP_TOOL_ID_PREFIX}{inputs.mcp_server_id}"
    write_server_id = f"{inputs.mcp_server_id}{WRITE_CONNECTION_SUFFIX}"
    write_tool_id = f"{MCP_TOOL_ID_PREFIX}{write_server_id}"
    admin_server_id = f"{inputs.mcp_server_id}{ADMIN_PRESET_SUFFIX}"
    admin_registered = admin_plan is not None and admin_plan.projected
    admin_connection_entry: dict[str, Any] | None = None
    if admin_registered:
        admin_connection_entry = _connection_payload(
            url=inputs.mcp_instance_url,
            server_id=admin_server_id,
            deployment=inputs.deployment,
            filter_list=admin_plan.filter_list,
            grants=_grant_set(admin_plan.group_ids),
            description=(
                f"OKG source administration for the {inputs.deployment} "
                "deployment: granted to the admin group (instance admins "
                "see it regardless)"
            ),
            auth_token=auth_token,
            auth_type=_mcp_connection_auth_type(inputs.chat),
        )
    write_system_text = system_text
    if write_connection is not None:
        # The answer contract, carried on the ONE channel that reaches a chat
        # model: Open WebUI never shows it the MCP server's own instructions,
        # so the preset's params.system is the only door (F1). The text is the
        # substrate's, authored once in mcp/operators.py alongside the renderer
        # that produces the answer it tells the model to relay.
        from okg.substrate.mcp.operators import WRITE_ANSWER_CONTRACT_INSTRUCTION

        write_system_text = f"{system_text}\n\n{WRITE_ANSWER_CONTRACT_INSTRUCTION}"
    if curation is not None:
        # Printed by the CLI's generic per-surface note loop, so the literal
        # default set, the produced connection state and the guidance
        # accounting reach an operator who never asks for --json.
        outcome.notes.append(
            f"tool curation: declared {list(curation.declared)} "
            f"({curation.plan.declaration_source}); substrate default set is "
            f"{list(CHAT_DEFAULT_TOOLS)}; posture-excluded "
            f"{list(curation.plan.excluded)}; filter {list(curation.filter_list)} "
            f"admits {list(curation.admitted)} of advertised "
            f"{list(curation.advertised)}; state {curation.connection_state}"
        )
    if guidance is not None:
        outcome.notes.append(
            "standing guidance: carried "
            f"{list(guidance.carried)}; declared but not carried "
            f"{list(guidance.declared_not_carried)}; absent "
            f"{list(guidance.absent)}; {len(guidance.text)} rendered chars"
        )
        from okg.substrate.mcp.operators import (
            SOURCE_DEPLOYMENT_OVERRIDE,
            SOURCE_SUBSTRATE_DEFAULT,
        )

        overridden = [
            d.name for d in guidance.tool_descriptions
            if d.source == SOURCE_DEPLOYMENT_OVERRIDE
        ]
        defaulted = [
            d.name for d in guidance.tool_descriptions
            if d.source == SOURCE_SUBSTRATE_DEFAULT
        ]
        outcome.notes.append(
            f"tool descriptions: deployment override(s) {overridden}; "
            f"substrate default(s) {defaulted}"
        )
    group_ids = _resolve_group_ids(
        client, list(chat.access.groups) if chat.access else [],
    )
    grants = _grant_set(group_ids) or _signed_in_grants(chat)
    # The entries this run INTENDS. Built once, here, because two consumers
    # need the clear values: the round trip that registers them, and the
    # instance-side verify, which must dial with the credential this run
    # resolved rather than with a digested readback.
    read_connection_entry = _connection_payload(
        url=inputs.mcp_instance_url,
        server_id=inputs.mcp_server_id,
        deployment=inputs.deployment,
        filter_list=filter_list,
        grants=grants,
        auth_token=auth_token,
        auth_type=_mcp_connection_auth_type(inputs.chat),
    )
    live_server_id = live_slot.server_id if live_slot is not None else None
    live_registered = live_slot is not None and live_slot.registered
    live_connection_entry: dict[str, Any] | None = None
    if live_registered:
        # BOTH directions, on the values that reach the wire. See
        # `assert_transports_disjoint`: the slot's own refusals should make
        # this unreachable, which is why it is asserted here rather than
        # assumed.
        assert_transports_disjoint(
            filter_list, live_slot, deployment=inputs.deployment,
        )
        live_grants = _grant_set(
            _resolve_group_ids(client, list(live_slot.audiences))
        )
        live_connection_entry = _connection_payload(
            url=live_slot.url,
            server_id=live_server_id,
            deployment=inputs.deployment,
            filter_list=live_slot.granted,
            grants=live_grants,
            description=(
                f"OKG live tools for the {inputs.deployment} deployment: a "
                "SEPARATE connection, granted only to the named audience(s)"
            ),
            auth_token=live_auth_token,
        )
        outcome.notes.append(
            f"live-tool connection {live_server_id}: allows "
            f"{list(live_slot.allowed)}, grants {list(live_slot.granted)} to "
            f"audience(s) {list(live_slot.audiences)} at {live_slot.url}; "
            f"auth_type {live_connection_entry['auth_type']}. The graph "
            f"connection's filter {list(filter_list)} shares no name with it."
        )
    elif live_slot is not None:
        outcome.notes.append(
            f"live-tool slot allows {list(live_slot.allowed)} and NO audience "
            "grants any, so no live connection is registered. A slot is a "
            "ceiling, never a grant."
        )
    display_name = (preset.display_name if preset else None) or inputs.preset_id
    description = preset.description if preset else None
    suggestions = list(preset.suggestions) if preset else []

    desired: list[Desired] = [
        Desired(outcome.surface, "connection:url", inputs.mcp_instance_url),
        Desired(outcome.surface, "connection:type", "mcp"),
        # F2. Asserted explicitly rather than implied by the connection being
        # present, because the failure mode is a connection that IS present
        # with info.id absent.
        Desired(outcome.surface, "connection:info.id", inputs.mcp_server_id),
        Desired(outcome.surface, "connection:config.enable", True),
        # The credential, asserted as SHAPE and DIGEST. Never as the value:
        # the readback returns it in clear, and a deviation prints both sides.
        # Asserted on the read connection unconditionally — it is the one a
        # read-only deployment registers, and the one the old `auth_type:
        # "none"` payload left dead against an authenticating endpoint.
        Desired(outcome.surface, "connection:auth_type", auth_type),
        Desired(outcome.surface, "connection:auth_key_digest", auth_digest),
        Desired(
            outcome.surface,
            "connection:config.function_name_filter_list",
            ",".join(filter_list),
        ),
        Desired(outcome.surface, "preset:name", display_name),
        Desired(outcome.surface, "preset:params.system", system_text),
        Desired(outcome.surface, "preset:meta.description", description),
        Desired(outcome.surface, "preset:meta.toolIds", [tool_id]),
        # Control (c) of req.toggle-verification's knowledge contract, asserted
        # HERE because this is the only place the preset row is read back.
        # INSTANCE-SIDE on purpose: the verb never emits a knowledge binding,
        # so a check on its own outbound payload is true by construction and
        # would report "no knowledge base bound" with identical confidence on
        # an instance where an admin bound one by hand — the exact case the
        # control exists to catch. Read from the complete GET
        # /api/v1/models/list row instead, which is where a hand-bound base
        # shows up.
        Desired(outcome.surface, "preset:meta.knowledge", []),
        # Cut 10: the vendor's built-in KNOWLEDGE tools only. v0.11.4 offers
        # them unless `meta.builtinTools.knowledge` is false
        # (utils/tools.py:536-540 and :607, utils/middleware.py:3104); the
        # other categories (time, chats, notes, ...) keep their default, and
        # `meta.capabilities.builtin_tools` is deliberately left alone.
        Desired(outcome.surface, "preset:meta.builtinTools.knowledge", False),
        Desired(
            outcome.surface,
            "preset:meta.suggestion_prompts",
            [{"content": s} for s in suggestions],
        ),
        Desired(outcome.surface, "preset:grants", grants),
        Desired(
            outcome.surface,
            "preset:base_model_id",
            chat.models.default if chat.models else None,
        ),
        # The exclusion asserted at the WIRE, against what the live listing
        # says the filter will actually admit.
        Desired(outcome.surface, "bound_tools", sorted(bound_tools)),
        # Asserted on EVERY run, authorized or not. False is the state a
        # deployment that authorizes no writes must be IN -- and a run that
        # only asserted the positive case could not tell "no write connection"
        # apart from "we never looked".
        Desired(
            outcome.surface,
            "write_connection:present",
            write_connection is not None,
        ),
        # Asserted on EVERY run, granted or not. False is the state a
        # deployment that grants no live tool must be IN, and a run that only
        # asserted the positive case could not tell "no live connection" from
        # "we never looked" -- which is the whole authority claim.
        Desired(outcome.surface, "live_connection:present", live_registered),
    ]
    if live_registered:
        desired.extend([
            Desired(outcome.surface, "live_connection:url", live_slot.url),
            Desired(outcome.surface, "live_connection:type", "mcp"),
            Desired(outcome.surface, "live_connection:info.id", live_server_id),
            Desired(outcome.surface, "live_connection:config.enable", True),
            Desired(
                outcome.surface,
                "live_connection:auth_type",
                live_connection_entry["auth_type"],
            ),
            Desired(
                outcome.surface,
                "live_connection:auth_key_digest",
                ConnectionKeyExpectation(connection_key_digest(live_auth_token)),
            ),
            Desired(
                outcome.surface,
                "live_connection:config.function_name_filter_list",
                ",".join(live_slot.granted),
            ),
            # The separation itself, asserted as a value rather than left to
            # the payload builders: the graph connection's filter must name no
            # live tool. Read back off the instance, not off this run.
            Desired(outcome.surface, "live_connection:graph_overlap", []),
        ])

    if write_connection is not None:
        write_grants = _grant_set(
            _resolve_group_ids(client, [write_connection.admin_group])
        )
        outcome.notes.append(
            f"write connection: {write_connection.server_id} carries "
            f"{list(write_connection.intended)} for group "
            f"{write_connection.admin_group!r}; filter "
            f"{list(write_connection.filter_list)} admits "
            f"{list(write_connection.admitted)}; endpoint auth posture "
            f"{write_connection.auth_posture}"
        )
        desired.extend([
            Desired(outcome.surface, "write_connection:url", inputs.mcp_instance_url),
            Desired(outcome.surface, "write_connection:type", "mcp"),
            Desired(
                outcome.surface, "write_connection:info.id", write_server_id,
            ),
            Desired(outcome.surface, "write_connection:config.enable", True),
            Desired(outcome.surface, "write_connection:auth_type", auth_type),
            Desired(
                outcome.surface, "write_connection:auth_key_digest", auth_digest,
            ),
            Desired(
                outcome.surface,
                "write_connection:config.function_name_filter_list",
                ",".join(write_connection.filter_list),
            ),
            # Recomputed from the STORED filter against the live listing, so
            # this is what a chat client on the write connection will be
            # offered -- not what the run intended to write.
            Desired(
                outcome.surface,
                "write_bound_tools",
                sorted(write_connection.intended),
            ),
            Desired(
                outcome.surface,
                "write_preset:name",
                f"{display_name} (write)",
            ),
            Desired(outcome.surface, "write_preset:params.system", write_system_text),
            Desired(outcome.surface, "write_preset:meta.toolIds", [write_tool_id]),
            Desired(outcome.surface, "write_preset:meta.knowledge", []),
            Desired(outcome.surface, "write_preset:meta.builtinTools.knowledge", False),
            # The whole point of the second registration: the write tools reach
            # the admin group and nobody else.
            Desired(outcome.surface, "write_preset:grants", write_grants),
            Desired(
                outcome.surface,
                "write_preset:base_model_id",
                chat.models.default if chat.models else None,
            ),
        ])

    if admin_plan is not None:
        # Declared by the manifest, so asserted either way: False is the state
        # a refused plan must leave the instance in.
        desired.append(
            Desired(outcome.surface, "admin_connection:present", admin_registered)
        )
    if admin_registered:
        outcome.notes.append(
            f"admin connection: {admin_server_id} carries "
            f"{list(admin_plan.filter_list)} for group "
            f"{admin_plan.admin_group!r}"
        )
        desired.extend([
            Desired(outcome.surface, "admin_connection:url", inputs.mcp_instance_url),
            Desired(outcome.surface, "admin_connection:type", "mcp"),
            Desired(outcome.surface, "admin_connection:info.id", admin_server_id),
            Desired(outcome.surface, "admin_connection:config.enable", True),
            Desired(outcome.surface, "admin_connection:auth_type", auth_type),
            Desired(outcome.surface, "admin_connection:auth_key_digest", auth_digest),
            Desired(
                outcome.surface,
                "admin_connection:config.function_name_filter_list",
                ",".join(admin_plan.filter_list),
            ),
            Desired(
                outcome.surface,
                "admin_connection:grants",
                _grant_set(admin_plan.group_ids),
            ),
        ])

    def _connection(entries: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        for entry in entries:
            if (entry.get("info") or {}).get("id") == inputs.mcp_server_id:
                return entry
        return None

    def _write_connection_entry(
        entries: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any] | None:
        for entry in entries:
            if (entry.get("info") or {}).get("id") == write_server_id:
                return entry
        return None

    # The credential keys whose readback carried a MASK, so the digest could
    # not be compared. Recorded once per key: `read` runs twice per
    # reconcile, and a note repeated per read reads like two findings.
    masked_noted: set[str] = set()

    def _observe_key(entry: Mapping[str, Any], key: str) -> Observed:
        """The credential observation for one connection.

        `entry` came through `redact_connection`, so `key` is ALREADY a
        digest or the mask marker here — there is no code path on which the
        clear value could reach a diff entry.
        """
        if "key" not in entry:
            return Observed.missing()
        observed = entry["key"]
        if observed == CONNECTION_KEY_MASKED and key not in masked_noted:
            masked_noted.add(key)
            if report is not None:
                report.mcp["connection_key_digest_compared"] = False
            outcome.notes.append(
                f"{key}: this Open WebUI version MASKS the tool-server key "
                "on readback, so the digest could not be compared; PRESENCE "
                "is asserted instead. The credential itself is still "
                "measured, by the instance-side verify below."
            )
        return Observed(True, observed)

    def read() -> dict[str, Observed]:
        entries = client.tool_servers()
        connection = _connection(entries)
        observations: dict[str, Observed] = {}
        if connection is None:
            for key in (
                "connection:url",
                "connection:type",
                "connection:info.id",
                "connection:config.enable",
                "connection:auth_type",
                "connection:auth_key_digest",
                "connection:config.function_name_filter_list",
                "bound_tools",
            ):
                observations[key] = Observed.missing()
        else:
            config = connection.get("config") or {}
            info = connection.get("info") or {}
            observations["connection:url"] = (
                Observed(True, connection["url"]) if "url" in connection else Observed.missing()
            )
            observations["connection:type"] = (
                Observed(True, connection["type"]) if "type" in connection else Observed.missing()
            )
            observations["connection:info.id"] = (
                Observed(True, info["id"]) if "id" in info else Observed.missing()
            )
            observations["connection:config.enable"] = (
                Observed(True, config["enable"]) if "enable" in config else Observed.missing()
            )
            observations["connection:auth_type"] = (
                Observed(True, connection["auth_type"])
                if "auth_type" in connection
                else Observed.missing()
            )
            observations["connection:auth_key_digest"] = _observe_key(
                connection, "connection:auth_key_digest",
            )
            raw_filter = config.get("function_name_filter_list")
            observations["connection:config.function_name_filter_list"] = (
                Observed(True, raw_filter)
                if "function_name_filter_list" in config
                else Observed.missing()
            )
            # The OBSERVED bound set is recomputed from the readback filter
            # list against the live tools/list, so the diff shows what the
            # instance will actually offer. Reporting the expected value back
            # on a match would make the comparison self-fulfilling and would
            # print nothing useful when it fails.
            observations["bound_tools"] = (
                Observed(
                    True,
                    list(
                        effective_tool_binding(
                            live_tools, str(raw_filter or "").split(",")
                        )
                    ),
                )
                if "function_name_filter_list" in config
                else Observed.missing()
            )
        live_entry = None
        if live_server_id is not None:
            for entry in entries:
                if (entry.get("info") or {}).get("id") == live_server_id:
                    live_entry = entry
                    break
        observations["live_connection:present"] = Observed(True, live_entry is not None)
        if live_registered:
            if live_entry is None:
                for key in (
                    "live_connection:url",
                    "live_connection:type",
                    "live_connection:info.id",
                    "live_connection:config.enable",
                    "live_connection:auth_type",
                    "live_connection:auth_key_digest",
                    "live_connection:config.function_name_filter_list",
                    "live_connection:graph_overlap",
                ):
                    observations[key] = Observed.missing()
            else:
                live_config = live_entry.get("config") or {}
                live_info = live_entry.get("info") or {}
                for key, source, leaf in (
                    ("live_connection:url", live_entry, "url"),
                    ("live_connection:type", live_entry, "type"),
                    ("live_connection:auth_type", live_entry, "auth_type"),
                    ("live_connection:info.id", live_info, "id"),
                    ("live_connection:config.enable", live_config, "enable"),
                    (
                        "live_connection:config.function_name_filter_list",
                        live_config,
                        "function_name_filter_list",
                    ),
                ):
                    observations[key] = (
                        Observed(True, source[leaf])
                        if leaf in source
                        else Observed.missing()
                    )
                observations["live_connection:auth_key_digest"] = _observe_key(
                    live_entry, "live_connection:auth_key_digest",
                )
                # The separation, measured off the two STORED filters rather
                # than off what this run meant to write. An out-of-band edit
                # that put a live tool on the graph connection lands here.
                graph_entry = _connection(entries) or {}
                graph_stored = str(
                    (graph_entry.get("config") or {}).get(
                        "function_name_filter_list"
                    ) or ""
                ).split(",")
                live_stored = str(
                    live_config.get("function_name_filter_list") or ""
                ).split(",")
                observations["live_connection:graph_overlap"] = Observed(
                    True,
                    sorted(
                        {n.strip().lstrip("!") for n in graph_stored if n.strip()}
                        & {n.strip().lstrip("!") for n in live_stored if n.strip()}
                    ),
                )
        write_entry = _write_connection_entry(entries)
        # Presence is a fact about the LIST, so it is readable either way --
        # unlike the fields below, whose absence means "not observed".
        observations["write_connection:present"] = Observed(
            True, write_entry is not None,
        )
        if write_entry is None:
            for key in (
                "write_connection:url",
                "write_connection:type",
                "write_connection:info.id",
                "write_connection:config.enable",
                "write_connection:auth_type",
                "write_connection:auth_key_digest",
                "write_connection:config.function_name_filter_list",
                "write_bound_tools",
            ):
                observations[key] = Observed.missing()
        else:
            write_config = write_entry.get("config") or {}
            write_info = write_entry.get("info") or {}
            observations["write_connection:url"] = (
                Observed(True, write_entry["url"])
                if "url" in write_entry
                else Observed.missing()
            )
            observations["write_connection:type"] = (
                Observed(True, write_entry["type"])
                if "type" in write_entry
                else Observed.missing()
            )
            observations["write_connection:info.id"] = (
                Observed(True, write_info["id"])
                if "id" in write_info
                else Observed.missing()
            )
            observations["write_connection:config.enable"] = (
                Observed(True, write_config["enable"])
                if "enable" in write_config
                else Observed.missing()
            )
            observations["write_connection:auth_type"] = (
                Observed(True, write_entry["auth_type"])
                if "auth_type" in write_entry
                else Observed.missing()
            )
            observations["write_connection:auth_key_digest"] = _observe_key(
                write_entry, "write_connection:auth_key_digest",
            )
            write_raw_filter = write_config.get("function_name_filter_list")
            observations["write_connection:config.function_name_filter_list"] = (
                Observed(True, write_raw_filter)
                if "function_name_filter_list" in write_config
                else Observed.missing()
            )
            # Same rule as the read connection: the tools a chat client on THIS
            # connection is offered, computed by the vendor's own predicate
            # from the STORED filter against the live listing.
            observations["write_bound_tools"] = (
                Observed(
                    True,
                    list(
                        effective_tool_binding(
                            live_tools, str(write_raw_filter or "").split(",")
                        )
                    ),
                )
                if "function_name_filter_list" in write_config
                else Observed.missing()
            )
        if admin_plan is not None:
            admin_entry = None
            for entry in entries:
                if (entry.get("info") or {}).get("id") == admin_server_id:
                    admin_entry = entry
                    break
            observations["admin_connection:present"] = Observed(
                True, admin_entry is not None,
            )
            if admin_registered:
                observations.update(_observe_admin_connection(admin_entry))
                if admin_entry is not None:
                    observations["admin_connection:auth_key_digest"] = _observe_key(
                        admin_entry, "admin_connection:auth_key_digest",
                    )
        rows = {str(r.get("id")): r for r in client.models_list()}
        if admin_plan is None and report is not None:
            # No requests of its own: the same two reads, interpreted. The
            # last read of the run (the post-write readback) is what remains.
            report.admin_preset_stale = _stale_admin_state(
                entries,
                rows,
                server_id=admin_server_id,
                preset_id=f"{inputs.preset_id}{ADMIN_PRESET_SUFFIX}",
            )
        if write_connection is not None:
            # Read BEFORE the read-preset early return below: a missing read
            # preset row must not take the write preset's observations with it,
            # or the diff would report the write half as unmeasured whenever
            # the first run creates the read half.
            write_row = rows.get(write_connection.preset_id)
            write_keys = (
                "write_preset:name",
                "write_preset:params.system",
                "write_preset:meta.toolIds",
                "write_preset:meta.knowledge",
                "write_preset:meta.builtinTools.knowledge",
                "write_preset:grants",
                "write_preset:base_model_id",
            )
            if write_row is None:
                for key in write_keys:
                    observations[key] = Observed.missing()
            else:
                write_meta = write_row.get("meta") or {}
                write_params = write_row.get("params") or {}
                observations["write_preset:name"] = (
                    Observed(True, write_row["name"])
                    if "name" in write_row
                    else Observed.missing()
                )
                observations["write_preset:params.system"] = (
                    Observed(True, write_params["system"])
                    if "system" in write_params
                    else Observed.missing()
                )
                observations["write_preset:meta.toolIds"] = (
                    Observed(True, list(write_meta["toolIds"] or []))
                    if "toolIds" in write_meta
                    else Observed.missing()
                )
                observations["write_preset:meta.knowledge"] = (
                    Observed(True, list(write_meta["knowledge"] or []))
                    if "knowledge" in write_meta
                    else Observed.missing()
                )
                observations["write_preset:meta.builtinTools.knowledge"] = (
                    _observed_builtin_knowledge(write_meta)
                )
                observations["write_preset:grants"] = _observed_grants(write_row)
                observations["write_preset:base_model_id"] = (
                    Observed(True, write_row["base_model_id"])
                    if "base_model_id" in write_row
                    else Observed.missing()
                )
        row = rows.get(inputs.preset_id)
        if row is None:
            for key in (
                "preset:name",
                "preset:params.system",
                "preset:meta.description",
                "preset:meta.toolIds",
                "preset:meta.knowledge",
                "preset:meta.builtinTools.knowledge",
                "preset:meta.suggestion_prompts",
                "preset:grants",
                "preset:base_model_id",
            ):
                observations[key] = Observed.missing()
            return observations
        meta = row.get("meta") or {}
        params = row.get("params") or {}
        observations["preset:name"] = (
            Observed(True, row["name"]) if "name" in row else Observed.missing()
        )
        observations["preset:params.system"] = (
            Observed(True, params["system"]) if "system" in params else Observed.missing()
        )
        observations["preset:meta.description"] = (
            Observed(True, meta["description"]) if "description" in meta else Observed.missing()
        )
        observations["preset:meta.toolIds"] = (
            Observed(True, list(meta["toolIds"] or [])) if "toolIds" in meta else Observed.missing()
        )
        # Absent is UNREADABLE, not empty — the same rule every other readback
        # here obeys. It costs nothing: the write below always sends
        # `knowledge: []`, so an absent field makes the value stale, the write
        # lands it, and the second read verifies it. Reading absence as
        # agreement would instead accept a row this verb never wrote.
        observations["preset:meta.knowledge"] = (
            Observed(True, list(meta["knowledge"] or []))
            if "knowledge" in meta
            else Observed.missing()
        )
        observations["preset:meta.builtinTools.knowledge"] = (
            _observed_builtin_knowledge(meta)
        )
        observations["preset:meta.suggestion_prompts"] = (
            Observed(True, [
                {"content": s.get("content")}
                for s in (meta["suggestion_prompts"] or [])
                if isinstance(s, Mapping)
            ])
            if "suggestion_prompts" in meta
            else Observed.missing()
        )
        observations["preset:grants"] = _observed_grants(row)
        observations["preset:base_model_id"] = (
            Observed(True, row["base_model_id"])
            if "base_model_id" in row
            else Observed.missing()
        )
        return observations

    def write(stale: Sequence[Desired]) -> None:
        keys = {d.key for d in stale}
        connection_keys = {"bound_tools", "write_bound_tools"}
        if any(
            k.startswith(
                (
                    "connection:",
                    "write_connection:",
                    "live_connection:",
                    "admin_connection:",
                )
            ) or k in connection_keys
            for k in keys
        ):
            # BOTH ids are dropped from the preserved list on every run, and
            # the write entry is re-appended only when one is authorized. That
            # is what makes withdrawing the authorization actually withdraw the
            # connection, instead of leaving a write surface registered because
            # nothing was left to notice it.
            #
            # RAW, and this is the one place that may be: `POST
            # /api/v1/configs/tool_servers` replaces the WHOLE list, so every
            # entry this deployment does not own is written straight back.
            # Reading it through the redacted view would rewrite a foreign
            # tool server's credential as the literal digest string and
            # destroy it. Nothing here is observed — the entries go to the
            # instance and nowhere else.
            entries = [
                dict(e)
                for e in client.tool_servers_raw()
                if (e.get("info") or {}).get("id")
                not in {
                    inputs.mcp_server_id,
                    write_server_id,
                    live_server_id,
                    # Only when this deployment declares the admin switch:
                    # without it, an entry carrying this id is not ours (an
                    # operator's, or another deployment's whose id collides)
                    # and is written back untouched. It is reported as
                    # possibly stale instead (`admin_preset.stale_connection`).
                    admin_server_id if admin_plan is not None else None,
                }
            ]
            entries.append(dict(read_connection_entry))
            if admin_connection_entry is not None:
                entries.append(dict(admin_connection_entry))
            if live_connection_entry is not None:
                entries.append(dict(live_connection_entry))
            if write_connection is not None:
                entries.append(
                    _connection_payload(
                        url=inputs.mcp_instance_url,
                        server_id=write_server_id,
                        deployment=inputs.deployment,
                        filter_list=write_connection.filter_list,
                        grants=_grant_set(
                            _resolve_group_ids(
                                client, [write_connection.admin_group],
                            )
                        ),
                        description=(
                            f"OKG write surface for the {inputs.deployment} "
                            "deployment: record and propose, held for review"
                        ),
                        auth_token=auth_token,
                        auth_type=_mcp_connection_auth_type(inputs.chat),
                    )
                )
            client.set_tool_servers(entries)
        if write_connection is not None and any(
            k.startswith("write_preset:") for k in keys
        ):
            write_form = _model_form(
                model_id=write_connection.preset_id,
                name=f"{display_name} (write)",
                base_model_id=chat.models.default if chat.models else None,
                meta={
                    "description": (
                        "Record a finding. Writes are held as a pending "
                        "admission envelope until an operator accepts them."
                    ),
                    "toolIds": [write_tool_id],
                    "knowledge": [],
                    "builtinTools": _preset_builtin_tools(
                        client, write_connection.preset_id,
                    ),
                    "suggestion_prompts": [],
                },
                params={"system": write_system_text},
                grants=_grant_set(
                    _resolve_group_ids(client, [write_connection.admin_group])
                ),
            )
            existing_ids = {str(r.get("id")) for r in client.models_list()}
            if write_connection.preset_id in existing_ids:
                client.update_model(write_form)
            else:
                client.create_model(write_form)
        if any(k.startswith("preset:") for k in keys):
            form = _model_form(
                model_id=inputs.preset_id,
                name=display_name,
                base_model_id=chat.models.default if chat.models else None,
                meta={
                    "description": description,
                    "toolIds": [tool_id],
                    # ALWAYS sent, including as an empty list. `meta` is
                    # replaced wholesale, so omitting the key on an instance
                    # where an admin bound a knowledge base by hand would
                    # leave the binding in place while control (c) reported it
                    # gone. Sending [] is what makes the correction land.
                    "knowledge": [],
                    "builtinTools": _preset_builtin_tools(client, inputs.preset_id),
                    "suggestion_prompts": [{"content": s} for s in suggestions],
                },
                params={"system": system_text},
                grants=grants,
            )
            existing = {str(r.get("id")) for r in client.models_list()}
            if inputs.preset_id in existing:
                # F7. NEVER /api/v1/models/sync: at v0.11.0 its update arm
                # raises, the exception is swallowed, and the router returns
                # HTTP 200 with [] while the row is unchanged.
                client.update_model(form)
            else:
                client.create_model(form)

    _reconcile(outcome, desired, read, write)
    _verify_registered_connection(
        inputs,
        client,
        outcome,
        connection=read_connection_entry,
        report=report,
    )
    if live_connection_entry is not None:
        # Its OWN measurement, against its own endpoint with its own
        # credential. The graph connection verifying says nothing about
        # whether the live one can reach the system it names.
        _verify_registered_connection(
            inputs,
            client,
            outcome,
            connection=live_connection_entry,
            report=report,
            server_id=live_server_id,
        )


#: The admin-connection fields the preset surface reads back, besides the
#: credential digest (which goes through ``_sync_preset``'s mask handling).
_ADMIN_CONNECTION_KEYS: tuple[str, ...] = (
    "admin_connection:url",
    "admin_connection:type",
    "admin_connection:info.id",
    "admin_connection:config.enable",
    "admin_connection:auth_type",
    "admin_connection:auth_key_digest",
    "admin_connection:config.function_name_filter_list",
    "admin_connection:grants",
)


def _observe_admin_connection(entry: Mapping[str, Any] | None) -> dict[str, Observed]:
    """The admin connection as the instance stores it. Absent is NOT-READ."""
    if entry is None:
        return {key: Observed.missing() for key in _ADMIN_CONNECTION_KEYS}
    config = entry.get("config") or {}
    info = entry.get("info") or {}
    observations: dict[str, Observed] = {}
    for key, source, leaf in (
        ("admin_connection:url", entry, "url"),
        ("admin_connection:type", entry, "type"),
        ("admin_connection:auth_type", entry, "auth_type"),
        ("admin_connection:info.id", info, "id"),
        ("admin_connection:config.enable", config, "enable"),
        (
            "admin_connection:config.function_name_filter_list",
            config,
            "function_name_filter_list",
        ),
    ):
        observations[key] = (
            Observed(True, source[leaf]) if leaf in source else Observed.missing()
        )
    # The same normalization as a model row's grants: the server may add an
    # id and a timestamp to each grant, and an absent field is NOT-READ.
    observations["admin_connection:grants"] = _observed_grants(config)
    return observations


def _sync_admin_preset(
    inputs: SyncInputs,
    client: InstanceClient,
    outcome: SurfaceOutcome,
    *,
    plan: AdminPresetPlan,
    system_text: str,
    live_tools: Sequence[str],
) -> None:
    """The source-admin preset ROW (PACT chat-source-admin-writer).

    A refused plan writes nothing and reports its named status with the
    refusal; it counts in ``written_surfaces_dropped`` so the accounting stays
    total. A projected plan is reconciled like every other written surface:
    read, diff, write the stale keys through ``POST /api/v1/models/create`` or
    ``.../model/update`` (never ``/api/v1/models/sync``, F7), read back, and
    assert. ``meta.toolIds`` and the grant set come from the projection.

    What this proves is SPECIFIED gating, not enforced gating: the group grant
    is visibility, which Open WebUI bypasses for every instance admin by role,
    and the per-caller refusal is blocked on mcp-principal-mapping. The
    projection's own flags carry that into the report.
    """
    if not plan.projected:
        outcome.status = plan.status
        outcome.ok = True
        outcome.refusal = plan.refusal
        outcome.notes.append(
            f"admin preset {plan.preset_id} NOT written ({plan.reason}); the "
            "preset surface registers no admin connection and removes one an "
            "earlier run left, and this surface creates or updates no row. "
            + (
                "An admin row from an earlier run EXISTS and is left in place "
                "(see admin_preset.fix_hint). "
                if plan.existing_row
                else ""
            )
            + "The rest of the run is unaffected."
        )
        return

    chat = inputs.chat
    projected = plan.projection.to_dict()
    tool_ids = list(projected["meta_tool_ids"])
    grants = _grant_set(plan.group_ids)
    preset = chat.preset
    display_name = (preset.display_name if preset else None) or inputs.preset_id
    name = f"{display_name} (source admin)"
    base_model_id = chat.models.default if chat.models else None
    if plan.projection.transport.warning:
        outcome.notes.append(f"transport warning: {plan.projection.transport.warning}")
    outcome.notes.append(
        f"gating claim {ADMIN_PRESET_GATING_CLAIM}: "
        f"{plan.projection.enforcement_label}, enforcement_sufficient "
        f"{plan.projection.enforcement_sufficient}; blocked "
        f"{list(plan.projection.blocked_arms)}"
    )

    desired = [
        Desired(outcome.surface, "admin_preset:name", name),
        Desired(outcome.surface, "admin_preset:params.system", system_text),
        Desired(outcome.surface, "admin_preset:meta.toolIds", tool_ids),
        Desired(outcome.surface, "admin_preset:meta.knowledge", []),
        Desired(outcome.surface, "admin_preset:meta.builtinTools.knowledge", False),
        Desired(outcome.surface, "admin_preset:grants", grants),
        Desired(outcome.surface, "admin_preset:base_model_id", base_model_id),
        # The tools reachable through the server id the row binds: recomputed
        # from the STORED admin-connection filter against the live listing.
        Desired(
            outcome.surface,
            "admin_preset:reachable_tools",
            sorted(plan.projection.bound_tools),
        ),
    ]
    keys = [d.key for d in desired]

    def read() -> dict[str, Observed]:
        observations: dict[str, Observed] = {}
        stored_filter: Any = None
        for entry in client.tool_servers():
            if (entry.get("info") or {}).get("id") == plan.server_id:
                stored_filter = (entry.get("config") or {}).get(
                    "function_name_filter_list"
                )
                break
        observations["admin_preset:reachable_tools"] = (
            Observed(
                True,
                sorted(
                    effective_tool_binding(live_tools, str(stored_filter).split(","))
                ),
            )
            if stored_filter is not None
            else Observed.missing()
        )
        row = {str(r.get("id")): r for r in client.models_list()}.get(plan.preset_id)
        if row is None:
            for key in keys:
                observations.setdefault(key, Observed.missing())
            return observations
        meta = row.get("meta") or {}
        params = row.get("params") or {}
        observations["admin_preset:name"] = (
            Observed(True, row["name"]) if "name" in row else Observed.missing()
        )
        observations["admin_preset:params.system"] = (
            Observed(True, params["system"]) if "system" in params else Observed.missing()
        )
        observations["admin_preset:meta.toolIds"] = (
            Observed(True, list(meta["toolIds"] or []))
            if "toolIds" in meta
            else Observed.missing()
        )
        observations["admin_preset:meta.knowledge"] = (
            Observed(True, list(meta["knowledge"] or []))
            if "knowledge" in meta
            else Observed.missing()
        )
        observations["admin_preset:meta.builtinTools.knowledge"] = (
            _observed_builtin_knowledge(meta)
        )
        observations["admin_preset:grants"] = _observed_grants(row)
        observations["admin_preset:base_model_id"] = (
            Observed(True, row["base_model_id"])
            if "base_model_id" in row
            else Observed.missing()
        )
        return observations

    def write(stale: Sequence[Desired]) -> None:
        if not any(
            d.key.startswith("admin_preset:") and d.key != "admin_preset:reachable_tools"
            for d in stale
        ):
            # Only the connection drifted, and the connection is the preset
            # surface's to write. The readback below fails by name.
            return
        form = _model_form(
            model_id=plan.preset_id,
            name=name,
            base_model_id=base_model_id,
            meta={
                "description": (
                    "Source administration: list, add, remove and set up "
                    "sources. Visible to the admin group; the per-caller "
                    "check is not yet enforced."
                ),
                "toolIds": tool_ids,
                "knowledge": [],
                "builtinTools": _preset_builtin_tools(client, plan.preset_id),
                "suggestion_prompts": [],
            },
            params={"system": system_text},
            grants=grants,
        )
        existing = {str(r.get("id")) for r in client.models_list()}
        if plan.preset_id in existing:
            # F7. NEVER /api/v1/models/sync.
            client.update_model(form)
        else:
            client.create_model(form)

    _reconcile(outcome, desired, read, write)


def _verify_registered_connection(
    inputs: SyncInputs,
    client: InstanceClient,
    outcome: SurfaceOutcome,
    *,
    connection: Mapping[str, Any],
    report: "SyncReport | None",
    server_id: str | None = None,
) -> None:
    """Surface 6's credential half: the INSTANCE dials, not this process.

    Surface 6's tools/list probe runs before anything is registered and dials
    with this process's own credential, so it answers "the endpoint is up and
    serves these operators" and cannot answer "the connection the instance
    stored can reach it" — a registration with a dead credential passed it.
    This runs after the registration, on the READ connection, which is
    unconditional, so a read-only deployment measures it too.

    It verifies the DESIRED connection, not the readback. Two reasons, and
    both matter. The readback is redacted by the time any caller sees it, so
    a verify built from it would present the instance a digest instead of a
    credential; and a stored ``headers.Authorization`` would survive that
    rebuild as a digest and be dialled as one. The desired payload carries
    the clear key and ``headers: None``, which is exactly what this run
    registered. What ties the two together is the reconcile assertion above:
    the connection the instance holds matched this run's credential (by
    digest, or by presence on a version that masks it) before this dial.
    """
    # `server_id` names the LIVE connection when one is passed (#1183 S4).
    # Everything below is per-connection already; only the id and the URL
    # differ, and both come off the payload being verified.
    server_id = server_id or inputs.mcp_server_id
    live = server_id != inputs.mcp_server_id
    endpoint = str(connection.get("url") or inputs.mcp_instance_url)
    registered = any(
        (candidate.get("info") or {}).get("id") == server_id
        for candidate in client.tool_servers()
    )
    if not registered:
        # Absence is already a reconcile failure on `connection:info.id`;
        # re-raising it here would rename the same defect.
        return
    auth_type = connection.get("auth_type")
    if auth_type == "system_oauth":
        # MEASURED at v0.11.0 (vendor routers/configs.py
        # `verify_tool_servers_config`): for `system_oauth` the instance dials
        # with the OAuth session of the admin making the request -- the
        # `oauth_session_id` cookie a browser sign-in sets. An operator run
        # authenticates with a bearer token and has no such session, so the
        # instance dials credential-less and answers 400 for a server that is
        # up. That is not a measurement of the stored connection, so this run
        # does not take it: the connection is registered, the endpoint was
        # proven above with the chat client's own service token, and each
        # signed-in person's token is what verifies the connection in use.
        state = VERIFY_NOT_PROBED
        detail = (
            "the connection forwards each signed-in person's own token "
            "(chat.oidc_login); no operator credential can verify it"
        )
    else:
        state, detail = client.verify_tool_server(connection)
    if report is not None and live:
        report.mcp["live_connection_probe"] = state
        report.mcp["live_connection_verified"] = (
            None
            if state in (VERIFY_UNAVAILABLE, VERIFY_NOT_PROBED)
            else state == VERIFY_HEALTHY
        )
        report.mcp["live_connection_detail"] = detail
        report.mcp["live_connection_auth_type"] = auth_type
    elif report is not None:
        report.mcp["registered_connection_probe"] = state
        report.mcp["registered_connection_verified"] = (
            None
            if state in (VERIFY_UNAVAILABLE, VERIFY_NOT_PROBED)
            else state == VERIFY_HEALTHY
        )
        report.mcp["registered_connection_detail"] = detail
        report.mcp["registered_connection_auth_type"] = auth_type
    if state == VERIFY_NOT_PROBED:
        outcome.notes.append(
            f"registered connection {server_id}: instance-side verify NOT "
            f"PROBED ({detail}); auth_type {auth_type}"
        )
        return
    if state == VERIFY_UNAVAILABLE:
        # NOT a refusal. This Open WebUI version has no working verify route,
        # so the instance-side measurement is simply absent — and an absent
        # measurement reported as a dead credential would send an operator to
        # rotate a token that was never the problem.
        outcome.notes.append(
            f"registered connection {server_id}: the instance-side "
            f"verify probe is UNAVAILABLE on this version ({detail}), so this "
            "run did not measure whether the stored connection can reach the "
            f"endpoint; auth_type {auth_type}"
        )
        return
    outcome.notes.append(
        f"registered connection {server_id}: instance-side verify "
        f"{'succeeded' if state == VERIFY_HEALTHY else 'FAILED'} ({detail}); "
        f"auth_type {auth_type}"
    )
    if state == VERIFY_HEALTHY:
        return
    raise ChatSyncError(
        f"the instance could not use the registered connection "
        f"{server_id!r}: its own "
        "POST /api/v1/configs/tool_servers/verify answered "
        f"{detail}. Two things this run did not separate can produce that "
        f"answer, and both must be checked: the CREDENTIAL the connection "
        f"carries (auth_type {auth_type}), and the URL the INSTANCE dials, "
        f"{endpoint!r} — which is not the URL this run's own "
        f"reachability probe used, {inputs.mcp_probe_url!r}. The endpoint "
        "answered this process at the probe URL, so what is broken is on the "
        "instance's side of the connection. A chat turn on this preset would "
        + ("reach no live tool." if live else "reach no graph tool."),
        data={
            "reason": "mcp_connection_credential_rejected",
            "surface": outcome.surface,
            "auth_type": auth_type,
            "detail": detail,
            "server_id": server_id,
            "instance_endpoint": endpoint,
            "probe_endpoint": inputs.mcp_probe_url,
        },
        fix_hint=(
            "check both: export the same token `okg mcp-serve "
            "--auth-token-env` reads into the variable named by "
            + (
                "chat.mcp.live_tools.auth_token_env"
                if live
                else "chat.mcp.auth_token_env"
            )
            + ", and confirm the instance can reach "
            + f"{endpoint} from INSIDE its own container "
            "(`--mcp-instance-url`/`--mcp-host`), then re-run "
            "`okg chat sync`"
        ),
    )


def _connection_credentials(chat: Any) -> tuple[str | None, str | None]:
    """The auth type and the stored credential the connections carry.

    With ``chat.oidc_login`` (PACT chat-oidc-login) the instance forwards each
    signed-in person's own token and the entry stores no credential, so the
    static variable is neither used nor REQUIRED: demanding it would refuse a
    sync whose connection deliberately carries none. Without the block,
    nothing changes -- the credential is resolved, and a declared-but-unset
    variable still refuses.
    """
    auth_type = _mcp_connection_auth_type(chat)
    if auth_type is not None:
        return auth_type, None
    return None, _resolve_connection_credential(chat)


def _resolve_connection_credential(
    chat: Any, *, env_name: str | None = _INHERIT,
) -> str | None:
    """The MCP credential the registered connections must carry, or ``None``.

    Resolved from the ENVIRONMENT at sync time. The manifest names a
    variable; it never holds a value, and this function never returns the
    variable's name and the value together into anything rendered.

    Declared-but-unresolvable REFUSES. Falling back to ``auth_type: "none"``
    would register a connection whose every tool call gets ``401`` and whose
    only symptom is a chat that answers without the graph — which is the
    defect this slice exists to close, re-created silently.

    Called by ``sync`` BEFORE the MCP probe, because the probe resolves the
    same variable and reports every failure as ``mcp_unreachable``. The
    surface named in the refusal is the preset surface — where the credential
    is used — but nothing has been applied on any surface when it fires.
    """
    # `env_name` names the LIVE slot's own variable when one is passed. The
    # two connections carry different authority, so they resolve different
    # references; sharing this function is what makes them share the refusal
    # too, rather than the live half degrading to `auth_type: "none"` -- the
    # defect #1738 was, re-created one connection over.
    #
    # Only an UNPASSED argument inherits the graph connection's variable.
    # `chat.mcp.live_tools.auth_token_env` is optional and parses to `None`,
    # so reading `None` as "inherit" handed the live endpoint the GRAPH
    # bearer token -- a credential for a different system, sent to a live
    # tool server, while the plan declared `live_connection:auth_type:
    # "none"` for the same manifest. An unauthenticated live connection is a
    # LEGAL state (the plan says so); a live connection carrying the graph
    # token is not.
    if env_name is _INHERIT:
        env_name = getattr(getattr(chat, "mcp", None), "auth_token_env", None)
    if not env_name:
        return None
    from okg.substrate.mcp.http_auth import resolve_auth_token

    try:
        return resolve_auth_token(str(env_name))
    except SubstrateError as exc:
        cause = str((exc.data or {}).get("reason") or "unresolvable")
        raise ChatSyncError(
            "chat.mcp.auth_token_env names the environment variable holding "
            f"the MCP bearer credential, and it did not resolve ({cause}). "
            "Refusing to register the tool-server connections without it: an "
            "unauthenticated registration answers every chat tool call with "
            "401 while the site still looks configured.",
            data={
                "reason": "mcp_connection_credential_unresolved",
                "surface": SURFACE_PRESET,
                "auth_token_env": str(env_name),
                "cause": cause,
            },
            fix_hint=(
                f"export {env_name} with the same token `okg mcp-serve "
                "--auth-token-env` reads, then re-run `okg chat sync`"
            ),
        ) from exc


def _mcp_connection_auth_type(chat: Any) -> str:
    """How the instance authenticates to ``mcp-serve`` on a person's behalf.

    With ``chat.oidc_login`` the instance holds each signed-in person's own
    token and forwards it (``system_oauth``), and ``mcp-serve`` verifies it
    itself (PACT mcp-oidc-auth). Without it, today's unauthenticated
    registration is unchanged.
    """
    if getattr(chat, "oidc_login", None) is None:
        return None
    return "system_oauth"


def _connection_payload(
    *,
    url: str,
    server_id: str,
    deployment: str,
    filter_list: Sequence[str],
    grants: Sequence[Mapping[str, str]],
    description: str | None = None,
    auth_token: str | None = None,
    auth_type: str | None = None,
) -> dict[str, Any]:
    """F4's entry shape, with F2's ``info.id`` never omitted.

    ``auth_token`` is the credential resolved from the environment variable
    named by ``chat.mcp.auth_token_env`` — never a manifest value, and never
    logged. MEASURED (#1738): against an ``mcp-serve --auth-token-env``, an
    ``auth_type: "none"`` entry makes the instance dial credential-less, the
    server answers ``401``, and the vendor's verify answers ``400``; the same
    entry with ``auth_type: "bearer"`` and ``key`` set verifies with the full
    operator list. ``headers`` stays ``None`` on purpose: the vendor reads
    ``key`` directly, while ``headers`` goes through ``get_custom_headers``,
    which interpolates template variables against the requesting user — an
    extra path a static shared secret has no reason to take.

    With no token the entry keeps ``auth_type: "none"`` EXPLICITLY, which is
    the right registration for an unauthenticated endpoint and a stated
    posture rather than an omission.
    """
    return {
        "url": url,
        "path": "",
        "type": "mcp",
        "auth_type": auth_type or ("bearer" if auth_token else "none"),
        "headers": None,
        "key": "" if auth_type else (auth_token or ""),
        "config": {
            "enable": True,
            "access_grants": [dict(g) for g in grants],
            # F4: the wire the query exclusion is enforced on.
            "function_name_filter_list": ",".join(filter_list),
        },
        "info": {
            # F2. Without this the registration is `server:mcp:None`.
            "id": server_id,
            "name": f"OKG {deployment}",
            "description": description or (
                f"OKG read algebra over the {deployment} deployment"
            ),
        },
    }


def _sync_prompts(
    inputs: SyncInputs,
    client: InstanceClient,
    outcome: SurfaceOutcome,
    *,
    prompt: ComposedPrompt,
    running_version: str,
) -> None:
    driven, reason = _prompts_proof(
        data_dir=inputs.chat_data_dir, running_version=running_version,
    )
    if not driven:
        # DROPPED, with the reason named — not written blind, and not
        # silently skipped. A dropped surface is `ok` because dropping it is
        # the specified outcome; what would not be ok is writing to it.
        outcome.status = "dropped"
        outcome.refusal = reason
        outcome.ok = True
        outcome.notes.append(
            "no prompt-library entry was written; record the readback proof "
            f"at {_report_path(inputs.chat_data_dir, PROMPTS_PROOF_FILE)} to drive "
            "this surface"
        )
        return

    command = f"/okg-{inputs.deployment}"
    desired = [
        Desired(outcome.surface, "prompt:command", command),
        Desired(outcome.surface, "prompt:content", prompt.text),
        Desired(outcome.surface, "prompt:name", f"OKG {inputs.deployment}"),
    ]

    def _row() -> Mapping[str, Any] | None:
        for entry in client.prompts():
            if entry.get("command") == command:
                return entry
        return None

    def read() -> dict[str, Observed]:
        row = _row()
        if row is None:
            return {d.key: Observed.missing() for d in desired}
        return {
            "prompt:command": Observed(True, row["command"]) if "command" in row else Observed.missing(),
            "prompt:content": Observed(True, row["content"]) if "content" in row else Observed.missing(),
            "prompt:name": Observed(True, row["name"]) if "name" in row else Observed.missing(),
        }

    def write(_stale: Sequence[Desired]) -> None:
        row = _row()
        form = {
            "command": command,
            "name": f"OKG {inputs.deployment}",
            "content": prompt.text,
            "access_grants": [],
        }
        if row is None:
            client.create_prompt(form)
        else:
            client.update_prompt(str(row.get("id")), form)

    outcome.notes.append(reason)
    _reconcile(outcome, desired, read, write)


def _sync_deep_links(
    inputs: SyncInputs, client: InstanceClient, outcome: SurfaceOutcome,
) -> None:
    """Surface 5: carry the declared bases into the components that consume them.

    Reverse-proxy route configuration is NOT rendered here: it is operator
    state proven by the umbrella's ``/console`` handshake. What this surface
    owns is the second-source-of-truth problem — a valve an operator typed by
    hand can disagree with the manifest, and when it does it disagrees
    silently.

    A function that is installed but whose valve SPEC does not declare the
    field is a named refusal, not a write: ``routers/functions.py:531``
    validates the posted valves against the function's own model, so an
    unknown field is silently dropped and would read back as a knob that
    worked.
    """
    bases = inputs.chat.deep_links
    if bases is None:
        outcome.status = "not_declared"
        outcome.ok = True
        # A named refusal, not only a note: `written_surfaces_dropped` reports
        # every written surface it did not verify, and an entry whose refusal
        # is null is indistinguishable in the output from one that was never
        # considered.
        outcome.refusal = (
            "the deep_links surface is NOT DECLARED: the manifest carries no "
            "chat.deep_links block, so there is no base to carry and nothing "
            "was written"
        )
        outcome.notes.append(outcome.refusal)
        return

    # Say where each declared base went, INCLUDING the ones that went nowhere.
    # A base the operator declared and that appears in no diff is otherwise
    # indistinguishable from a base that was carried successfully.
    for base_name, why in UNCONSUMED_BASES:
        if getattr(bases, base_name, None):
            outcome.notes.append(
                f"{base_name} is declared but was carried NOWHERE: {why}"
            )

    installed = {str(f.get("id")) for f in client.functions()}
    targets: list[tuple[ValveTarget, str]] = []
    for target in DEEP_LINK_VALVE_TARGETS:
        value = getattr(bases, target.base, None)
        if not value:
            outcome.notes.append(
                f"{target.base} is not declared, so nothing was carried into "
                f"{target.function_id}.{target.valve}"
            )
            continue
        if target.function_id not in installed:
            outcome.notes.append(
                f"{target.function_id} is not installed on this instance, so "
                f"{target.base} was carried nowhere"
            )
            continue
        spec = client.function_valves_spec(target.function_id)
        properties = (spec or {}).get("properties")
        if not isinstance(properties, Mapping) or target.valve not in properties:
            raise ChatSyncError(
                f"function {target.function_id!r} is installed but its valves "
                f"spec declares no {target.valve!r} field, so writing "
                f"{target.base} into it would be silently dropped by the "
                "instance and read back as a knob that worked. Refused.",
                data={
                    "reason": "valve_not_declared",
                    "surface": SURFACE_DEEP_LINKS,
                    "function_id": target.function_id,
                    "valve": target.valve,
                    "declared": sorted(properties) if isinstance(properties, Mapping) else [],
                },
                fix_hint=(
                    f"add a {target.valve!r} valve to {target.function_id}, or "
                    "remove it from the consuming set"
                ),
            )
        targets.append((target, str(value)))

    desired = [
        Desired(outcome.surface, f"{t.function_id}.{t.valve}", value)
        for t, value in targets
    ]
    if not desired:
        outcome.status = "no_consumer"
        outcome.ok = True
        # Named, and carrying the per-base reasons already collected above, so
        # the surface is ACCOUNTED FOR in the run's written-surface arithmetic
        # rather than dropping out of both the verified and the dropped count.
        outcome.refusal = (
            "the deep_links surface has NO CONSUMER on this instance: every "
            "declared base resolved to a component that is not installed or "
            "that declares no valve reading it, so nothing was written. "
            + " ".join(outcome.notes)
        )
        return

    def read() -> dict[str, Observed]:
        observations: dict[str, Observed] = {}
        for target, _value in targets:
            valves = client.function_valves(target.function_id)
            observations[f"{target.function_id}.{target.valve}"] = (
                Observed(True, valves[target.valve])
                if target.valve in valves
                else Observed.missing()
            )
        return observations

    def write(stale: Sequence[Desired]) -> None:
        by_function: dict[str, dict[str, Any]] = {}
        for item in stale:
            function_id, _, valve = item.key.rpartition(".")
            by_function.setdefault(function_id, {})[valve] = item.value
        for function_id, updates in by_function.items():
            current = json.loads(json.dumps(client.function_valves(function_id)))
            current.update(updates)
            client.set_function_valves(function_id, current)

    _reconcile(outcome, desired, read, write)
