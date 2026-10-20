"""``okg chat setup`` — one guided entry point from an installed deployment to
a conversation that can use the graph (issue #1792).

Before this verb, a working chat took five hand steps in two vocabularies:
``okg chat-instance up``, a first account through the vendor's signup form,
an admin token exported by hand, a model provider configured in the vendor's
admin pages, and ``okg chat sync`` — which could then report green on a site
whose model could not reply. This module runs those steps in order, reusing
the existing lifecycle code for each, and adds the two checks nothing made
before: that the configured model answers, and that a conversation on the
preset actually invokes the graph tools.

Six steps, each reported by name, after an optional ``configure`` step. The
first failure stops the run, names its cause and its fix, and marks every later
step ``not_attempted``; the operator corrects the named input and re-runs the
SAME command. There is no second procedure.

0. ``configure`` -- issue #2288. When the operator passes chat answers
   (``--provider``, ``--model``, ``--mcp-port``, ...), the deployment's
   ``chat:`` block is written from them by
   :func:`okg.substrate.chat.block_scaffold.ensure_chat_block`, so no step
   needs a hand-edited ``deployment.yaml``. An existing block is never
   overwritten: agreeing answers are a no-op, disagreeing ones refuse. With no
   answers the step is ``skipped`` and the manifest is used as it is.
   ``--configure-only`` stops after this step.
1. ``instance`` — :func:`okg.substrate.chat.instance.stand_up`. Idempotent.
2. ``admin`` — the admin credential. ``OKG_CHAT_ADMIN_TOKEN`` wins when set;
   otherwise ``OKG_CHAT_ADMIN_EMAIL`` (defaulted) and
   ``OKG_CHAT_ADMIN_PASSWORD`` create the FIRST account while the instance
   has none, and sign in to it afterwards. The session token lives in this
   process's memory only. Nothing here resets an existing account: a wrong
   password refuses, it never re-creates.
3. ``provider`` — ``chat.provider`` (base URL plus the NAME of the variable
   holding the key) is verified against the provider through the vendor's
   own verify route, then registered as ONE entry in the instance's
   OpenAI-connection list, leaving every other entry untouched. Then the
   deployment's ``chat.models.default`` must appear in the instance's served
   model list.
4. ``graph`` — the deployment's mcp-serve endpoint. Dead, it is started as a
   detached child of this process (``okg mcp-serve --transport
   streamable-http``) with its log under the chat-instances state root.
5. ``sync`` — :func:`okg.substrate.chat.sync.sync`, unchanged. Green here is
   **configuration convergence**.
6. ``conversation`` — one question to the preset in a new chat the vendor
   stores, with the graph tool server listed EXPLICITLY in ``tool_ids`` (the
   pinned vendor attaches a preset's tools through the API only when the
   caller lists them). Ready only when the model replied AND the stored
   answer records a graph tool call the graph answered without refusing. That is
   **conversation readiness**, reported apart from convergence, and the exit
   code is 0 only when both hold. When
   ``chat.provider.tools`` is ``provider`` the provider brings its own tools
   and the site's tool loop never runs, so the evidence is instead the
   provider's own record of tool calls (``okg_codex.tool_calls``) with at
   least one call that ANSWERED -- reported as ``provider_reported``, a
   weaker kind of evidence than the site's own tool-result sources.

Measured against ``ghcr.io/open-webui/open-webui:v0.11.0`` (a live container,
2026-09-11):

* ``GET /api/config`` carries ``onboarding: true`` only while the instance
  has zero users; the key is ABSENT afterwards.
* ``POST /api/v1/auths/signup`` makes the first user ``admin``; later signups
  answer 403. ``POST /api/v1/auths/signin`` answers 400 to a wrong password.
  ``GET /api/v1/auths/`` answers the caller's own row (``role``); a bad
  bearer is 401.
* ``POST /openai/verify {url, key}`` answers the provider's model listing;
  a dead URL is 500 and a rejected key is the provider's 401 passed through.
  ``GET /openai/config`` / ``POST /openai/config/update`` carry the whole
  connection list, keys IN CLEAR (``POST /openai/config`` is the passthrough
  and answers 403).
* ``GET /api/models`` is the served, de-duplicated model list.
* ``POST /api/chat/completions`` runs the preset's tools only in its
  streaming handler, and only when the request names a stored chat and an
  assistant message id (issue #2907, the pinned 0.11.0 image). Function
  calling defaults to ``native`` (``main.py:1203``); the non-streaming handler
  (``utils/middleware.py:3566``) returns the model's tool calls to the caller
  unexecuted; the streaming handler runs the tool loop only with an event
  emitter (``:3783``), which exists only for a chat id plus a message id
  (``:2990``). Without a ``session_id`` the route runs that turn inline and
  answers ``null`` (``main.py:1783``). The turn is stored on the chat's
  assistant message as ``output`` items: ``function_call`` (``name`` is
  ``<tool server id>_<tool>``, ``middleware.py:2770``) and
  ``function_call_output`` (the result text, same ``call_id``,
  ``:5119``). No ``sources`` entry is made for an MCP tool (``:5060``).

Re-read against the v0.11.4 source (revision ``8bd8b4fa``) for the move to
the v0.11.4-based image. Every point above still holds, at new lines:
function calling defaults to ``native`` at ``main.py:1293``; the inline
branch is ``main.py:1883-1885``; the non-streaming handler
(``utils/middleware.py:4211``) still returns tool calls unexecuted; the
streaming loop runs only with an event emitter (``:4467``), which still
needs a chat id and a message id (``:3285``); tool names are composed at
``:2995``; results are stored as ``function_call_output`` at ``:6144``;
citation sources are still made only for five built-in tools
(``:6089-6099``); and the finished turn is stored with ``done: true``
beside its ``output`` (``:6553-6562``). Tool permissions stay off by default
(``chat.tool_permissions.enable``, ``config.py:2196``), so the approval mode
is ``full`` and tools run unasked (``main.py:1255-1263``). Three things
changed, and the first one matters here: a tool call that raises is now
stored as ``{"error": str(e)}`` (``:6027``) rather than as ``str(e)``, which
:func:`_parse_result` reads as the error form; a ``function_call_output``
now carries ``status: failed`` for an error-looking result (``:6124-6127``);
and a base64 image data URI inside a tool result is moved out into an
attached image (``extract_base64_images``, ``:1010-1050``, applied at
``:1248``) instead of reaching the model as text. OKG answers carry no data
URIs, so the last one does not reach this probe.

Secrets. The password, the session token, the provider key and any
operator-supplied token are held in memory, passed to every HTTP call as
identities to scrub, and never placed in a step, a report, an error payload,
a log line or a file. The report names VARIABLES and an email address.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from okg.substrate.chat.block_scaffold import base_url_masked_parts, masked_base_url
from okg.substrate.chat.container_network import (
    LOOPBACK_MODES,
    NETWORK_AUTO,
    NETWORK_GATEWAY,
    NETWORK_PASTA,
    loopback_url_ports,
)
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.result_text import (
    classify_status_line,
    first_text_block,
    parse_status_line,
)

# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------

#: The operator-supplied admin credential (an API key or session token). When
#: set it is used as-is and no account is created or signed into.
ENV_ADMIN_TOKEN = "OKG_CHAT_ADMIN_TOKEN"
#: The admin account's email. Identity, not a secret; defaulted per deployment.
ENV_ADMIN_EMAIL = "OKG_CHAT_ADMIN_EMAIL"
#: The admin account's password. Creates the first account while the instance
#: has none; signs in afterwards. Never stored anywhere by this program.
ENV_ADMIN_PASSWORD = "OKG_CHAT_ADMIN_PASSWORD"  # noqa: S105 - a variable NAME
ADMIN_DISPLAY_NAME = "okg operator"

STEP_CONFIGURE = "configure"
STEP_INSTANCE = "instance"
STEP_ADMIN = "admin"
STEP_PROVIDER = "provider"
STEP_MODEL = "model"
STEP_GRAPH = "graph"
STEP_SYNC = "sync"
STEP_CONVERSATION = "conversation"
STEPS: tuple[str, ...] = (
    STEP_CONFIGURE, STEP_INSTANCE, STEP_ADMIN, STEP_PROVIDER, STEP_MODEL, STEP_GRAPH,
    STEP_SYNC, STEP_CONVERSATION,
)

STATUS_OK = "ok"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUS_NOT_ATTEMPTED = "not_attempted"

CREDENTIAL_SOURCE_ENV = "env"
CREDENTIAL_SOURCE_SIGNUP = "signup"
CREDENTIAL_SOURCE_SIGNIN = "signin"

#: Why a replied conversation is still not ready.
REASON_TOOL_USE_UNOBSERVED = "tool_use_unobserved"
#: The model called graph tools and none of them returned a graph result.
REASON_GRAPH_TOOLS_FAILED = "graph_tool_calls_failed"
#: The model called graph tools and the graph refused every call it received.
REASON_GRAPH_TOOLS_REFUSED = "graph_tool_calls_refused"
#: ``chat.provider.tools: provider`` -- the provider called the graph tools
#: but none of them answered.
REASON_PROVIDER_TOOLS_FAILED = "provider_tool_calls_failed"

#: Where a provider that brings its own tools records the calls it made, in
#: its chat-completion response (``okg chat codex-shim`` writes it; Open WebUI
#: 0.11 passes an extra top-level key through unchanged -- measured
#: 2026-09-23).
PROVIDER_TOOL_RECORD_KEY = "okg_codex"
TOOL_EVIDENCE_INSTANCE = "instance_tool_result"
TOOL_EVIDENCE_PROVIDER = "provider_reported"

#: Vendor routes, each measured (module docstring).
PATH_PUBLIC_CONFIG = "/api/config"
PATH_SIGNUP = "/api/v1/auths/signup"
PATH_SIGNIN = "/api/v1/auths/signin"
PATH_WHOAMI = "/api/v1/auths/"
PATH_PROVIDER_CONFIG = "/openai/config"
PATH_PROVIDER_CONFIG_UPDATE = "/openai/config/update"
PATH_PROVIDER_VERIFY = "/openai/verify"
PATH_MODELS = "/api/models"
PATH_COMPLETIONS = "/api/chat/completions"
PATH_CHAT_NEW = "/api/v1/chats/new"
PATH_CHAT = "/api/v1/chats/{chat_id}"

MCP_SERVE_STATE_FILE = "mcp-serve.json"
MCP_SERVE_LOG_FILE = "mcp-serve.log"
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")

DEFAULT_QUESTION = (
    "Using the graph tools, what kinds of records does this deployment hold? "
    "Name the tools you called and the generation the answer comes from."
)


class ChatSetupError(SubstrateError):
    """``okg chat setup`` stopped at a named step.

    One code, with ``data['step']`` and ``data['reason']`` naming where and
    why, because the operator action is always the same: fix the named input
    and re-run the same command.
    """

    code = "chat_setup_failed"


Opener = Callable[[urllib.request.Request, float], tuple[int, str]]


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

@dataclass
class SetupStep:
    name: str
    status: str = STATUS_NOT_ATTEMPTED
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    #: Things this step CHANGED on the instance or the host, stated out loud.
    changes: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "data": dict(self.data),
            "changes": list(self.changes),
            "notes": list(self.notes),
        }


@dataclass
class SetupReport:
    deployment: str
    steps: dict[str, SetupStep] = field(
        default_factory=lambda: {name: SetupStep(name) for name in STEPS},
    )
    base_url: str | None = None
    site_url: str | None = None
    preset_id: str | None = None
    admin_email: str | None = None
    credential_source: str | None = None
    #: Steps 1-6 green: the declared configuration is applied and read back.
    converged: bool = False
    #: Step 7 green: a completion on the preset replied AND used graph tools.
    conversation_ready: bool = False
    conversation: dict[str, Any] = field(default_factory=dict)
    error: dict[str, Any] | None = None
    failed_step: str | None = None
    #: ``--configure-only``: the run ends after the configure step, and ``ok``
    #: means only that the chat: block is declared and valid.
    configure_only: bool = False

    @property
    def ok(self) -> bool:
        if self.configure_only:
            return (
                self.failed_step is None
                and self.steps[STEP_CONFIGURE].status == STATUS_OK
            )
        return self.converged and self.conversation_ready

    @property
    def next_action(self) -> str | None:
        if self.ok and self.configure_only:
            return (
                f"run `okg chat setup --deployment {self.deployment}` to start "
                "the site and prove a grounded conversation"
            )
        if self.ok:
            return None
        return (
            "fix the cause named above, then re-run "
            f"`okg chat setup --deployment {self.deployment}`"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "surface": "chat-setup",
            "deployment": self.deployment,
            "ok": self.ok,
            "converged": self.converged,
            "conversation_ready": self.conversation_ready,
            "steps": [self.steps[name].as_dict() for name in STEPS],
            "base_url": self.base_url,
            "site_url": self.site_url,
            "preset_id": self.preset_id,
            "admin_email": self.admin_email,
            "credential_source": self.credential_source,
            "conversation": dict(self.conversation),
            "failed_step": self.failed_step,
            "error": self.error,
            "next_action": self.next_action,
            "configure_only": self.configure_only,
        }


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SetupOptions:
    """Everything the CLI turns its flags into. Names, never values."""

    app_database_env: str = "OKG_CHAT_APP_DATABASE_URL"
    secret_env: str = "OKG_CHAT_WEBUI_SECRET_KEY"
    instance_port: int | None = None
    image: str | None = None
    host: str = "127.0.0.1"
    host_gateway_alias: str | None = "host.docker.internal"
    container_runtime: str = "docker"
    #: ``auto``, ``gateway``, ``pasta`` or ``host`` (#2876 item 2): how the
    #: container reaches this host's loopback. ``chat.container_network``.
    container_network: str = NETWORK_AUTO
    ready_timeout: float = 120.0
    mcp_probe_url: str | None = None
    mcp_instance_url: str | None = None
    mcp_host: str = "127.0.0.1"
    install_plan: str | None = None
    admin_email: str | None = None
    question: str | None = None
    #: Start mcp-serve when its endpoint does not answer. Off, a dead
    #: endpoint is a refusal naming the command instead.
    start_mcp_serve: bool = True
    mcp_ready_timeout: float = 60.0
    #: None starts THIS okg (running interpreter, ``-P``; issue #2301),
    #: never the first ``okg`` on PATH. A string names another CLI.
    okg_cli: str | None = None
    completion_timeout: float = 300.0
    #: The chat-block answers (issue #2288). Empty: the manifest is used as is.
    chat_answers: Any = None
    #: Stop after the configure step.
    configure_only: bool = False


# ---------------------------------------------------------------------------
# One HTTP seam, anonymous or bearer
# ---------------------------------------------------------------------------

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every 3xx: the instance is dialled at the address this command
    started it on, and a redirect elsewhere is a failure, not a hop."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirect())


def _default_opener(request: urllib.request.Request, timeout: float) -> tuple[int, str]:
    with _NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:  # noqa: S310
        return response.status, response.read().decode("utf-8")


def _dial(
    base_url: str,
    method: str,
    path: str,
    payload: Any = None,
    *,
    token: str | None,
    opener: Opener | None,
    timeout: float,
    secrets: Sequence[str | None],
) -> tuple[int, Any]:
    """One round trip that RETURNS the status rather than raising on 4xx.

    :class:`~okg.substrate.chat.sync.InstanceClient` raises on every non-2xx,
    which is right for a projection but wrong for the three routes here that
    answer a question WITH a 4xx — 400 is "wrong password", 401 is "not a
    credential", 403 is "signup closed". Transport failures still raise.
    Every body that could echo the request is scrubbed by identity.
    """
    from okg.substrate.chat.sync import payload_secrets, redact

    data = None if payload is None else json.dumps(payload).encode("utf-8")
    scrub_with = (*payload_secrets(payload), *secrets, token)

    def scrub(text: str) -> str:
        return redact(text, *scrub_with)

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}", data=data, method=method, headers=headers,
    )
    open_ = opener or _default_opener
    try:
        status, body = open_(request, timeout)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace") if exc.fp else ""
        status, body = exc.code, raw
    except (urllib.error.URLError, OSError) as exc:
        raise ChatSetupError(
            f"{method} {path} could not reach the instance at {base_url}: "
            f"{scrub(str(exc))}",
            data={"reason": "instance_unreachable", "method": method, "path": path},
            fix_hint="check that the instance is up (`okg chat-instance status`)",
        ) from exc
    parsed: Any
    try:
        parsed = json.loads(body) if body else None
    except json.JSONDecodeError:
        parsed = scrub(body)
    if status >= 400 and isinstance(parsed, str):
        parsed = scrub(parsed)
    return status, parsed


def _scrub(text: str, *secrets: str | None) -> str:
    """Replace each supplied secret by identity, longest first. No shape pass."""
    out = text
    for secret in sorted({s for s in secrets if s}, key=len, reverse=True):
        out = out.replace(secret, "<redacted>")
    return out


def _scrub_tree(node: Any, secrets: Sequence[str | None]) -> Any:
    """Sweep every string in a nested structure by identity."""
    if isinstance(node, str):
        return _scrub(node, *secrets)
    if isinstance(node, Mapping):
        return {k: _scrub_tree(v, secrets) for k, v in node.items()}
    if isinstance(node, (list, tuple)):
        return [_scrub_tree(v, secrets) for v in node]
    return node


def _detail_text(parsed: Any, *secrets: str | None) -> str:
    """The vendor's ``detail`` (or error message), redacted and bounded."""
    from okg.substrate.chat.sync import redact

    if isinstance(parsed, Mapping):
        detail = parsed.get("detail")
        if detail is None and isinstance(parsed.get("error"), Mapping):
            detail = parsed["error"].get("message")
        text = json.dumps(detail, default=str) if not isinstance(detail, str) else detail
    else:
        text = "" if parsed is None else str(parsed)
    return redact(text, *secrets)


# ---------------------------------------------------------------------------
# Step 2: the admin credential
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AdminAccess:
    """The credential the rest of the run dials with. ``token`` never renders."""

    token: str = field(repr=False)
    email: str | None
    source: str
    account_created: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "email": self.email,
            "source": self.source,
            "account_created": self.account_created,
        }


def default_admin_email(deployment: str) -> str:
    """Passes the vendor's ``[^@]+@[^@]+\\.[^@]+`` check for any deployment name."""
    return f"okg-admin@{deployment}.invalid"


def _whoami(
    base_url: str, token: str, *, opener: Opener | None, timeout: float,
    secrets: Sequence[str | None],
) -> tuple[int, Any]:
    return _dial(
        base_url, "GET", PATH_WHOAMI, token=token, opener=opener,
        timeout=timeout, secrets=secrets,
    )


def _require_admin_row(
    status: int, row: Any, *, source: str, base_url: str, secrets: Sequence[str | None],
) -> str | None:
    """The row's email once it is a 200 with ``role == admin``; else refuse."""
    if status == 401 or status == 403:
        raise ChatSetupError(
            f"the instance at {base_url} rejected the admin credential from "
            f"{source} (HTTP {status}).",
            data={
                "reason": "admin_credential_rejected",
                "step": STEP_ADMIN,
                "source": source,
                "http_status": status,
            },
            fix_hint=(
                f"unset {ENV_ADMIN_TOKEN} to sign in with {ENV_ADMIN_EMAIL} and "
                f"{ENV_ADMIN_PASSWORD} instead, or export a credential this "
                "instance accepts; nothing was changed"
                if source == ENV_ADMIN_TOKEN
                else "check the account, then re-run"
            ),
        )
    if status >= 400 or not isinstance(row, Mapping):
        raise ChatSetupError(
            f"GET {PATH_WHOAMI} did not identify the caller (HTTP {status}): "
            f"{_detail_text(row, *secrets)}",
            data={
                "reason": "admin_identity_unreadable",
                "step": STEP_ADMIN,
                "http_status": status,
            },
            fix_hint="check that the instance is Open WebUI and healthy",
        )
    role = row.get("role")
    if role != "admin":
        raise ChatSetupError(
            f"the credential from {source} identifies a user with role "
            f"{role!r}, not an admin. Setup configures the instance, which "
            "only an admin may do.",
            data={
                "reason": "admin_credential_not_admin",
                "step": STEP_ADMIN,
                "source": source,
                "role": role,
            },
            fix_hint="use an admin account's credential",
        )
    email = row.get("email")
    return str(email) if isinstance(email, str) else None


def resolve_admin_access(
    base_url: str,
    deployment: str,
    *,
    environ: Mapping[str, str] | None = None,
    opener: Opener | None = None,
    timeout: float = 60.0,
    admin_email: str | None = None,
) -> AdminAccess:
    """The admin credential, in this order, refusing by name at each gap.

    1. ``OKG_CHAT_ADMIN_TOKEN`` — verified, never minted, never stored.
    2. While the instance has NO users (``onboarding``): create the first
       account from ``OKG_CHAT_ADMIN_EMAIL`` (defaulted) and
       ``OKG_CHAT_ADMIN_PASSWORD``. The vendor makes it admin.
    3. Otherwise sign in with the same two variables.

    A second run never reaches step 2: the instance has a user, so the same
    password signs in and nothing is re-created. A wrong password is a
    refusal that names the variable, not a reset.
    """
    env = os.environ if environ is None else environ
    token = (env.get(ENV_ADMIN_TOKEN) or "").strip()
    password = env.get(ENV_ADMIN_PASSWORD) or ""
    email = (
        admin_email
        or (env.get(ENV_ADMIN_EMAIL) or "").strip()
        or default_admin_email(deployment)
    )
    secrets: tuple[str | None, ...] = (token or None, password or None)

    if token:
        status, row = _whoami(
            base_url, token, opener=opener, timeout=timeout, secrets=secrets,
        )
        found = _require_admin_row(
            status, row, source=ENV_ADMIN_TOKEN, base_url=base_url, secrets=secrets,
        )
        return AdminAccess(token=token, email=found, source=CREDENTIAL_SOURCE_ENV)

    status, public = _dial(
        base_url, "GET", PATH_PUBLIC_CONFIG, token=None, opener=opener,
        timeout=timeout, secrets=secrets,
    )
    if status >= 400 or not isinstance(public, Mapping):
        raise ChatSetupError(
            f"GET {PATH_PUBLIC_CONFIG} did not answer (HTTP {status}), so "
            "whether the instance has any account yet is UNKNOWN. Refused "
            "rather than guessed.",
            data={
                "reason": "public_config_unreadable",
                "step": STEP_ADMIN,
                "http_status": status,
            },
            fix_hint="check that the instance is Open WebUI and healthy",
        )
    onboarding = public.get("onboarding") is True

    if not password:
        if onboarding:
            raise ChatSetupError(
                f"the instance at {base_url} has no account yet, and "
                f"{ENV_ADMIN_PASSWORD} is unset. Setup creates the first "
                f"(admin) account from {ENV_ADMIN_EMAIL} and "
                f"{ENV_ADMIN_PASSWORD}; it does not invent a password.",
                data={
                    "reason": "admin_credential_missing",
                    "step": STEP_ADMIN,
                    "instance_has_accounts": False,
                    "email": email,
                },
                fix_hint=(
                    f"export {ENV_ADMIN_PASSWORD} (and optionally "
                    f"{ENV_ADMIN_EMAIL}; default {email}), then re-run"
                ),
            )
        raise ChatSetupError(
            f"the instance at {base_url} already has accounts, and no admin "
            f"credential is available: {ENV_ADMIN_TOKEN} is unset and so is "
            f"{ENV_ADMIN_PASSWORD}.",
            data={
                "reason": "admin_credential_missing",
                "step": STEP_ADMIN,
                "instance_has_accounts": True,
                "email": email,
            },
            fix_hint=(
                f"export {ENV_ADMIN_TOKEN} (an admin API key or session "
                f"token), or {ENV_ADMIN_EMAIL} and {ENV_ADMIN_PASSWORD} for an "
                "existing admin account, then re-run"
            ),
        )

    if onboarding:
        status, row = _dial(
            base_url, "POST", PATH_SIGNUP,
            {"name": ADMIN_DISPLAY_NAME, "email": email, "password": password},
            token=None, opener=opener, timeout=timeout, secrets=secrets,
        )
        if status >= 400 or not isinstance(row, Mapping):
            raise ChatSetupError(
                f"creating the first account {email!r} failed (HTTP "
                f"{status}): {_detail_text(row, *secrets)}",
                data={
                    "reason": "admin_signup_failed",
                    "step": STEP_ADMIN,
                    "http_status": status,
                    "email": email,
                },
                fix_hint=(
                    "check the email format and the vendor's password rules, "
                    "then re-run"
                ),
            )
        session = row.get("token")
        if not isinstance(session, str) or not session:
            raise ChatSetupError(
                "the signup answered without a session token.",
                data={"reason": "admin_signup_no_token", "step": STEP_ADMIN},
                fix_hint="check that the instance is Open WebUI v0.11.0",
            )
        secrets = (*secrets, session)
        status, row = _whoami(
            base_url, session, opener=opener, timeout=timeout, secrets=secrets,
        )
        _require_admin_row(
            status, row, source="the new account", base_url=base_url, secrets=secrets,
        )
        return AdminAccess(
            token=session, email=email, source=CREDENTIAL_SOURCE_SIGNUP,
            account_created=True,
        )

    status, row = _dial(
        base_url, "POST", PATH_SIGNIN, {"email": email, "password": password},
        token=None, opener=opener, timeout=timeout, secrets=secrets,
    )
    if status in (400, 401, 403):
        raise ChatSetupError(
            f"the instance at {base_url} did not accept the sign-in for "
            f"{email!r} (HTTP {status}). Setup never resets an existing "
            "account's password.",
            data={
                "reason": "admin_credential_rejected",
                "step": STEP_ADMIN,
                "source": f"{ENV_ADMIN_EMAIL}+{ENV_ADMIN_PASSWORD}",
                "http_status": status,
                "email": email,
            },
            fix_hint=(
                f"export the password of the existing admin account {email!r} "
                f"in {ENV_ADMIN_PASSWORD} (or its email in {ENV_ADMIN_EMAIL}), "
                f"or export {ENV_ADMIN_TOKEN}; then re-run"
            ),
        )
    if status >= 400 or not isinstance(row, Mapping):
        raise ChatSetupError(
            f"POST {PATH_SIGNIN} failed (HTTP {status}): "
            f"{_detail_text(row, *secrets)}",
            data={
                "reason": "admin_signin_failed",
                "step": STEP_ADMIN,
                "http_status": status,
            },
            fix_hint="check that the instance is Open WebUI and healthy",
        )
    session = row.get("token")
    if not isinstance(session, str) or not session:
        raise ChatSetupError(
            "the sign-in answered without a session token.",
            data={"reason": "admin_signin_no_token", "step": STEP_ADMIN},
            fix_hint="check that the instance is Open WebUI v0.11.0",
        )
    secrets = (*secrets, session)
    status, who = _whoami(
        base_url, session, opener=opener, timeout=timeout, secrets=secrets,
    )
    _require_admin_row(
        status, who, source=f"{ENV_ADMIN_EMAIL}+{ENV_ADMIN_PASSWORD}",
        base_url=base_url, secrets=secrets,
    )
    return AdminAccess(token=session, email=email, source=CREDENTIAL_SOURCE_SIGNIN)


# ---------------------------------------------------------------------------
# Step 3: the provider, and the model it must serve
# ---------------------------------------------------------------------------

def normalize_provider_url(url: str) -> str:
    """The comparison form: lowercase scheme and host, no trailing slash."""
    split = urllib.parse.urlsplit(url.strip())
    netloc = split.netloc.lower()
    path = split.path.rstrip("/")
    return urllib.parse.urlunsplit((split.scheme.lower(), netloc, path, split.query, ""))


def _provider_config(
    client: Any, *, key: str, extra_secrets: Sequence[str] = (),
) -> dict[str, Any]:
    payload = client.call("GET", PATH_PROVIDER_CONFIG, secrets=(key, *extra_secrets))
    if not isinstance(payload, Mapping):
        raise ChatSetupError(
            f"GET {PATH_PROVIDER_CONFIG} did not return an object; the "
            "provider connections cannot be read back.",
            data={"reason": "provider_config_unreadable", "step": STEP_PROVIDER},
            fix_hint="check the admin credential",
        )
    urls = payload.get("OPENAI_API_BASE_URLS")
    keys = payload.get("OPENAI_API_KEYS")
    configs = payload.get("OPENAI_API_CONFIGS")
    if not isinstance(urls, list) or not isinstance(keys, list) or len(urls) != len(keys):
        raise ChatSetupError(
            f"GET {PATH_PROVIDER_CONFIG} answered with URL and key lists of "
            "different shapes; refusing to rewrite what cannot be read.",
            data={
                "reason": "provider_config_unreadable",
                "step": STEP_PROVIDER,
                "urls": len(urls) if isinstance(urls, list) else None,
                "keys": len(keys) if isinstance(keys, list) else None,
            },
            fix_hint="repair the connection list in the vendor admin pages",
        )
    return {
        "ENABLE_OPENAI_API": bool(payload.get("ENABLE_OPENAI_API")),
        "OPENAI_API_BASE_URLS": [str(u) for u in urls],
        "OPENAI_API_KEYS": ["" if k is None else str(k) for k in keys],
        "OPENAI_API_CONFIGS": dict(configs) if isinstance(configs, Mapping) else {},
    }


def converge_provider(
    client: Any,
    chat: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> SetupStep:
    """Verify ``chat.provider`` against the provider, then register it once.

    The instance's connection list is a LIST the vendor replaces whole, so
    this reads it, changes exactly the entry for the declared base URL
    (matched after :func:`normalize_provider_url`; two matches refuse), and
    writes the rest back byte-identical — proved by readback. A key that
    differs from the one already registered is replaced and REPORTED as a
    change, never silently.
    """
    from okg.substrate.chat.sync import ChatSyncError

    step = SetupStep(STEP_PROVIDER)
    provider = getattr(chat, "provider", None)
    if provider is None:
        step.status = STATUS_SKIPPED
        step.detail = (
            "chat.provider is not declared; the instance's existing "
            "connections are used as they are"
        )
        return step

    # Every rendering of the URL goes through masked_base_url (#2344); the
    # raw value is only compared with and written to the instance.
    shown = masked_base_url(provider.base_url)
    url_secrets = tuple(base_url_masked_parts(provider.base_url))
    env = os.environ if environ is None else environ
    key = ""
    if provider.api_key_env:
        key = env.get(provider.api_key_env) or ""
        if not key.strip():
            raise ChatSetupError(
                f"chat.provider.api_key_env names {provider.api_key_env}, and it "
                "is unset or empty. The provider credential is operator "
                "environment; setup does not register the connection without it.",
                data={
                    "reason": "provider_credential_unresolved",
                    "step": STEP_PROVIDER,
                    "api_key_env": provider.api_key_env,
                    "base_url": shown,
                },
                fix_hint=f"export {provider.api_key_env}=<the provider key>, then re-run",
            )

    # Verify BEFORE registering: a rejected or unreachable provider must not
    # become a configured-looking connection that answers nothing.
    try:
        listing = client.call(
            "POST", PATH_PROVIDER_VERIFY,
            {"url": provider.base_url, "key": key}, secrets=(key, *url_secrets),
        )
    except ChatSyncError as exc:
        http_status = exc.data.get("http_status")
        rejected = http_status in (401, 403)
        raise ChatSetupError(
            f"the provider at {shown} "
            + (
                f"rejected the credential named by "
                f"chat.provider.api_key_env={provider.api_key_env} (HTTP {http_status})"
                if rejected
                else f"did not answer the verify probe ({exc.message})"
            )
            + ". Nothing was registered.",
            data={
                "reason": (
                    "provider_credential_rejected" if rejected else "provider_unreachable"
                ),
                "step": STEP_PROVIDER,
                "base_url": shown,
                "api_key_env": provider.api_key_env,
                "http_status": http_status,
                "detail": exc.data.get("detail"),
            },
            fix_hint=(
                f"export a valid key in {provider.api_key_env}, then re-run"
                if rejected
                else (
                    "check that the provider is running and reachable FROM THE "
                    "CONTAINER at chat.provider.base_url (a host-local server is "
                    "http://host.docker.internal:<port>/v1, not 127.0.0.1), then re-run"
                )
            ),
        ) from exc
    served = [
        str(m.get("id"))
        for m in (listing.get("data") if isinstance(listing, Mapping) else []) or []
        if isinstance(m, Mapping) and m.get("id")
    ]

    current = _provider_config(client, key=key, extra_secrets=url_secrets)
    wanted = normalize_provider_url(provider.base_url)
    matches = [
        index for index, url in enumerate(current["OPENAI_API_BASE_URLS"])
        if normalize_provider_url(url) == wanted
    ]
    if len(matches) > 1:
        raise ChatSetupError(
            f"the instance registers {shown} {len(matches)} times; "
            "setup cannot tell which entry the deployment means.",
            data={
                "reason": "provider_entry_ambiguous",
                "step": STEP_PROVIDER,
                "base_url": shown,
                "indexes": matches,
            },
            fix_hint="remove the duplicate connection in the vendor admin pages, then re-run",
        )

    desired = {
        "ENABLE_OPENAI_API": True,
        "OPENAI_API_BASE_URLS": list(current["OPENAI_API_BASE_URLS"]),
        "OPENAI_API_KEYS": list(current["OPENAI_API_KEYS"]),
        "OPENAI_API_CONFIGS": {
            str(k): (dict(v) if isinstance(v, Mapping) else v)
            for k, v in current["OPENAI_API_CONFIGS"].items()
        },
    }
    if matches:
        index = matches[0]
        existing_url = desired["OPENAI_API_BASE_URLS"][index]
        existing_key = desired["OPENAI_API_KEYS"][index]
        # The URL already registered may carry what the declared one does
        # not (a query, a key-shaped part); a vendor error that echoes the
        # connection list must not print it either.
        url_secrets = (*url_secrets, *base_url_masked_parts(existing_url))
        if existing_key != key:
            step.changes.append(
                f"the credential registered for {shown} was "
                f"replaced by the value of {provider.api_key_env or '<none>'}"
            )
        if existing_url != provider.base_url:
            step.changes.append(
                f"the registered URL for connection {index} was rewritten "
                f"from {masked_base_url(existing_url)} to the declared {shown}"
            )
        desired["OPENAI_API_BASE_URLS"][index] = provider.base_url
        desired["OPENAI_API_KEYS"][index] = key
    else:
        index = len(desired["OPENAI_API_BASE_URLS"])
        desired["OPENAI_API_BASE_URLS"].append(provider.base_url)
        desired["OPENAI_API_KEYS"].append(key)
        step.changes.append(f"registered {shown} as connection {index}")
    entry_config = dict(desired["OPENAI_API_CONFIGS"].get(str(index)) or {})
    # Set `enable` only when it is not already truthy, so an entry the vendor
    # stored without the key (its default) is not rewritten -- and the change
    # is reported when it is made, never a silent write.
    if not entry_config.get("enable"):
        entry_config["enable"] = True
        if matches:
            step.changes.append(f"enabled connection {index}")
    desired["OPENAI_API_CONFIGS"][str(index)] = entry_config
    if not current["ENABLE_OPENAI_API"]:
        step.changes.append("enabled the OpenAI-compatible connection type")

    if desired != current:
        client.call("POST", PATH_PROVIDER_CONFIG_UPDATE, desired, secrets=(key, *url_secrets))
        after = _provider_config(client, key=key, extra_secrets=url_secrets)
        if after != desired:
            drift = [
                name for name in desired
                if after.get(name) != desired[name]
            ]
            raise ChatSetupError(
                f"the provider connection list did not read back as written "
                f"(differs in {drift}).",
                data={
                    "reason": "provider_readback_mismatch",
                    "step": STEP_PROVIDER,
                    "differs": drift,
                },
                fix_hint="see the instance log, then re-run",
            )
        step.detail = f"registered {shown} as connection {index} and read it back"
    else:
        step.detail = f"{shown} is already connection {index}; nothing written"
    step.status = STATUS_OK
    step.data = {
        "base_url": shown,
        "index": index,
        "api_key_env": provider.api_key_env,
        "credential": "set" if key else "none",
        "provider_models": served[:50],
        "provider_model_count": len(served),
    }
    return step


def check_model_ready(client: Any, chat: Any) -> SetupStep:
    """``chat.models.default`` must be a model the instance SERVES right now."""
    step = SetupStep(STEP_MODEL)
    models = getattr(chat, "models", None)
    default = getattr(models, "default", None) if models is not None else None
    if not default:
        raise ChatSetupError(
            "chat.models.default is not declared, so there is no provider "
            "model for the preset to run on.",
            data={"reason": "default_model_undeclared", "step": STEP_MODEL},
            fix_hint="declare chat.models.default (a member of chat.models.builtin)",
        )
    listing = client.call("GET", PATH_MODELS)
    rows = listing.get("data") if isinstance(listing, Mapping) else None
    served = [
        str(m.get("id")) for m in (rows or [])
        if isinstance(m, Mapping) and m.get("id")
    ]
    if default not in served:
        raise ChatSetupError(
            f"the instance serves no model {default!r} (chat.models.default). "
            f"It serves {len(served)} model(s)"
            + (f": {sorted(served)[:20]}" if served else "")
            + ". A preset on a model the provider does not serve cannot reply.",
            data={
                "reason": "provider_model_missing",
                "step": STEP_MODEL,
                "default": default,
                "served": sorted(served)[:50],
            },
            fix_hint=(
                "declare chat.provider with a base_url that serves this model, "
                "or set chat.models.default to a served id, then re-run"
            ),
        )
    step.status = STATUS_OK
    step.detail = f"{default!r} is served by the instance"
    step.data = {"default": default, "served_count": len(served)}
    return step


# ---------------------------------------------------------------------------
# Step 4: the graph endpoint
# ---------------------------------------------------------------------------

Launcher = Callable[[Sequence[str], Path], int]


def _default_launcher(argv: Sequence[str], log_path: Path) -> int:
    """Start a detached child; stdout and stderr go to ``log_path``."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        child = subprocess.Popen(  # noqa: S603 - argv list, shell=False
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    return child.pid


def mcp_serve_argv(
    deployment: str, *, port: int, host: str, auth_token_env: str | None,
    okg_cli: str | None = None, allowed_hosts: Sequence[str] = (),
) -> list[str]:
    """The exact command an operator would type. Names only.

    ``okg_cli=None`` is the running okg through :func:`running_okg_argv`
    (issue #2301): a bare ``okg`` would start whatever install is first on
    PATH, with other code than the one running this setup.

    ``allowed_hosts`` carries the hostname the INSTANCE dials — a container
    reaches a host-loopback server through its gateway alias, and mcp-serve's
    DNS-rebinding guard answers ``421 Misdirected Request`` to any ``Host``
    header it was not told about (measured: the instance's own
    ``tool_servers/verify`` reported HTTP 400 over exactly that 421).
    """
    prefix = running_okg_argv() if okg_cli is None else [okg_cli]
    argv = [
        *prefix, "mcp-serve", "--deployment", deployment,
        "--transport", "streamable-http", "--host", host, "--port", str(port),
    ]
    for allowed in allowed_hosts:
        argv += ["--allowed-host", allowed]
    if auth_token_env:
        argv += ["--auth-token-env", auth_token_env]
    return argv


def instance_mcp_url(
    probe_url: str, *, declared: str | None, host_gateway_alias: str | None,
) -> tuple[str, str | None]:
    """``(the URL the instance is registered with, the note explaining it)``.

    The probe URL is where THIS process dials. A container cannot reach the
    host's loopback at ``127.0.0.1`` — that is the container itself — so a
    loopback probe URL is rewritten to the gateway alias for the instance,
    and the rewrite is stated. An explicit ``--mcp-instance-url`` wins.
    """
    if declared:
        return declared, None
    split = urllib.parse.urlsplit(probe_url)
    if not host_gateway_alias or split.hostname not in _LOOPBACK_HOSTS:
        return probe_url, None
    port_part = f":{split.port}" if split.port else ""
    rewritten = urllib.parse.urlunsplit((
        split.scheme, f"{host_gateway_alias}{port_part}",
        split.path, split.query, split.fragment,
    ))
    note = (
        f"the instance is registered with {rewritten}: a container reaches "
        f"the host's loopback through {host_gateway_alias}, not 127.0.0.1"
    )
    return rewritten, note


def _probe_loopback_ports(probe_url: str | None) -> tuple[int, ...]:
    """The port of a loopback ``--mcp-probe-url``: the container must reach
    it too, so the Linux network modes forward it."""
    if not probe_url:
        return ()
    split = urllib.parse.urlsplit(probe_url)
    try:
        port = split.port
    except ValueError:
        return ()
    if split.hostname not in _LOOPBACK_HOSTS or not port:
        return ()
    return (port,)


def _setup_forward_ports(opts: SetupOptions) -> tuple[int, ...]:
    """Host-loopback ports setup knows the container dials beyond the
    manifest: a loopback ``--mcp-probe-url``, and a ``--mcp-instance-url``
    naming the loopback or the gateway alias."""
    ports = set(_probe_loopback_ports(opts.mcp_probe_url))
    ports.update(loopback_url_ports([opts.mcp_instance_url], opts.host_gateway_alias))
    return tuple(sorted(ports))


def _graph_endpoint_path(deployment: str, state_root: Path | str | None) -> Path:
    from okg.substrate.chat.instance import GRAPH_ENDPOINT_STATE_FILE, _state_root

    return _state_root(state_root) / deployment / GRAPH_ENDPOINT_STATE_FILE


def _same_base_url(left: str | None, right: str | None) -> bool:
    """Two instance base URLs name the same listener (loopback spellings,
    case and a trailing slash aside)."""
    if not left or not right:
        return False

    def norm(url: str) -> tuple[str, str, int | None, str]:
        split = urllib.parse.urlsplit(url.strip())
        host = (split.hostname or "").lower()
        # A container published on the wildcard (`up --host 0.0.0.0`,
        # recorded as such) is dialled on the host's loopback.
        if host in (*_LOOPBACK_HOSTS, "0.0.0.0", "::"):
            host = "127.0.0.1"
        return split.scheme.lower(), host, split.port, split.path.rstrip("/")

    try:
        return norm(left) == norm(right)
    except ValueError:
        return False


def record_graph_endpoint(
    deployment: str, *, probe_url: str, instance_url: str,
    instance_base_url: str, state_root: Path | str | None = None,
) -> Path:
    """Record the graph URL setup registers with the instance at
    ``instance_base_url``. URLs only."""
    path = _graph_endpoint_path(deployment, state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "deployment": deployment,
        "probe_url": probe_url,
        "instance_url": instance_url,
        "instance_base_url": instance_base_url,
        "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _container_reaches_loopback(container: Any, probe_url: str) -> bool:
    """The recorded container dials ``probe_url``'s host-loopback port as
    127.0.0.1: any port on the host network, a forwarded one under pasta."""
    mode = getattr(container, "network_mode", NETWORK_GATEWAY)
    if mode not in LOOPBACK_MODES:
        return False
    if mode != NETWORK_PASTA:
        return True
    ports = _probe_loopback_ports(probe_url)
    forwarded = set(getattr(container, "forwarded_ports", ()) or ())
    return bool(ports) and set(ports) <= forwarded


def resolve_sync_instance_url(
    deployment: str, *, probe_url: str, instance_base_url: str | None,
    state_root: Path | str | None = None,
) -> tuple[str | None, str | None]:
    """``(graph URL to register, note)`` for a sync run given no
    ``--mcp-instance-url`` (issue #2876 item 1).

    ``instance_base_url`` is the instance the run talks to
    (``OKG_CHAT_INSTANCE_URL``); ``None`` when the verb talks to none
    (``okg chat plan`` without the variable). The deployment's managed
    container is TARGETED when it is recorded and either no instance URL is
    known or that URL is the container's own host port.

    * the container is targeted, and setup recorded a URL for this probe URL
      and this same instance: reuse it, and say so;
    * the container is targeted and the probe URL is loopback: refuse BEFORE
      anything is written, because the container cannot reach the host's
      127.0.0.1;
    * a container is recorded, the run talks to ANOTHER address (a front
      door, a LAN name, another instance) and the probe URL is loopback:
      refuse too, naming both, since which instance it is cannot be told;
    * otherwise ``(None, None)``: the probe URL is registered as before (no
      container recorded -- a record left from one that is gone is removed
      with it -- or a non-loopback probe URL for another instance).
    ``0.0.0.0`` and ``::`` in the container's record match the loopback.
    """
    from okg.substrate.chat.instance import read_instance
    from okg.substrate.chat.sync import ChatSyncError

    container = read_instance(deployment, state_root=state_root)
    if container is None:
        return None, None
    from okg.substrate.chat.instance import instance_base_url as base_url_for

    container_url = base_url_for(container.host, container.port)
    path = _graph_endpoint_path(deployment, state_root)
    loopback_probe = urllib.parse.urlsplit(probe_url).hostname in _LOOPBACK_HOSTS

    def read_record() -> Any:
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ChatSyncError(
                f"the graph URL record {path} could not be read ({exc}); "
                "refused rather than registering a guessed URL.",
                data={
                    "reason": "graph_endpoint_record_unreadable",
                    "deployment": deployment,
                    "path": str(path),
                },
                fix_hint=(
                    "pass --mcp-instance-url, or re-run `okg chat setup "
                    f"--deployment {deployment}` to rewrite the record"
                ),
            ) from exc

    if instance_base_url is not None and not _same_base_url(
        instance_base_url, container_url,
    ):
        if not loopback_probe:
            return None, None
        # Another address for the instance (a front door, a LAN name) or
        # another instance altogether: which one cannot be told from here,
        # and the loopback probe URL is unreachable from a container.
        # The refusal stands either way; an unreadable record only means
        # there is no saved URL to quote, and the data says so.
        try:
            saved = read_record()
            saved_unreadable = False
        except ChatSyncError:
            saved, saved_unreadable = None, True
        saved_url = (
            saved.get("instance_url")
            if isinstance(saved, Mapping) and saved.get("probe_url") == probe_url
            and isinstance(saved.get("instance_url"), str)
            else None
        )
        raise ChatSyncError(
            f"no --mcp-instance-url was given, the graph's probe URL "
            f"{probe_url} is the host's loopback, and the instance this run "
            f"talks to ({instance_base_url}) is not deployment {deployment!r}'s "
            f"recorded container ({container_url}). Registering the loopback "
            "URL would break a container's graph connection. Nothing was "
            "written.",
            data={
                "reason": "mcp_instance_url_unresolved",
                "deployment": deployment,
                "probe_url": probe_url,
                "instance_url": instance_base_url,
                "container_url": container_url,
                "saved_instance_url": saved_url,
                "saved_record_unreadable": saved_unreadable,
                "record": str(path),
            },
            fix_hint=(
                "pass --mcp-instance-url with the graph URL that instance "
                "reaches (the probe URL itself when it runs on this host)"
                + (
                    f"; for the container, okg chat setup registered {saved_url}"
                    if saved_url
                    else ""
                )
            ),
        )

    recorded = read_record()
    if (
        isinstance(recorded, Mapping)
        and recorded.get("probe_url") == probe_url
        and isinstance(recorded.get("instance_url"), str)
        and recorded["instance_url"]
        and _same_base_url(recorded.get("instance_base_url"), container_url)
        and (
            instance_base_url is None
            or _same_base_url(recorded.get("instance_base_url"), instance_base_url)
        )
    ):
        url = recorded["instance_url"]
        return url, (
            f"the instance is registered with {url}, the graph URL "
            f"`okg chat setup` recorded in {path}; pass --mcp-instance-url "
            "to use another"
        )
    if loopback_probe and _container_reaches_loopback(container, probe_url):
        return probe_url, (
            f"the instance is registered with {probe_url}: deployment "
            f"{deployment!r}'s container runs in the "
            f"{container.network_mode} network mode and dials the host's "
            "loopback as 127.0.0.1"
        )
    if loopback_probe:
        raise ChatSyncError(
            f"no --mcp-instance-url was given and no graph URL is recorded for "
            f"{probe_url}, which is the host's loopback. Deployment "
            f"{deployment!r}'s instance runs in a container, which cannot "
            "reach the host's 127.0.0.1, so registering it would break the "
            "graph connection. Nothing was written.",
            data={
                "reason": "mcp_instance_url_unresolved",
                "deployment": deployment,
                "probe_url": probe_url,
                "record": str(path),
            },
            fix_hint=(
                "pass --mcp-instance-url with a URL the container reaches, or "
                f"re-run `okg chat setup --deployment {deployment}`, which "
                "records one. host.docker.internal does not reach a service "
                "bound to the host's 127.0.0.1 on Linux"
            ),
        )
    return None, None


def ensure_graph_endpoint(
    deployment: str,
    chat: Any,
    *,
    options: SetupOptions,
    environ: Mapping[str, str] | None = None,
    state_root: Path | str | None = None,
    probe: Callable[[str], Any] | None = None,
    launcher: Launcher | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> SetupStep:
    """The mcp-serve endpoint answers, or is started here and then answers.

    "Answers" is the transport observation :func:`probe_mcp_endpoint` makes;
    the tools/list round trip with the registered credential is ``sync``'s
    own MCP surface, which runs next and is not restated here.
    """
    from okg.substrate.chat.instance import (
        _state_root,
        mcp_endpoint_for,
        probe_mcp_endpoint,
    )

    step = SetupStep(STEP_GRAPH)
    env = os.environ if environ is None else environ
    mcp = getattr(chat, "mcp", None)
    port = getattr(mcp, "port", None) if mcp is not None else None
    endpoint = options.mcp_probe_url or mcp_endpoint_for(chat, host=options.mcp_host)
    if not endpoint or not port:
        missing = "chat.mcp.port" if not port else "a probe endpoint"
        raise ChatSetupError(
            f"deployment {deployment!r} has no graph endpoint to connect: "
            f"{missing} is missing and the endpoint is never guessed. "
            "chat.mcp.port is also what mcp-serve is started on, so it is "
            "required even when --mcp-probe-url is given.",
            data={"reason": "mcp_port_undeclared", "step": STEP_GRAPH},
            fix_hint="declare chat.mcp.port, then re-run",
        )
    auth_env = getattr(mcp, "auth_token_env", None)
    registered, _note = instance_mcp_url(
        endpoint, declared=options.mcp_instance_url,
        host_gateway_alias=options.host_gateway_alias,
    )
    # mcp-serve's DNS-rebinding guard answers 421 to any Host header it was
    # not told to permit — INCLUDING the loopback host-and-port THIS process
    # dials and the gateway alias the container dials. Both are named with a
    # wildcard port (measured: a bare 127.0.0.1:PORT is refused otherwise).
    allowed_hosts = []
    for candidate in (
        urllib.parse.urlsplit(endpoint).hostname,
        urllib.parse.urlsplit(registered).hostname,
    ):
        entry = f"{candidate}:*"
        if candidate and entry not in allowed_hosts:
            allowed_hosts.append(entry)
    argv = mcp_serve_argv(
        deployment, port=int(port), host=options.mcp_host,
        auth_token_env=auth_env, okg_cli=options.okg_cli,
        allowed_hosts=allowed_hosts,
    )
    observe = probe or (lambda url: probe_mcp_endpoint(url))
    observation = observe(endpoint)
    if not observation.dead:
        step.status = STATUS_OK
        step.detail = f"mcp-serve answers at {endpoint}"
        step.data = {"endpoint": endpoint, "started": False, "command": argv}
        return step

    if auth_env and not (env.get(auth_env) or "").strip():
        raise ChatSetupError(
            f"mcp-serve is not answering at {endpoint}, and starting it needs "
            f"the credential named by chat.mcp.auth_token_env={auth_env}, which "
            "is unset. Refused rather than started unauthenticated.",
            data={
                "reason": "mcp_credential_unresolved",
                "step": STEP_GRAPH,
                "endpoint": endpoint,
                "auth_token_env": auth_env,
            },
            fix_hint=f"export {auth_env}=<a long random token>, then re-run",
        )
    if not options.start_mcp_serve:
        raise ChatSetupError(
            f"mcp-serve is not answering at {endpoint} ({observation.detail}).",
            data={
                "reason": "mcp_endpoint_dead",
                "step": STEP_GRAPH,
                "endpoint": endpoint,
                "command": argv,
            },
            fix_hint=f"start it with `{' '.join(argv)}`, then re-run",
        )

    root = _state_root(state_root) / deployment
    log_path = root / MCP_SERVE_LOG_FILE
    start = launcher or _default_launcher
    pid = start(argv, log_path)
    record = {
        "deployment": deployment,
        "pid": pid,
        "endpoint": endpoint,
        "argv": argv,
        "log": str(log_path),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / MCP_SERVE_STATE_FILE).write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    deadline = now() + options.mcp_ready_timeout
    while True:
        observation = observe(endpoint)
        if not observation.dead:
            break
        if now() >= deadline:
            raise ChatSetupError(
                f"mcp-serve was started (pid {pid}) but did not answer at "
                f"{endpoint} within {options.mcp_ready_timeout:.0f}s.",
                data={
                    "reason": "mcp_serve_not_answering",
                    "step": STEP_GRAPH,
                    "endpoint": endpoint,
                    "pid": pid,
                    "log": str(log_path),
                },
                fix_hint=f"read {log_path}, fix the cause, then re-run",
            )
        sleep(0.5)
    step.status = STATUS_OK
    step.detail = f"started mcp-serve (pid {pid}); it answers at {endpoint}"
    step.changes.append(f"started `{' '.join(argv)}` (pid {pid}, log {log_path})")
    step.data = {
        "endpoint": endpoint,
        "started": True,
        "pid": pid,
        "log": str(log_path),
        "command": argv,
    }
    return step


# ---------------------------------------------------------------------------
# Step 6: the conversation
# ---------------------------------------------------------------------------

def refresh_served_models(client: Any, *, preset_id: str) -> int:
    """Reload the instance's model list; the preset must be in it.

    Issue #2874. Open WebUI 0.11.0 answers a completion from a cached model
    list (``main.py:1059``, lookup ``:1070``) that is loaded only while
    empty, and the model step already loaded it BEFORE sync created the
    preset. ``GET /api/models`` rebuilds that cache on every call, reading
    the workspace models from the database (``utils/models.py:146``, stored
    at ``:426``), so one call after sync makes the new preset answerable.
    Returns the number of models served.
    """
    listing = client.call("GET", PATH_MODELS)
    rows = listing.get("data") if isinstance(listing, Mapping) else None
    served = [
        str(m.get("id")) for m in (rows or [])
        if isinstance(m, Mapping) and m.get("id")
    ]
    if preset_id not in served:
        raise ChatSetupError(
            f"after sync, the instance's refreshed model list does not serve "
            f"the preset {preset_id!r} ({len(served)} model(s) served), so "
            "the test question would answer 'Model not found'.",
            data={
                "reason": "preset_not_served",
                "step": STEP_CONVERSATION,
                "preset_id": preset_id,
                "served": sorted(served)[:50],
            },
            fix_hint=(
                "read the sync report's preset surface and the instance log, "
                "then re-run"
            ),
        )
    return len(served)


#: The title of the chat the test question is asked in. It stays in the
#: admin's chat list, where the operator can open the recorded tool run.
PROBE_CHAT_TITLE = "okg chat setup: test question"


def _reply_failed(preset_id: str, exc: Any) -> ChatSetupError:
    return ChatSetupError(
        f"the preset {preset_id!r} did not reply: {exc.message} "
        f"({exc.data.get('detail') or exc.data.get('reason')}).",
        data={
            "reason": "model_reply_failed",
            "step": STEP_CONVERSATION,
            "preset_id": preset_id,
            "http_status": exc.data.get("http_status"),
            "detail": exc.data.get("detail"),
        },
        fix_hint=(
            "check the provider key and that chat.models.default is a model "
            "the provider serves (the instance log names the upstream "
            "error), then re-run"
        ),
    )


RESULT_ANSWER = "answer"
RESULT_REFUSED = "refused"
RESULT_FAILED = "failed"


def _literal_dict(text: str) -> Any:
    """``text`` as a Python dict or list literal, or None."""
    text = text.strip()
    if not text.startswith(("{", "[")):
        return None
    try:
        value = ast.literal_eval(text)
    except (ValueError, SyntaxError, MemoryError, RecursionError):
        return None
    return value if isinstance(value, (dict, list)) else None


def _unwrap_content(value: Any) -> Any:
    """The text of a stored MCP ``content`` list, else ``value`` itself.

    Since issue #3059 an okg result carries no ``structuredContent``, so both
    the compatibility image and upstream Open WebUI select the ``content``
    list: a success is stored as that list (JSON), and an error is raised as
    ``Exception(<the list>)``, whose ``str`` is a Python list literal. The
    one text block inside is the result itself.
    """
    if isinstance(value, list):
        return first_text_block(value)
    return value


def _parse_result(text: str) -> tuple[Any, bool]:
    """The recorded result, and whether it was the error form.

    The result is the text of the stored content list's text block (a
    rendered okg result since issue #3059), or a dictionary for a result
    recorded before it. A tool's answer is stored as JSON
    (``tool_result_content``, ``utils/middleware.py:313-318`` at 0.11.4). An
    MCP result flagged ``isError`` is raised as ``Exception(<selected
    object>)`` (``utils/mcp/client.py:126-127``), and the two pinned
    versions store that exception differently:

    * 0.11.0 stored ``str(e)`` itself (``middleware.py:5006``): a Python
      literal;
    * 0.11.4 stores ``{'error': str(e)}`` (``middleware.py:6027``), which
      reaches the chat as the JSON ``{"error": "<python literal>"}``.

    Both are the error form. Read as plain JSON, the 0.11.4 shape is a dict
    with a single ``error`` key holding a literal, which is what identifies
    it. Any other ``{"error": "<text>"}`` -- a connection failure, a missing
    tool -- carries no graph object and stays a failure, as the bare text did
    at 0.11.0.
    """
    text = text.strip()
    if not text.startswith(("{", "[")):
        return None, False
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = None
    if parsed is not None:
        if isinstance(parsed, dict) and set(parsed) == {"error"} and isinstance(parsed["error"], str):
            inner = _literal_dict(parsed["error"])
            return (_unwrap_content(inner), True) if inner is not None else (None, False)
        return _unwrap_content(parsed), False
    inner = _literal_dict(text)
    return (_unwrap_content(inner), True) if inner is not None else (None, False)


def _classify_result(text: str) -> tuple[str, str | None]:
    """``answer``, ``refused`` (with the graph's refusal code) or ``failed``.

    ``answer``: a result the graph returned without refusing. ``refused``:
    the graph answered with a refusal or through the MCP error flag -- it was
    reached, but the question did not consult it. ``failed``: the vendor's
    own failure text (``Error: Tool "<name>" not found.``, unparseable
    arguments, or the exception text of a call that never reached the graph),
    and a raised write or admin error, whose text has no status line.

    A rendered result (issue #3059) is classified by its status line:
    ``ok`` is an answer, ``refused``/``error`` a refusal with its code,
    ``status=<v>`` failed for ``error``/``failed`` and an answer otherwise,
    and no status line a failure.
    """
    parsed, error_form = _parse_result(text)
    if isinstance(parsed, str):
        line = parse_status_line(parsed)
        kind = classify_status_line(line)
        if kind == RESULT_REFUSED:
            return RESULT_REFUSED, str(line.code) if line and line.code else "is_error"
        if kind == RESULT_ANSWER and not error_form:
            return RESULT_ANSWER, None
        return RESULT_FAILED, None
    if not isinstance(parsed, dict):
        return RESULT_FAILED, None
    if error_form or parsed.get("status") == "refused":
        error = parsed.get("error")
        code = error.get("code") if isinstance(error, dict) else None
        return RESULT_REFUSED, str(code) if code else "is_error"
    # The vendor's own error test (`_is_tool_result_error`,
    # utils/middleware.py:158-188 at 0.11.4): a non-empty `error`, or a
    # `status` of error/failed, is not a graph answer whatever else it holds.
    status = parsed.get("status")
    if parsed.get("error") or (
        isinstance(status, str) and status.strip().lower() in {"error", "failed"}
    ):
        return RESULT_FAILED, None
    return RESULT_ANSWER, None


def _graph_tool_runs(output: Any, *, server_id: str) -> list[dict[str, Any]]:
    """The stored turn's graph tool calls, each with its recorded result."""
    items = [item for item in (output or []) if isinstance(item, Mapping)]
    results: dict[str, str] = {}
    #: call ids whose output the vendor itself marked failed (0.11.4,
    #: utils/middleware.py:6124-6127). Such an output is never an answer.
    vendor_failed: set[str] = set()
    for item in items:
        if item.get("type") != "function_call_output":
            continue
        parts = item.get("output") if isinstance(item.get("output"), list) else []
        call_id = str(item.get("call_id") or "")
        results[call_id] = "".join(
            str(part.get("text") or "") for part in parts if isinstance(part, Mapping)
        )
        if item.get("status") == "failed":
            vendor_failed.add(call_id)
    prefix = f"{server_id}_"
    runs = []
    for item in items:
        name = str(item.get("name") or "")
        if item.get("type") != "function_call" or not name.startswith(prefix):
            continue
        call_id = str(item.get("call_id") or "")
        text = results.get(call_id)
        kind, code = _classify_result(text) if text is not None else (RESULT_FAILED, None)
        if kind == RESULT_ANSWER and call_id in vendor_failed:
            kind, code = RESULT_FAILED, None
        runs.append({
            "tool": name,
            "result": kind,
            "refusal_code": code,
            "generation": kind == RESULT_ANSWER and "generation" in (text or ""),
        })
    return runs


def _timed_out(exc: Any) -> bool:
    """The client gave up waiting; the vendor may still be running the turn."""
    cause = exc.__cause__
    if isinstance(cause, urllib.error.URLError):
        cause = cause.reason
    return isinstance(cause, TimeoutError)


#: Seconds between reads of the stored turn while it is still running.
PROBE_POLL_INTERVAL = 5.0


def probe_conversation(
    client: Any,
    *,
    preset_id: str,
    tool_id: str,
    question: str,
    secrets: Sequence[str | None] = (),
    tool_source: str = "instance",
    wait_timeout: float = 300.0,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[SetupStep, dict[str, Any]]:
    """One question to the preset, tools attached EXPLICITLY.

    Ready means: a non-empty assistant message, and at least one call to a
    graph tool (``<tool server id>_<operator>``) whose recorded result is a
    graph answer the graph did not refuse -- the vendor's own record that the
    graph was consulted during this turn. A reply with no graph call is a
    model answering from its own knowledge, which is the failure #1183
    reported: ``tool_use_unobserved``, never ready. Graph calls that were all
    refused are ``graph_tool_calls_refused``; calls that all failed before
    reaching the graph are ``graph_tool_calls_failed``.

    Open WebUI 0.11 runs tools only in a streamed turn of a stored chat
    (module docstring, issue #2907), so the question is asked in a new chat
    and the verdict is read from the stored assistant message. Function
    calling is pinned to ``native``, the vendor default the browser uses,
    because the stored record read here is the native loop's. The whole tool
    loop runs inside the completion request; when the client stops waiting
    first, the stored turn is read every :data:`PROBE_POLL_INTERVAL` seconds
    for up to ``wait_timeout`` more before ``probe_turn_timeout``.
    """
    from okg.substrate.chat.sync import MCP_TOOL_ID_PREFIX, ChatSyncError

    if tool_source == "provider":
        return _probe_provider_tools(
            client, preset_id=preset_id, tool_id=tool_id, question=question,
            secrets=secrets,
        )
    step = SetupStep(STEP_CONVERSATION)
    held = tuple(s for s in secrets if s)
    try:
        chat = client.call(
            "POST", PATH_CHAT_NEW,
            {"chat": {"title": PROBE_CHAT_TITLE, "models": [preset_id]}},
            secrets=held,
        )
    except ChatSyncError as exc:
        chat = exc
    chat_id = chat.get("id") if isinstance(chat, Mapping) else None
    if not isinstance(chat_id, str) or not chat_id:
        failure = chat if isinstance(chat, ChatSyncError) else None
        raise ChatSetupError(
            "the instance did not create the chat the test question is asked in"
            + (f": {failure.message}." if failure else "."),
            data={
                "reason": "probe_chat_not_created",
                "step": STEP_CONVERSATION,
                "http_status": failure.data.get("http_status") if failure else None,
                "detail": failure.data.get("detail") if failure else None,
            },
            fix_hint="read the instance log, then re-run",
        ) from failure
    user_message_id, assistant_id = str(uuid.uuid4()), str(uuid.uuid4())
    payload = {
        "model": preset_id,
        "chat_id": chat_id,
        "id": assistant_id,
        "stream": True,
        "messages": [{"role": "user", "content": question}],
        "user_message": {
            "id": user_message_id, "role": "user", "content": question,
            "parentId": None, "childrenIds": [assistant_id], "models": [preset_id],
            "timestamp": int(time.time()),
        },
        "tool_ids": [tool_id],
        "params": {"function_calling": "native"},
        "background_tasks": {"title_generation": False, "tags_generation": False},
    }
    completion_timed_out = False
    try:
        client.call("POST", PATH_COMPLETIONS, payload, secrets=held)
    except ChatSyncError as exc:
        if not _timed_out(exc):
            raise _reply_failed(preset_id, exc) from exc
        completion_timed_out = True
    chat_path = PATH_CHAT.format(chat_id=urllib.parse.quote(chat_id, safe=""))
    polls = max(1, int(-(-wait_timeout // PROBE_POLL_INTERVAL)))
    for attempt in range(polls + 1):
        try:
            row = client.call("GET", chat_path, secrets=held)
        except ChatSyncError as exc:
            raise ChatSetupError(
                f"the test question was asked, but reading its stored answer "
                f"back failed: {exc.message}.",
                data={
                    "reason": "probe_readback_failed",
                    "step": STEP_CONVERSATION,
                    "chat_id": chat_id,
                    "http_status": exc.data.get("http_status"),
                    "detail": exc.data.get("detail"),
                },
                fix_hint=(
                    "open the chat titled "
                    f"{PROBE_CHAT_TITLE!r} on the site to see the answer, read the "
                    "instance log, then re-run"
                ),
            ) from exc
        history = (
            ((row or {}).get("chat") or {}).get("history") if isinstance(row, Mapping) else None
        )
        messages = history.get("messages") if isinstance(history, Mapping) else None
        message = messages.get(assistant_id) if isinstance(messages, Mapping) else None
        message = message if isinstance(message, Mapping) else {}
        if message.get("done") is True or message.get("error"):
            break
        if attempt == polls:
            raise ChatSetupError(
                "the test question's turn did not finish within "
                f"{wait_timeout:.0f}s of "
                + ("the request timing out" if completion_timed_out else "the request returning")
                + "; the site may still be running it.",
                data={
                    "reason": "probe_turn_timeout",
                    "step": STEP_CONVERSATION,
                    "chat_id": chat_id,
                    "completion_timed_out": completion_timed_out,
                    "waited_s": round(polls * PROBE_POLL_INTERVAL),
                    "tool_call_count": len(_graph_tool_runs(
                        message.get("output"),
                        server_id=tool_id.removeprefix(MCP_TOOL_ID_PREFIX),
                    )),
                },
                fix_hint=(
                    f"open the chat titled {PROBE_CHAT_TITLE!r} on the site to see "
                    "whether it finished; with a slow model, re-run with a "
                    "simpler --question"
                ),
            )
        sleep(PROBE_POLL_INTERVAL)
    error = message.get("error")
    if error:
        detail = error.get("content") if isinstance(error, Mapping) else error
        raise ChatSetupError(
            f"the preset {preset_id!r} did not reply: "
            f"{_scrub(str(detail)[:300], *secrets)}.",
            data={
                "reason": "model_reply_failed",
                "step": STEP_CONVERSATION,
                "preset_id": preset_id,
                "chat_id": chat_id,
                "detail": _scrub(str(detail)[:300], *secrets),
            },
            fix_hint=(
                "check the provider key and that chat.models.default is a model "
                "the provider serves (the instance log names the upstream "
                "error), then re-run"
            ),
        )
    content = message.get("content") if isinstance(message.get("content"), str) else ""
    output = message.get("output") if isinstance(message.get("output"), list) else []
    runs = _graph_tool_runs(output, server_id=tool_id.removeprefix(MCP_TOOL_ID_PREFIX))
    answered = [r for r in runs if r["result"] == RESULT_ANSWER]
    refused = [r for r in runs if r["result"] == RESULT_REFUSED]
    refusal_codes = sorted({str(r["refusal_code"]) for r in refused})
    conversation = {
        "question": question,
        "preset_id": preset_id,
        "tool_id": tool_id,
        "chat_id": chat_id,
        "replied": bool(content.strip()),
        "reply_excerpt": _scrub(content.strip()[:240], *secrets),
        "tools_invoked": sorted({r["tool"] for r in answered}),
        "tool_call_count": len(runs),
        "tool_result_count": len(answered),
        "tool_refusal_count": len(refused),
        "refusal_codes": refusal_codes,
        "generation_evidence": any(r["generation"] for r in answered),
        "tool_evidence": TOOL_EVIDENCE_INSTANCE,
    }
    if not content.strip():
        # Shape only, never content: what the empty turn still recorded, so
        # an operator can tell a model that stopped on a tool call or spent
        # its turn thinking from one that answered nothing.
        raise ChatSetupError(
            f"the preset {preset_id!r} answered with an empty message.",
            data={
                "reason": "model_reply_empty",
                "step": STEP_CONVERSATION,
                "preset_id": preset_id,
                "chat_id": chat_id,
                "turn_finished": message.get("done") is True,
                "output_item_types": sorted({
                    str(i.get("type")) for i in output if isinstance(i, Mapping)
                })[:10],
                "tool_call_count": len(runs),
                "tool_result_count": len(answered),
                "tool_refusal_count": len(refused),
                "reasoning_present": any(
                    isinstance(i, Mapping) and i.get("type") == "reasoning" for i in output
                ),
            },
            fix_hint="check the provider and model in the instance log, then re-run",
        )
    if not answered:
        if refused:
            reason = REASON_GRAPH_TOOLS_REFUSED
            detail = (
                f"the preset replied; the graph refused every call that reached "
                f"it ({len(refused)} of {len(runs)} graph tool call(s), {reason}): "
                + ", ".join(refusal_codes)
            )
        elif runs:
            reason = REASON_GRAPH_TOOLS_FAILED
            detail = (
                f"the preset replied; it called {len(runs)} graph tool(s) and none "
                f"returned a graph result ({reason}): "
                + ", ".join(sorted({r["tool"] for r in runs}))
            )
        else:
            reason = REASON_TOOL_USE_UNOBSERVED
            detail = (
                "the preset replied but its stored turn records no graph tool "
                f"call: the graph was not consulted ({reason})"
            )
        conversation["ready"] = False
        conversation["reason"] = reason
        step.status = STATUS_FAILED
        step.detail = detail
        step.data = dict(conversation)
        return step, conversation
    conversation["ready"] = True
    step.status = STATUS_OK
    step.detail = (
        f"the preset replied and invoked {conversation['tools_invoked']}"
        + (
            "; a generation is named in the tool results"
            if conversation["generation_evidence"] else ""
        )
        + (f"; {len(refused)} other call(s) refused" if refused else "")
    )
    step.data = dict(conversation)
    return step, conversation


def _probe_provider_tools(
    client: Any,
    *,
    preset_id: str,
    tool_id: str,
    question: str,
    secrets: Sequence[str | None],
) -> tuple[SetupStep, dict[str, Any]]:
    """``chat.provider.tools: provider``: one plain completion.

    The provider runs the graph tools itself and reports them in its own
    response record, which the vendor passes through only on the
    non-streaming path (measured 2026-09-23); the site's tool loop never runs.
    """
    from okg.substrate.chat.sync import ChatSyncError

    step = SetupStep(STEP_CONVERSATION)
    payload = {
        "model": preset_id,
        "messages": [{"role": "user", "content": question}],
        "stream": False,
        "tool_ids": [tool_id],
    }
    try:
        answer = client.call("POST", PATH_COMPLETIONS, payload, secrets=tuple(secrets))
    except ChatSyncError as exc:
        raise _reply_failed(preset_id, exc) from exc
    choices = answer.get("choices") if isinstance(answer, Mapping) else None
    first = choices[0] if choices and isinstance(choices[0], Mapping) else {}
    message = first.get("message")
    content = message.get("content") if isinstance(message, Mapping) else None
    content = content if isinstance(content, str) else ""
    conversation = {
        "question": question,
        "preset_id": preset_id,
        "tool_id": tool_id,
        "replied": bool(content.strip()),
        "reply_excerpt": _scrub(content.strip()[:240], *secrets),
        "tools_invoked": [],
        "generation_evidence": False,
        "tool_evidence": TOOL_EVIDENCE_PROVIDER,
    }
    if not content.strip():
        finish_reason = first.get("finish_reason")
        raise ChatSetupError(
            f"the preset {preset_id!r} answered with an empty message.",
            data={
                "reason": "model_reply_empty",
                "step": STEP_CONVERSATION,
                "preset_id": preset_id,
                "finish_reason": (
                    finish_reason[:40] if isinstance(finish_reason, str) else None
                ),
                "message_has_tool_calls": bool(
                    isinstance(message, Mapping) and message.get("tool_calls")
                ),
                "reasoning_present": bool(
                    isinstance(message, Mapping) and any(
                        message.get(key)
                        for key in ("reasoning_content", "reasoning", "thinking")
                    )
                ),
            },
            fix_hint="check the provider and model in the instance log, then re-run",
        )
    return _provider_tool_verdict(step, conversation, answer)


def _provider_tool_verdict(
    step: SetupStep, conversation: dict[str, Any], answer: Any,
) -> tuple[SetupStep, dict[str, Any]]:
    """Readiness for ``chat.provider.tools: provider``: the provider's record."""
    conversation["tool_evidence"] = TOOL_EVIDENCE_PROVIDER
    record = answer.get(PROVIDER_TOOL_RECORD_KEY) if isinstance(answer, Mapping) else None
    calls = record.get("tool_calls") if isinstance(record, Mapping) else None
    calls = [c for c in (calls or []) if isinstance(c, Mapping)]
    answered = [c for c in calls if c.get("status") == "completed"]
    conversation["provider_tool_calls"] = [
        {"server": c.get("server"), "tool": c.get("tool"), "status": c.get("status")}
        for c in calls
    ]
    conversation["tools_invoked"] = sorted({str(c.get("tool") or "?") for c in answered})
    if not isinstance(record, Mapping):
        conversation["ready"] = False
        conversation["reason"] = REASON_TOOL_USE_UNOBSERVED
        step.status = STATUS_FAILED
        step.detail = (
            "chat.provider.tools is 'provider', but the response carries no "
            f"{PROVIDER_TOOL_RECORD_KEY!r} tool-call record, so there is no evidence "
            f"the graph was consulted ({REASON_TOOL_USE_UNOBSERVED})"
        )
    elif not answered:
        conversation["ready"] = False
        conversation["reason"] = (
            REASON_PROVIDER_TOOLS_FAILED if calls else REASON_TOOL_USE_UNOBSERVED
        )
        step.status = STATUS_FAILED
        step.detail = (
            f"the preset replied; the provider called {len(calls)} graph tool(s) and "
            f"none answered ({conversation['reason']}): "
            + ", ".join(f"{c.get('tool')}={c.get('status')}" for c in calls)
            if calls
            else "the preset replied but the provider reports no graph tool call "
            f"({REASON_TOOL_USE_UNOBSERVED})"
        )
    else:
        conversation["ready"] = True
        step.status = STATUS_OK
        step.detail = (
            f"the preset replied and the provider reports it invoked "
            f"{conversation['tools_invoked']} (provider-reported, not the site's "
            "own tool-result sources)"
        )
    step.data = dict(conversation)
    return step, conversation


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def site_url_for(base_url: str, preset_id: str) -> str:
    """The page to open: the site, pre-selecting the preset by id."""
    return f"{base_url.rstrip('/')}/?models={urllib.parse.quote(preset_id, safe='')}"


def _error_payload(err: SubstrateError) -> dict[str, Any]:
    return err.to_json()["error"]


#: The ``data`` fields a configure-step error may carry. The configure step
#: is where operator answers -- one of which may be a pasted key -- enter,
#: so its error data is kept by ALLOWLIST: a field not named here is dropped
#: and only its name is reported under ``withheld_fields``. A new field an
#: error grows later is withheld until someone adds it here on purpose.
CONFIGURE_ERROR_DATA_FIELDS: frozenset[str] = frozenset({
    "reason", "flag", "path", "observed", "observed_type", "shape",
    "requested", "known", "declared", "deployment", "conflicts", "line",
    "column", "missing", "unknown", "extra", "required", "admitted",
    "builtin", "step",
    # Search-settings errors: key names from the file and the loader's own
    # accepted keys. Never an answer value.
    "unknown_keys", "accepted_keys", "key", "undeclared",
})
#: The fields each entry of ``data['conflicts']`` may carry.
CONFIGURE_CONFLICT_FIELDS: frozenset[str] = frozenset({
    "path", "declared", "answered", "flag",
})


def _allowlist_configure_data(data: Mapping[str, Any]) -> dict[str, Any]:
    kept: dict[str, Any] = {}
    withheld: list[str] = []
    for key, value in data.items():
        if key not in CONFIGURE_ERROR_DATA_FIELDS:
            withheld.append(str(key))
            continue
        if key == "conflicts" and isinstance(value, (list, tuple)):
            value = [
                {k: v for k, v in entry.items() if k in CONFIGURE_CONFLICT_FIELDS}
                for entry in value if isinstance(entry, Mapping)
            ]
        kept[key] = value
    if withheld:
        kept["withheld_fields"] = sorted(withheld)
    return kept


def run_setup(
    deployment: str,
    *,
    options: SetupOptions | None = None,
    environ: Mapping[str, str] | None = None,
    state_root: Path | str | None = None,
    load_manifest: Callable[[], tuple[Any, str | None]] | None = None,
    stand_up: Callable[..., Any] | None = None,
    opener: Opener | None = None,
    load_inputs: Callable[..., Any] | None = None,
    run_sync: Callable[[Any, Any], Any] | None = None,
    mcp_probe: Callable[[str], Any] | None = None,
    mcp_launcher: Launcher | None = None,
    sleep: Callable[[float], None] = time.sleep,
    configure: Callable[[Any], Any] | None = None,
) -> SetupReport:
    """Run the steps in order; return the report (never raise for a step).

    Every seam is injectable so the hermetic suite drives the real decision
    code against a fake instance; the CLI passes none of them.
    """
    from okg.substrate.chat.instance import DockerRuntime
    from okg.substrate.chat.instance import stand_up as real_stand_up
    from okg.substrate.chat.sync import InstanceClient

    opts = options or SetupOptions()
    env = os.environ if environ is None else environ
    report = SetupReport(deployment=deployment)
    steps = report.steps
    held_secrets: list[str | None] = [
        env.get(ENV_ADMIN_TOKEN) or None, env.get(ENV_ADMIN_PASSWORD) or None,
    ]
    if opts.chat_answers is not None:
        from okg.substrate.chat.block_scaffold import secret_answers

        # An answer that may itself be a credential (a key pasted into
        # --provider-key-env, a URL with user:password@) is scrubbed by
        # identity from every error, on top of the configure allowlist.
        held_secrets.extend(secret_answers(opts.chat_answers))

    def fail(name: str, err: SubstrateError) -> SetupReport:
        payload = _error_payload(err)
        # By IDENTITY only: the shape pass in `redact` would also eat
        # variable names and hostnames, which are exactly what a fix hint
        # has to carry. `held_secrets` is the ceiling this module owns, so
        # message, fix hint AND every string in `data` are swept -- a vendor
        # body reaching `data['detail']` was already scrubbed at the call,
        # and this is the belt to that suspenders.
        payload["message"] = _scrub(str(payload.get("message", "")), *held_secrets)
        if payload.get("fix_hint"):
            payload["fix_hint"] = _scrub(str(payload["fix_hint"]), *held_secrets)
        payload.setdefault("data", {})
        data = dict(payload["data"])
        if name == STEP_CONFIGURE:
            data = _allowlist_configure_data(data)
        payload["data"] = _scrub_tree(data, held_secrets)
        payload["data"].setdefault("step", name)
        steps[name].status = STATUS_FAILED
        steps[name].detail = payload["message"]
        steps[name].data = {
            "reason": payload["data"].get("reason"),
            "code": payload.get("code"),
        }
        report.failed_step = name
        report.error = payload
        return report

    # 0. configure -------------------------------------------------------
    report.configure_only = opts.configure_only
    answers = opts.chat_answers
    if answers is not None and (answers.given or opts.configure_only):
        from okg.substrate.chat.block_scaffold import ensure_chat_block

        try:
            outcome = (
                configure or (lambda a: ensure_chat_block(deployment, a, environ=env))
            )(answers)
        except SubstrateError as err:
            return fail(STEP_CONFIGURE, err)
        step = steps[STEP_CONFIGURE]
        step.status = STATUS_OK
        step.detail = (
            f"wrote the chat: block into {outcome.manifest}"
            if outcome.status == "written"
            else f"the chat: block in {outcome.manifest} is declared and valid"
        )
        step.data = outcome.as_dict()
        step.changes.extend(f"wrote {path}" for path in outcome.written)
        step.notes.extend(outcome.notes)
        step.notes.extend(f"warning: {w}" for w in getattr(outcome, "warnings", ()))
    else:
        steps[STEP_CONFIGURE].status = STATUS_SKIPPED
        steps[STEP_CONFIGURE].detail = (
            "no chat answers given; the manifest's chat: block is used as is"
        )
    if opts.configure_only:
        return report

    # 1. instance ---------------------------------------------------------
    try:
        if load_manifest is None:
            from okg.substrate.chat.instance_cli import _load_manifest

            chat, graph_dsn = _load_manifest(deployment, require_graph_dsn=True)
        else:
            chat, graph_dsn = load_manifest()
        up = (stand_up or real_stand_up)(
            deployment,
            chat_block=chat,
            # The bundle's site name reaches WEBUI_NAME, as readback expects.
            install_plan=opts.install_plan,
            graph_dsn=graph_dsn,
            runtime=DockerRuntime(binary=opts.container_runtime),
            environ=env,
            app_database_env=opts.app_database_env,
            secret_env=opts.secret_env,
            instance_port_override=opts.instance_port,
            image=opts.image,
            host=opts.host,
            host_gateway_alias=opts.host_gateway_alias or None,
            state_root=state_root,
            ready_timeout=opts.ready_timeout,
            container_network=opts.container_network,
            extra_forward_ports=_setup_forward_ports(opts),
        )
    except SubstrateError as err:
        return fail(STEP_INSTANCE, err)
    # In the Linux modes the container dials the host's loopback as
    # 127.0.0.1 itself, so the graph URL is registered as the probe URL and
    # setup and a later plain sync register the same URL (#2876 item 2).
    graph_opts = opts
    if getattr(up, "network_mode", NETWORK_GATEWAY) != NETWORK_GATEWAY:
        graph_opts = replace(opts, host_gateway_alias=None)
    report.base_url = up.base_url
    steps[STEP_INSTANCE].status = STATUS_OK
    steps[STEP_INSTANCE].detail = (
        f"{up.outcome} at {up.base_url} serving version {up.version}"
    )
    steps[STEP_INSTANCE].data = {
        "outcome": up.outcome, "version": up.version, "base_url": up.base_url,
        "container": up.container, "image": up.image,
    }
    if up.outcome == "started":
        steps[STEP_INSTANCE].changes.append(f"started container {up.container}")
    steps[STEP_INSTANCE].notes.extend(up.notes)

    # 2. admin ------------------------------------------------------------
    try:
        access = resolve_admin_access(
            up.base_url, deployment, environ=env, opener=opener,
            admin_email=opts.admin_email,
        )
    except SubstrateError as err:
        return fail(STEP_ADMIN, err)
    held_secrets.append(access.token)
    report.admin_email = access.email
    report.credential_source = access.source
    steps[STEP_ADMIN].status = STATUS_OK
    steps[STEP_ADMIN].detail = (
        f"admin credential from {ENV_ADMIN_TOKEN}"
        if access.source == CREDENTIAL_SOURCE_ENV
        else f"{'created' if access.account_created else 'signed in as'} "
             f"the admin account {access.email!r}"
    )
    steps[STEP_ADMIN].data = access.as_dict()
    if access.account_created:
        steps[STEP_ADMIN].changes.append(
            f"created the first (admin) account {access.email!r} from "
            f"{ENV_ADMIN_EMAIL} and {ENV_ADMIN_PASSWORD}"
        )
    client = InstanceClient(up.base_url, access.token, opener=opener)

    # 3. provider, and the model ------------------------------------------
    # The provider key joins the swept set BEFORE the step that uses it can
    # fail, so a refusal on that step cannot carry it.
    provider = getattr(chat, "provider", None)
    if provider is not None and provider.api_key_env:
        held_secrets.append(env.get(provider.api_key_env) or None)
    try:
        steps[STEP_PROVIDER] = converge_provider(client, chat, environ=env)
    except SubstrateError as err:
        return fail(STEP_PROVIDER, err)
    try:
        steps[STEP_MODEL] = check_model_ready(client, chat)
    except SubstrateError as err:
        return fail(STEP_MODEL, err)

    # 4. graph endpoint ---------------------------------------------------
    try:
        steps[STEP_GRAPH] = ensure_graph_endpoint(
            deployment, chat, options=graph_opts, environ=env, state_root=state_root,
            probe=mcp_probe, launcher=mcp_launcher, sleep=sleep,
        )
    except SubstrateError as err:
        return fail(STEP_GRAPH, err)

    # 5. sync -------------------------------------------------------------
    try:
        from okg.substrate.chat.sync import load_inputs as real_load_inputs

        probe_url = steps[STEP_GRAPH].data["endpoint"]
        instance_url, gateway_note = instance_mcp_url(
            probe_url, declared=opts.mcp_instance_url,
            host_gateway_alias=graph_opts.host_gateway_alias,
        )
        # Recorded BEFORE sync registers it, so a later plain `okg chat
        # sync` re-registers this URL rather than the probe URL (#2876).
        record_graph_endpoint(
            deployment, probe_url=probe_url, instance_url=instance_url,
            instance_base_url=up.base_url, state_root=state_root,
        )
        inputs = (load_inputs or real_load_inputs)(
            deployment,
            mcp_probe_url=probe_url,
            mcp_instance_url=instance_url,
            mcp_host=opts.mcp_host,
            install_plan_dir=opts.install_plan,
        )
        report.preset_id = inputs.preset_id
        if run_sync is None:
            from okg.substrate.chat.health_banner import cli_health_probe
            from okg.substrate.chat.instance import container_name_for
            from okg.substrate.chat.sync import sync

            sync_report = sync(
                inputs, client,
                probe_health=cli_health_probe,
                container_runtime=DockerRuntime(binary=opts.container_runtime),
                container=container_name_for(deployment),
            )
        else:
            sync_report = run_sync(inputs, client)
    except SubstrateError as err:
        partial = getattr(err, "partial_report", None)
        out = fail(STEP_SYNC, err)
        if partial is not None:
            steps[STEP_SYNC].data["partial_report"] = partial.to_dict()
        return out
    if gateway_note:
        steps[STEP_SYNC].notes.append(gateway_note)
    if not sync_report.ok:
        steps[STEP_SYNC].status = STATUS_FAILED
        steps[STEP_SYNC].detail = "sync reported a surface it could not verify"
        steps[STEP_SYNC].data = {"sync": sync_report.to_dict()}
        report.failed_step = STEP_SYNC
        report.error = {
            "code": "chat_sync_failed",
            "message": steps[STEP_SYNC].detail,
            "fix_hint": "read the sync report's deviations, fix them, then re-run",
            "data": {"step": STEP_SYNC, "reason": "sync_not_ok"},
        }
        return report
    steps[STEP_SYNC].status = STATUS_OK
    steps[STEP_SYNC].detail = (
        f"configuration converged: {sync_report.written_surfaces_verified} "
        f"surface(s) verified, {sync_report.changes} change(s)"
    )
    steps[STEP_SYNC].data = {
        "plan_digest": sync_report.plan_digest,
        "changes": sync_report.changes,
        "surfaces_verified": sync_report.written_surfaces_verified,
        "mcp": dict(sync_report.mcp),
    }
    report.converged = True
    report.site_url = site_url_for(up.base_url, inputs.preset_id)

    # 6. conversation -----------------------------------------------------
    from okg.substrate.chat.sync import MCP_TOOL_ID_PREFIX

    question = opts.question or (
        chat.preset.suggestions[0]
        if getattr(chat, "preset", None) and chat.preset.suggestions
        else DEFAULT_QUESTION
    )
    client_long = InstanceClient(
        up.base_url, access.token, opener=opener, timeout=opts.completion_timeout,
    )
    try:
        # Sync just created or changed the preset; the vendor's cached model
        # list predates it (#2874).
        served_count = refresh_served_models(client, preset_id=inputs.preset_id)
        steps[STEP_SYNC].notes.append(
            f"reloaded the instance's model list after sync; it serves "
            f"{inputs.preset_id!r} among {served_count} model(s)"
        )
        step, conversation = probe_conversation(
            client_long,
            preset_id=inputs.preset_id,
            tool_id=f"{MCP_TOOL_ID_PREFIX}{inputs.mcp_server_id}",
            question=question,
            secrets=tuple(held_secrets),
            tool_source=getattr(getattr(chat, "provider", None), "tools", None) or "instance",
            wait_timeout=opts.completion_timeout,
            sleep=sleep,
        )
    except SubstrateError as err:
        return fail(STEP_CONVERSATION, err)
    steps[STEP_CONVERSATION] = step
    report.conversation = conversation
    report.conversation_ready = bool(conversation.get("ready"))
    if not report.conversation_ready:
        report.failed_step = STEP_CONVERSATION
        reason = conversation.get("reason") or REASON_TOOL_USE_UNOBSERVED
        report.error = {
            "code": ChatSetupError.code,
            "message": step.detail,
            "fix_hint": (
                "check that mcp-serve serves the preset's tools (the sync report's "
                "mcp block lists them) and that the model follows tool calls; ask "
                "a question the graph can answer with --question; then re-run"
            ),
            "data": {"step": STEP_CONVERSATION, "reason": reason},
        }
    return report


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_human(report: SetupReport) -> str:
    headline = "ready" if report.ok else f"stopped at {report.failed_step}"
    if report.configure_only and report.ok:
        headline = "chat: block ready (configure only; nothing started)"
    lines = [f"chat setup {report.deployment}: {headline}"]
    for name in STEPS:
        step = report.steps[name]
        lines.append(f"  [{step.status}] {name}: {step.detail}".rstrip(": "))
        for change in step.changes:
            lines.append(f"      changed: {change}")
        for note in step.notes:
            lines.append(f"      note: {note}")
    lines.append(f"  configuration converged: {'yes' if report.converged else 'no'}")
    lines.append(
        "  conversation ready: "
        + ("yes" if report.conversation_ready else "no")
        + (
            f" — tools invoked {report.conversation.get('tools_invoked')}"
            if report.conversation_ready
            else ""
        )
    )
    if report.site_url:
        lines.append(
            f"  open: {report.site_url}"
            + (f" (sign in as {report.admin_email})" if report.admin_email else "")
        )
    if report.error:
        err = report.error
        lines.append(f"  error [{err.get('code')}]: {err.get('message')}")
        if err.get("fix_hint"):
            lines.append(f"    fix_hint: {err['fix_hint']}")
    if report.next_action:
        lines.append(f"  next: {report.next_action}")
    return "\n".join(lines)


def open_in_browser(url: str) -> bool:
    import webbrowser

    return bool(webbrowser.open(url))


__all__ = [
    "ADMIN_DISPLAY_NAME",
    "ENV_ADMIN_EMAIL",
    "ENV_ADMIN_PASSWORD",
    "ENV_ADMIN_TOKEN",
    "PROVIDER_TOOL_RECORD_KEY",
    "REASON_PROVIDER_TOOLS_FAILED",
    "REASON_TOOL_USE_UNOBSERVED",
    "STEPS",
    "TOOL_EVIDENCE_INSTANCE",
    "TOOL_EVIDENCE_PROVIDER",
    "AdminAccess",
    "ChatSetupError",
    "SetupOptions",
    "SetupReport",
    "SetupStep",
    "check_model_ready",
    "converge_provider",
    "default_admin_email",
    "ensure_graph_endpoint",
    "instance_mcp_url",
    "mcp_serve_argv",
    "normalize_provider_url",
    "open_in_browser",
    "probe_conversation",
    "record_graph_endpoint",
    "refresh_served_models",
    "render_human",
    "resolve_admin_access",
    "resolve_sync_instance_url",
    "run_setup",
    "site_url_for",
]
