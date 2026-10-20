"""Source administration from chat (PACT ``chat-source-admin``).

design.md §6 (the admin-surface replacement table) and §7 Plane 4. This
module holds the chat-side *binding* of source administration and nothing
else: the nine existing MCP source-admin tools
(:data:`~okg.substrate.mcp.operators.SOURCE_ADMIN_TOOLS`) are the complete
capability surface, and every recovery action here drives a command the
CLI already has. No new tool, no chat-local source registry, no
chat-local retry queue, no copy of source state.

The pieces ``okg chat sync`` (PACT ``chat-sync-projection``) calls:

* :func:`resolve_admin_tool_binding` — the bound set, derived from an
  authenticated ``tools/list`` READBACK of the live endpoint, not from the
  manifest. The admitted surface is a launch-time property of the serving
  process (``allow_writes`` plus ``OKG_MCP_TOOL_ALLOWLIST``), so a manifest
  cannot state it.
* :func:`admin_transport_precondition` — refuse the admin preset over an
  unauthenticated network endpoint; permit the loopback interim and CARRY
  the server's own warning rather than suppressing it.
* :func:`project_admin_preset` — the projection itself, which either
  returns a complete preset or raises. There is no degraded admin preset.
  ``okg chat sync`` is its writer (PACT ``chat-source-admin-writer``):
  when the deployment declares ``chat.sso.admin_group``, sync calls this
  function (``chat.sync.plan_admin_preset``), registers the admin
  connection, writes the preset row with explicit group grants, and reads
  both back. A projection this function refuses is reported on sync's
  ``admin_preset`` surface by name and nothing admin-bearing is written.
  What a written row proves is SPECIFIED gating only: the grant is
  visibility (see :data:`VISIBILITY_BOUNDS_ROLE_NOT_GROUP`), and the
  per-caller refusal stays blocked on :data:`PER_CALLER_ARM_BLOCKED_ON`.
* :func:`verify_admin_preset_readback` — the acceptance criterion's TWO
  assertions against a ``GET /api/v1/models/list`` read: the preset's
  ``meta.toolIds`` (which carry ``server:mcp:<server_id>``, never a tool
  name), and the tool set that server actually reaches.

And the pieces the operator-facing affordances need:
:func:`interpret_commit_envelope` (the two-phase flow, including the
``plan_not_found`` re-propose arm), :func:`retry_plan` /
:func:`retry_verdict` (the DLQ retry chain and its honest verdict),
:func:`shared_status_fields` (one status vocabulary), and
:func:`diagnose_generation`.

Four rules run through all of it.

**An absent reference set is not agreement.** An EMPTY ``tools/list``
readback is "the surface is unknown", not "no tool is admitted": it is
refused, because an empty live set would make the bound set empty, the
forbidden-set intersection empty, and every membership test pass while
proving nothing.

**Absent counts are not zero.** ``add_source`` returns a count-less
envelope when it cannot attribute rows to the run. Reading that as a zero
delta reports a failed ingest as a correct empty one, so
:class:`CommitOutcome` carries ``counts_authoritative`` and leaves the
counts ``None``.

**Hiding is not denying.** The admin preset's group grant is VISIBILITY.
The per-caller typed refusal needs ``mcp-principal-mapping`` (a bearer
token authenticates the Open WebUI *app*, not the person), so the interim
this module implements is labeled :data:`ENFORCEMENT_INTERIM_LABEL` and
:func:`evaluate_gating_evidence` never reports the full claim satisfied.
And the grant is not even a boundary between ADMINS: Open WebUI keys its
bypass on the caller's ROLE, so every instance admin sees the preset
whether or not they are in the deployment's admin group
(:data:`VISIBILITY_BOUNDS_ROLE_NOT_GROUP`, carried on every verdict this
module emits so no reader can cite a passing visibility readback against
non-group instance admins).

**A field the vendor does not have is a field the vendor drops.**
``ModelForm`` is ``ConfigDict(extra='ignore')`` at v0.11.0, so a body
naming a nonexistent key is accepted with HTTP 200 and silently
discarded. Every key :meth:`AdminPresetProjection.to_dict` emits names a
real v0.11.0 field, and the readback helpers refuse the OpenAI-compat
route, a bare-list payload, and a truncated page rather than reading any
of them as "not found".

**A retry that did not clear the failure did not succeed.** ``okg ingest
--enqueue`` carries no supersession flag, so it can advance a watermark
and add rows while the DLQ row stays current forever. Only
``okg jobs enqueue-source --supersedes-workflow-id <failed>`` satisfies
the DLQ's supersession predicate, and a verdict is NOT succeeded whenever
the row is still current — regardless of row delta.

Nothing here imports Open WebUI, the operator-console bundle, or a
database session.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.operators import (
    ALL_BUILTIN_TOOLS,
    SOURCE_ADMIN_TOOLS,
)


__all__ = [
    "ACCESS_GRANT_PERMISSION",
    "ACCESS_GRANT_PRINCIPAL_TYPE",
    "AUTH_OPEN",
    "AUTH_REQUIRED",
    "AUTH_UNKNOWN",
    "AdminBindingInputs",
    "AdminPresetProjection",
    "ChatSourceAdminError",
    "admin_binding_inputs",
    "CommitOutcome",
    "ENFORCEMENT_INTERIM_LABEL",
    "FORBIDDEN_SOURCE_ADMIN_SURFACE",
    "GatingEvidence",
    "GatingVerdict",
    "GenerationDiagnosis",
    "LiveToolSurface",
    "OPENAI_COMPAT_MODEL_ROUTE",
    "PER_CALLER_ARM_BLOCKED_ON",
    "PresetReadbackVerdict",
    "RESYNC_COMMAND_HINT",
    "RetryObservation",
    "RetryPlan",
    "RetryVerdict",
    "SHARED_STATUS_FIELDS",
    "TransportDecision",
    "VISIBILITY_BOUNDS_ROLE_NOT_GROUP",
    "WORKSPACE_MODEL_LIST_PAGE_SIZE",
    "WORKSPACE_MODEL_LIST_ROUTE",
    "access_grant_set",
    "admin_transport_precondition",
    "call_live_tool",
    "diagnose_generation",
    "digest_registry_file",
    "evaluate_gating_evidence",
    "interpret_commit_envelope",
    "mcp_tool_id",
    "observe_auth_posture",
    "parse_openai_compat_model_ids",
    "parse_workspace_model_listing",
    "project_admin_preset",
    "read_live_tools",
    "require_admin_transport",
    "resolve_admin_tool_binding",
    "retry_plan",
    "retry_verdict",
    "shared_status_fields",
    "status_field_diff",
    "user_preset_tools",
    "verify_admin_preset_readback",
]


class ChatSourceAdminError(SubstrateError):
    """A chat source-admin projection or affordance refused.

    Carries a stable wire ``code`` so the CLI, the tests, and the
    projection's failure message can dispatch on the specific refusal
    rather than on message text.
    """

    code = "chat_source_admin_refused"


# ---------------------------------------------------------------------------
# The source-admin non-replication boundary, as a checkable set
# ---------------------------------------------------------------------------

#: Surfaces design.md §6 says are DELIBERATELY NOT REPLICATED, named so the
#: claim is a test rather than prose. Two rows of the §6 chat table
#: ("Agent spec editing at runtime", "Runtime config editing
#: (``update_config``)") plus four out-of-scope external services (a
#: grader, Piazza, Mattermost, and Redmine interfaces).
#:
#: The set is asserted NON-EMPTY by its consumers: an empty forbidden set
#: makes every intersection empty, which would read as "nothing forbidden
#: is bound" while checking nothing.
FORBIDDEN_SOURCE_ADMIN_SURFACE: frozenset[str] = frozenset({
    # Runtime configuration mutation.
    "update_config",
    "set_config",
    "edit_config",
    "update_runtime_config",
    # Agent-spec editing.
    "update_agent_spec",
    "edit_agent_spec",
    "set_agent_prompt",
    "update_prompt",
    # External services the chat surface does not replicate.
    "grader_submit",
    "grader_grade",
    "piazza_post",
    "mattermost_post",
    "redmine_issue",
})


# ---------------------------------------------------------------------------
# The live tools/list readback
# ---------------------------------------------------------------------------

#: The endpoint DEMANDS a credential: an unauthenticated read was
#: refused and an authenticated one succeeded, so the endpoint is
#: reachable and the credential was the only difference.
AUTH_REQUIRED = "required"
#: An UNAUTHENTICATED read succeeded. Anyone who can reach the port can
#: use it.
AUTH_OPEN = "open"
#: Neither was established — typically the endpoint is unreachable.
#: Treated as NOT authenticated everywhere, which is the fail-closed
#: direction.
AUTH_UNKNOWN = "unknown"


@dataclass(frozen=True)
class LiveToolSurface:
    """One ``tools/list`` readback of a live endpoint.

    ``tool_names`` is the observed surface. It is deliberately not
    defaulted: a caller that has not read the endpoint back has no
    surface, and must not be able to express that as an empty tuple that
    silently satisfies a subset test.

    ``auth_posture`` is what was OBSERVED of the endpoint, not what the
    caller sent. Sending a bearer header proves the operator holds a
    credential; only a refused unauthenticated read proves the SERVER
    demands one. ``_write_transport_guard`` is explicit that its
    ``auth_active`` input must never come from a flag or an environment
    variable, and :attr:`authenticated` is the value fed to it.
    """

    endpoint: str
    tool_names: tuple[str, ...]
    auth_posture: str = AUTH_UNKNOWN
    read_back_at: Optional[str] = None

    @property
    def authenticated(self) -> bool:
        """True only when the endpoint was observed to REFUSE anonymity."""
        return self.auth_posture == AUTH_REQUIRED

    @property
    def source_admin_tools(self) -> tuple[str, ...]:
        """The nine, in :data:`SOURCE_ADMIN_TOOLS` order, that are live."""
        live = set(self.tool_names)
        return tuple(name for name in SOURCE_ADMIN_TOOLS if name in live)


def _bearer_headers(
    auth_token_env: Optional[str],
    environ: Optional[Mapping[str, str]],
) -> dict[str, str]:
    if auth_token_env is None:
        return {}
    from okg.substrate.mcp.http_auth import resolve_auth_token

    token = resolve_auth_token(auth_token_env, environ=environ)
    return {"Authorization": f"Bearer {token}"}


def _guard_dial(
    endpoint: str,
    *,
    credentialed: bool,
    anonymous_probe: bool,
) -> None:
    """Refuse to dial a routable endpoint with no credential.

    Every network call in this module goes through here, which is what
    makes the egress-coverage exemption for this file true rather than
    aspirational: a dial is either loopback, or carries a credential, or
    is the explicit ANONYMOUS PROBE that
    :func:`observe_auth_posture` needs in order to learn whether the
    endpoint demands one.
    """
    from okg.substrate.mcp.server import is_loopback_host

    if is_loopback_host(_host_of(endpoint)) or credentialed or anonymous_probe:
        return
    raise ChatSourceAdminError(
        f"refusing to dial the routable MCP endpoint {endpoint!r} with no "
        "credential. A source-admin readback against a non-loopback host "
        "requires the bearer credential mcp-http-auth introduced; without "
        "one there is nothing to authenticate the endpoint OR the caller.",
        data={"endpoint": endpoint, "reason": "uncredentialed_routable_dial"},
        fix_hint=(
            "pass --auth-token-env NAME, or point at a loopback endpoint"
        ),
    )


def _dial_tools_list(
    endpoint: str,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> tuple[str, ...]:
    """One raw ``tools/list`` round trip. Factored out so it is stubbable."""
    import asyncio

    async def _read() -> tuple[str, ...]:
        from datetime import timedelta

        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(
            endpoint,
            headers=dict(headers) or None,
            timeout=timedelta(seconds=timeout_seconds),
        ) as (read_stream, write_stream, _get_session_id):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=timeout_seconds),
            ) as session:
                await session.initialize()
                result = await session.list_tools()
                return tuple(tool.name for tool in result.tools)

    return asyncio.run(_read())


def observe_auth_posture(
    endpoint: str,
    *,
    auth_token_env: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    timeout_seconds: float = 20.0,
) -> str:
    """Observe whether ``endpoint`` DEMANDS a credential.

    Returns :data:`AUTH_OPEN` when an anonymous ``tools/list`` succeeds,
    :data:`AUTH_REQUIRED` when the anonymous read fails AND the
    credentialed one succeeds (so the endpoint is reachable and the
    credential was the only difference), and :data:`AUTH_UNKNOWN`
    otherwise — including the unreachable case, where a one-sided
    "the anonymous read failed" would otherwise be read as proof of
    authentication.
    """
    try:
        _dial_tools_list(endpoint, {}, timeout_seconds)
        return AUTH_OPEN
    except Exception:  # noqa: BLE001 — refusal or unreachable, decided below
        pass
    if auth_token_env is None:
        # Nothing to distinguish "refused us" from "refused everyone".
        return AUTH_UNKNOWN
    headers = _bearer_headers(auth_token_env, environ)
    try:
        _dial_tools_list(endpoint, headers, timeout_seconds)
    except Exception:  # noqa: BLE001 — both failed: unreachable
        return AUTH_UNKNOWN
    return AUTH_REQUIRED


def call_live_tool(
    endpoint: str,
    name: str,
    arguments: Mapping[str, Any],
    *,
    auth_token_env: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    timeout_seconds: float = 300.0,
) -> dict[str, Any]:
    """Call one MCP tool over streamable-HTTP and return its payload.

    The CLIENT half of the round trip: it invokes tools the server
    already registers and adds no capability. ``name`` must be one of
    :data:`SOURCE_ADMIN_TOOLS` — an out-of-surface tool name is an error
    here rather than a request the server happens to reject, so this
    helper cannot become a general escape hatch.
    """
    import asyncio
    import json

    if name not in set(SOURCE_ADMIN_TOOLS):
        raise ChatSourceAdminError(
            f"{name!r} is not one of the source-admin tools "
            f"{list(SOURCE_ADMIN_TOOLS)}. This surface binds the existing "
            "tools and introduces no source-administration capability of "
            "its own.",
            data={"tool": name, "reason": "out_of_surface"},
            fix_hint=f"call one of {list(SOURCE_ADMIN_TOOLS)}",
        )
    headers = _bearer_headers(auth_token_env, environ)
    _guard_dial(
        endpoint,
        credentialed=bool(headers),
        anonymous_probe=False,
    )

    async def _call() -> dict[str, Any]:
        from datetime import timedelta

        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(
            endpoint,
            headers=headers or None,
            timeout=timedelta(seconds=timeout_seconds),
        ) as (read_stream, write_stream, _get_session_id):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=timeout_seconds),
            ) as session:
                await session.initialize()
                # A program reads the envelope: ask for compact JSON
                # (issue #3059); the default result is text for models.
                result = await session.call_tool(
                    name,
                    arguments=dict(arguments),
                    meta={"okg/result_format": "json"},
                )
                structured = getattr(result, "structuredContent", None)
                if isinstance(structured, Mapping):
                    return dict(structured)
                for chunk in result.content or ():
                    text = getattr(chunk, "text", None)
                    if not text:
                        continue
                    try:
                        parsed = json.loads(text)
                    except ValueError:
                        continue
                    if isinstance(parsed, dict):
                        return parsed
                raise ChatSourceAdminError(
                    f"tool {name!r} returned no JSON envelope",
                    data={"tool": name, "reason": "no_envelope"},
                )

    return asyncio.run(_call())


def read_live_tools(
    endpoint: str,
    *,
    auth_token_env: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    timeout_seconds: float = 20.0,
    _guard: bool = True,
) -> LiveToolSurface:
    """Read ``tools/list`` from a streamable-HTTP MCP endpoint.

    The live half of the operational definition of the admitted surface.
    ``auth_token_env`` names an environment VARIABLE (never a literal
    credential); the value is resolved through ``mcp-http-auth``'s own
    :func:`~okg.substrate.mcp.http_auth.resolve_auth_token`, so an
    unresolvable credential fails closed here exactly as it does at the
    server.

    The returned surface's ``auth_posture`` is OBSERVED, not asserted:
    when a credential is supplied, the endpoint is additionally probed
    anonymously so the surface can record whether the server actually
    demands one (:func:`observe_auth_posture`).

    Deliberately not exercised end-to-end by the hermetic tests — they
    inject a :class:`LiveToolSurface` or stub :func:`_dial_tools_list`.
    A transport failure raises rather than yielding an empty surface.
    """
    headers = _bearer_headers(auth_token_env, environ)
    _guard_dial(
        endpoint,
        credentialed=bool(headers),
        anonymous_probe=not _guard,
    )
    try:
        names = _dial_tools_list(endpoint, headers, timeout_seconds)
    except Exception as exc:  # noqa: BLE001 — every transport failure
        # A failed readback is NOT an empty surface. Refuse loudly: an
        # empty tool set would flow into resolve_admin_tool_binding and
        # be refused there too, but with the wrong reason recorded.
        raise ChatSourceAdminError(
            f"tools/list readback of {endpoint!r} failed: {exc}. The "
            "admitted tool surface is a live property of the serving "
            "process; a failed readback yields no surface, never an "
            "empty one.",
            data={"endpoint": endpoint, "reason": "readback_failed"},
            fix_hint=(
                "check the endpoint is serving (okg mcp-serve --transport "
                "streamable-http ...) and that the bearer credential named "
                "by --auth-token-env matches"
            ),
        ) from exc
    posture = AUTH_UNKNOWN
    if auth_token_env is not None:
        posture = observe_auth_posture(
            endpoint,
            auth_token_env=auth_token_env,
            environ=environ,
            timeout_seconds=timeout_seconds,
        )
    from datetime import datetime, timezone

    return LiveToolSurface(
        endpoint=endpoint,
        tool_names=names,
        auth_posture=posture,
        read_back_at=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Tool binding
# ---------------------------------------------------------------------------

def _require_non_empty_surface(live: LiveToolSurface) -> frozenset[str]:
    observed = frozenset(live.tool_names)
    if not observed:
        raise ChatSourceAdminError(
            f"the tools/list readback of {live.endpoint!r} is EMPTY, so "
            "there is no admitted tool surface to bind against. An empty "
            "reference set is the absence of a readback, not the absence "
            "of tools: it would make every membership test pass while "
            "proving nothing.",
            data={"endpoint": live.endpoint, "reason": "empty_readback"},
            fix_hint=(
                "re-run the readback against the deployment's serving "
                "endpoint; a server with no registered tools is itself a "
                "defect"
            ),
        )
    return observed


def resolve_admin_tool_binding(
    *,
    live: LiveToolSurface,
    requested: Sequence[str] = (),
) -> tuple[str, ...]:
    """The admin preset's bound source-admin tool ids.

    Equals :data:`SOURCE_ADMIN_TOOLS` intersected with the LIVE readback,
    in ``SOURCE_ADMIN_TOOLS`` order. ``requested`` (whatever the manifest
    named) is checked against the same readback: a named tool absent from
    the live list raises with that tool named, and is never silently
    dropped from the bound set nor silently added to it.

    Refuses when the readback registers NONE of the nine: with
    ``allow_writes`` absent an explicit allowlist the nine are
    all-or-nothing, so a readback carrying none of them is a READ-ONLY
    endpoint, and an admin preset cannot be projected against it.
    """
    observed = _require_non_empty_surface(live)

    forbidden = sorted(set(requested) & FORBIDDEN_SOURCE_ADMIN_SURFACE)
    if forbidden:
        raise ChatSourceAdminError(
            f"the admin preset was asked to bind {forbidden}, which "
            "design.md §6 records as deliberately NOT replicated "
            "(runtime configuration editing, agent-spec editing, and the "
            "grader/Piazza/Mattermost/Redmine surfaces). deployment.yaml "
            "is canonical and audited; specs are versioned deployment "
            "content changed by PR.",
            data={"forbidden": forbidden, "endpoint": live.endpoint},
            fix_hint="remove the forbidden tool name(s) from the preset",
        )

    missing = sorted(set(requested) - observed)
    if missing:
        raise ChatSourceAdminError(
            f"the admin preset names tool(s) {missing} that the live "
            f"tools/list readback of {live.endpoint!r} does not carry. "
            f"The endpoint registers {sorted(observed)}. A named tool "
            "absent from the live surface is an error at sync time, "
            "never a silent drop.",
            data={
                "missing": missing,
                "endpoint": live.endpoint,
                "observed": sorted(observed),
            },
            fix_hint=(
                "launch the endpoint with --allow-writes (and, if set, an "
                "OKG_MCP_TOOL_ALLOWLIST that admits the tool), or stop "
                "naming it in the preset"
            ),
        )

    bound = live.source_admin_tools
    if not bound:
        raise ChatSourceAdminError(
            f"the live tools/list readback of {live.endpoint!r} registers "
            f"none of the {len(SOURCE_ADMIN_TOOLS)} source-admin tools "
            f"{list(SOURCE_ADMIN_TOOLS)}, so this is a READ-ONLY "
            "endpoint. The nine are all-or-nothing with allow_writes "
            "absent an explicit allowlist -- on a read-only server even "
            "list_sources is unregistered -- so the admin preset can only "
            "bind against the write-enabled admin endpoint.",
            data={
                "endpoint": live.endpoint,
                "observed": sorted(observed),
                "reason": "read_only_endpoint",
            },
            fix_hint=(
                "point the admin preset at the endpoint launched with "
                "--allow-writes"
            ),
        )
    return bound


def user_preset_tools(
    requested: Sequence[str],
    *,
    live: Optional[LiveToolSurface] = None,
) -> tuple[str, ...]:
    """The non-admin preset's tools. Refuses any source-admin tool.

    The non-admin preset binds NONE of the nine. Enforced here rather
    than left to the read-only endpoint's registration, because the
    projection must be wrong-configuration-proof on its own.
    """
    admin_named = sorted(set(requested) & set(SOURCE_ADMIN_TOOLS))
    if admin_named:
        raise ChatSourceAdminError(
            f"the non-admin preset names source-admin tool(s) "
            f"{admin_named}. Source administration is admin-group only "
            "(design.md §5 mechanism 4, §7 Plane 4); the non-admin preset "
            "binds none of the nine.",
            data={"admin_named": admin_named},
            fix_hint="move those tools to the admin preset",
        )
    if live is not None:
        observed = _require_non_empty_surface(live)
        missing = sorted(set(requested) - observed)
        if missing:
            raise ChatSourceAdminError(
                f"the non-admin preset names tool(s) {missing} absent "
                f"from the live tools/list readback of "
                f"{live.endpoint!r}.",
                data={"missing": missing, "endpoint": live.endpoint},
                fix_hint="remove the tool(s) or register them",
            )
    return tuple(requested)


# ---------------------------------------------------------------------------
# Transport precondition
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TransportDecision:
    """Whether an admin preset may be projected at this endpoint."""

    host: str
    permitted: bool
    authenticated: bool
    loopback: bool
    #: The SERVER's own loopback warning, carried verbatim. ``None`` when
    #: the server would not warn (authenticated).
    warning: Optional[str] = None
    unmet_precondition: Optional[str] = None


def admin_transport_precondition(
    *,
    host: str,
    auth_token_env: Optional[str] = None,
    auth_observed: bool = False,
    environ: Optional[Mapping[str, str]] = None,
    transport: str = "streamable-http",
) -> TransportDecision:
    """Decide the admin preset's transport precondition.

    Three outcomes, and only three:

    * **Authenticated**: permitted on any host. This is what
      ``mcp-http-auth`` bought. "Authenticated" means ``auth_observed``
      — the endpoint was OBSERVED to refuse an anonymous request while
      answering a credentialed one (see :func:`observe_auth_posture`).
      A resolvable
      ``auth_token_env`` is NOT that observation: the guard this
      function feeds says so in as many words, because a credential that
      resolved but was never wired would otherwise unlock the routable
      write surface.
    * **Loopback, unauthenticated**: permitted as the recorded interim,
      carrying the server's own warning verbatim in
      :attr:`TransportDecision.warning`. "Loopback-local to the
      instance" concretely means the chat instance shares the host
      network namespace; a container reaches host loopback only then.
    * **Non-loopback, unauthenticated**: NOT permitted. The caller must
      raise; a degraded admin preset that quietly omits the write tools
      while still presenting itself as the admin surface is the silent
      fallback the umbrella forbids program-wide.

    A named-but-unresolvable ``auth_token_env`` raises (mcp-http-auth's
    own fail-closed contract) rather than degrading to the unauthenticated
    arms — an operator who asked for authentication must never be handed
    a decision computed as if they had not.
    """
    # One definition of loopback semantics and one definition of the
    # warning text: both are the server's, imported rather than restated,
    # so the surface cannot drift from the guard it is reporting.
    from okg.substrate.mcp.server import (
        _write_transport_guard,
        is_loopback_host,
    )

    if auth_token_env is not None:
        from okg.substrate.mcp.http_auth import resolve_auth_token

        # Fail closed on an unresolvable credential. Resolving it proves
        # the OPERATOR has a token; it proves nothing about the SERVER.
        resolve_auth_token(auth_token_env, environ=environ)

    authenticated = bool(auth_observed)
    loopback = is_loopback_host(host)
    guard = _write_transport_guard(
        allow_writes=True,
        transport=transport,
        host=host,
        auth_active=authenticated,
    )
    if guard is None:
        return TransportDecision(
            host=host,
            permitted=True,
            authenticated=authenticated,
            loopback=loopback,
        )
    level, message = guard
    if level == "warn":
        return TransportDecision(
            host=host,
            permitted=True,
            authenticated=authenticated,
            loopback=loopback,
            warning=message,
        )
    return TransportDecision(
        host=host,
        permitted=False,
        authenticated=authenticated,
        loopback=loopback,
        unmet_precondition=message,
    )


# ---------------------------------------------------------------------------
# The instance readback: the vendor's real fields, at the real routes
# ---------------------------------------------------------------------------
#
# Three vendor facts, each MEASURED in the running v0.11.0 container, that
# an earlier form of this surface got wrong. Every one of them fails
# SILENTLY -- a wrong route prints nothing, a wrong field is dropped
# without complaint, and a truncated page just omits rows -- so each is
# encoded as a refusal rather than as a comment.

#: The WORKSPACE model listing. ``ModelAccessListResponse``
#: ``{"items": [...], "total": N}`` (routers/models.py:131-135,
#: models/models.py:167-169), whose items are ModelModel records carrying
#: ``meta`` and ``access_grants`` at the TOP level -- not nested under an
#: ``info`` key. This is the only route that carries either field.
WORKSPACE_MODEL_LIST_ROUTE = "/api/v1/models/list"

#: The OpenAI-compat route (main.py:840), which returns ``{"data": [...]}``
#: (main.py:887). Right for the VISIBILITY read -- what a session can SEE
#: -- and wrong for anything about ``meta`` or grants. ``jq '.[]'`` over
#: it yields nothing at all, which reads as "no grant found".
OPENAI_COMPAT_MODEL_ROUTE = "/api/v1/models"

#: ``PAGE_ITEM_COUNT`` at v0.11.0 (routers/models.py). The listing
#: PAGINATES, and its ``?query=`` filter matches ``Model.name``,
#: ``Model.base_model_id`` and the owning user's name/email/username --
#: never ``Model.id`` (models/models.py:349-357). So a preset cannot be
#: found by filtering on its id, and a single-page read silently omits
#: every preset past the first thirty. Both failures look like absence.
WORKSPACE_MODEL_LIST_PAGE_SIZE = 30

#: Per-model access at v0.11.0 is ``access_grants``: a list of
#: AccessGrantModel (models/access_grants.py:48-57). There is NO
#: ``access_control`` field, and ``ModelForm`` is declared
#: ``ConfigDict(extra='ignore')`` (models/models.py), so a body naming one
#: is accepted with HTTP 200 and the field is dropped on the floor.
ACCESS_GRANT_PRINCIPAL_TYPE = "group"
ACCESS_GRANT_PERMISSION = "read"

#: What a passing visibility readback does and does NOT bound. Open WebUI
#: keys its access-control bypass on the caller's ROLE, never on group
#: membership: BYPASS_ADMIN_ACCESS_CONTROL defaults True
#: (config.py:2062-2068) and utils/access_control/__init__.py:163 returns
#: True for any caller whose role is 'admin'. So the group grant bounds
#: NON-ADMIN visibility only, and no readback here may be cited as
#: evidence that an instance admin outside the deployment's admin group is
#: excluded -- that person sees the preset by vendor design.
VISIBILITY_BOUNDS_ROLE_NOT_GROUP = (
    "non-admin visibility only: Open WebUI bypasses per-model access "
    "control for any caller whose ROLE is admin (BYPASS_ADMIN_ACCESS_"
    "CONTROL defaults True), so an instance admin outside the "
    "deployment's admin group still sees this preset. Hiding a control "
    "is not denying it; enforcement lives at the MCP boundary."
)


def mcp_tool_id(server_id: str) -> str:
    """The ``meta.toolIds`` entry Open WebUI records for an MCP server.

    A preset's ``toolIds`` can NEVER contain individual tool names for an
    MCP server: the middleware matches the ``server:mcp:`` prefix and
    takes the WHOLE remainder as one server id
    (utils/middleware.py:2740,2742), then registers every tool that
    server advertises (:2758). A set-equality of ``toolIds`` against the
    nine source-admin names would fail on a correctly configured
    instance.

    The prefix is IMPORTED from the renderer that writes it rather than
    restated, so the two cannot drift apart.
    """
    from okg.substrate.chat.sync import MCP_TOOL_ID_PREFIX

    if not str(server_id).strip():
        raise ChatSourceAdminError(
            "an empty MCP server id renders as `server:mcp:` with nothing "
            "after it, which registers a connection that looks configured "
            "and resolves to no server.",
            data={"server_id": server_id, "reason": "empty_server_id"},
            fix_hint="pass the connection's info.id",
        )
    return f"{MCP_TOOL_ID_PREFIX}{server_id}"


def access_grant_set(group_ids: Mapping[str, str]) -> list[dict[str, str]]:
    """The per-model grant set, in the shape the instance stores.

    ``(principal_type, principal_id, permission)`` only: the server
    assigns each grant its own ``id`` and ``created_at``, so a comparison
    over raw grant rows would never converge. Group NAMES are the
    manifest's declaration; ``principal_id`` is the instance's group ID,
    which is why this takes a resolved name-to-id mapping rather than the
    names — emitting a name where the instance stores a uuid produces a
    readback that can never match and always reads as "no grant".
    """
    return sorted(
        (
            {
                "principal_type": ACCESS_GRANT_PRINCIPAL_TYPE,
                "principal_id": str(group_id),
                "permission": ACCESS_GRANT_PERMISSION,
            }
            for group_id in group_ids.values()
        ),
        key=lambda grant: grant["principal_id"],
    )


def parse_workspace_model_listing(
    pages: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Every row of a ``GET /api/v1/models/list`` read, or a refusal.

    Refuses each way this read is silently wrong:

    * a BARE LIST payload -- neither route returns one, so a caller
      holding one read something else;
    * the OpenAI-compat ``{"data": [...]}`` shape, which carries no
      ``meta`` and no ``access_grants`` at all;
    * a page with no ``total``, which cannot be shown complete; and
    * a TRUNCATED read -- fewer rows than ``total``. The listing
      paginates at :data:`WORKSPACE_MODEL_LIST_PAGE_SIZE` and its
      ``?query=`` filter does not match ``Model.id``, so the preset a
      caller is looking for can sit on page two and read as absent.
    """
    if not pages:
        raise ChatSourceAdminError(
            f"no {WORKSPACE_MODEL_LIST_ROUTE} page was supplied, so there "
            "is no listing to read. An unread listing is not an empty one.",
            data={"reason": "no_pages"},
            fix_hint=f"GET {WORKSPACE_MODEL_LIST_ROUTE}?page=1",
        )
    rows: list[Mapping[str, Any]] = []
    total: Optional[int] = None
    for index, page in enumerate(pages):
        if isinstance(page, (list, tuple)):
            raise ChatSourceAdminError(
                f"page {index + 1} is a bare list. Neither model route "
                f"returns one: {WORKSPACE_MODEL_LIST_ROUTE} returns "
                f"{{items, total}} and {OPENAI_COMPAT_MODEL_ROUTE} returns "
                "{data: [...]}, so `jq '.[]'` over either yields nothing "
                "and every lookup reads as absent.",
                data={"reason": "bare_list_payload", "page": index + 1},
                fix_hint=f"read {WORKSPACE_MODEL_LIST_ROUTE} and use .items",
            )
        if not isinstance(page, Mapping):
            raise ChatSourceAdminError(
                f"page {index + 1} is not a JSON object.",
                data={"reason": "not_an_object", "page": index + 1},
            )
        if "items" not in page and "data" in page:
            raise ChatSourceAdminError(
                f"page {index + 1} carries `data`, which is the "
                f"OpenAI-compat {OPENAI_COMPAT_MODEL_ROUTE} shape "
                "(main.py:840,887). That route carries neither `meta` nor "
                "`access_grants`, so no grant or tool-binding assertion "
                "can be made from it.",
                data={"reason": "openai_compat_route", "page": index + 1},
                fix_hint=(
                    f"read the workspace listing {WORKSPACE_MODEL_LIST_ROUTE}"
                ),
            )
        if "items" not in page:
            raise ChatSourceAdminError(
                f"page {index + 1} carries no `items`, so it is not a "
                f"{WORKSPACE_MODEL_LIST_ROUTE} response.",
                data={"reason": "no_items", "page": index + 1},
            )
        if page.get("total") is None:
            raise ChatSourceAdminError(
                f"page {index + 1} carries no `total`, so the read cannot "
                "be shown complete. A listing that might be truncated is "
                "not evidence of absence.",
                data={"reason": "no_total", "page": index + 1},
            )
        page_total = int(page["total"])
        if total is not None and page_total != total:
            raise ChatSourceAdminError(
                f"page {index + 1} reports total {page_total} where an "
                f"earlier page reported {total}. The listing changed under "
                "the read, so the pages do not compose into one snapshot.",
                data={
                    "reason": "total_disagreement",
                    "page": index + 1,
                    "totals": [total, page_total],
                },
            )
        total = page_total
        rows.extend(
            row for row in (page.get("items") or ()) if isinstance(row, Mapping)
        )
    assert total is not None  # every page was checked above
    if len(rows) != total:
        raise ChatSourceAdminError(
            f"the {WORKSPACE_MODEL_LIST_ROUTE} read carries {len(rows)} of "
            f"{total} rows, so it is TRUNCATED. The listing paginates at "
            f"{WORKSPACE_MODEL_LIST_PAGE_SIZE} and its `?query=` filter "
            "matches name/base_model_id/owner -- never Model.id -- so a "
            "preset past the first page reads as absent rather than as "
            "unread.",
            data={
                "reason": "listing_truncated",
                "seen": len(rows),
                "total": total,
                "page_size": WORKSPACE_MODEL_LIST_PAGE_SIZE,
            },
            fix_hint=(
                "read every page (?page=1, 2, ...) and pass them all; do "
                "not filter with ?query=<model id>"
            ),
        )
    return tuple(rows)


def parse_openai_compat_model_ids(payload: Any) -> tuple[str, ...]:
    """The model ids a session can SEE, from ``GET /api/v1/models``.

    The visibility read, and only that. It is the route a chat session's
    picker is populated from, and it is NOT the route to read grants or
    ``meta`` from — see :func:`parse_workspace_model_listing`.
    """
    if isinstance(payload, (list, tuple)):
        raise ChatSourceAdminError(
            f"{OPENAI_COMPAT_MODEL_ROUTE} returns "
            "{'data': [...]} (main.py:887), not a bare list. Reading it "
            "as an array yields nothing and makes EVERY preset look "
            "absent — a pass by absence.",
            data={"reason": "bare_list_payload"},
            fix_hint="read the response as .data[].id",
        )
    if not isinstance(payload, Mapping):
        raise ChatSourceAdminError(
            f"{OPENAI_COMPAT_MODEL_ROUTE} returned no JSON object.",
            data={"reason": "not_an_object"},
        )
    if "data" not in payload and "items" in payload:
        raise ChatSourceAdminError(
            "this payload carries `items`, which is the workspace listing "
            f"{WORKSPACE_MODEL_LIST_ROUTE} shape. The visibility claim is "
            f"about what a session SEES, which is {OPENAI_COMPAT_MODEL_ROUTE}.",
            data={"reason": "workspace_listing_route"},
        )
    if "data" not in payload:
        raise ChatSourceAdminError(
            f"{OPENAI_COMPAT_MODEL_ROUTE} response carries no `data`.",
            data={"reason": "no_data"},
        )
    return tuple(
        str(row.get("id"))
        for row in (payload.get("data") or ())
        if isinstance(row, Mapping) and row.get("id") is not None
    )


@dataclass(frozen=True)
class PresetReadbackVerdict:
    """The two assertions the acceptance criterion actually names."""

    admin_preset_id: str
    #: (i) what the wire carries, and what it should carry.
    meta_tool_ids: tuple[str, ...]
    expected_meta_tool_ids: tuple[str, ...]
    meta_tool_ids_ok: bool
    #: (ii) SOURCE_ADMIN_TOOLS ∩ the live readback, after the
    #: connection's narrowing. NON-EMPTY or the verdict is a refusal.
    admitted_tools: tuple[str, ...]
    admitted_count: int
    reachable_tools_ok: bool
    #: The non-admin preset carries none of the admin server's tool ids.
    user_preset_clean: bool
    grant_ok: bool
    positive_control_grants: int
    ok: bool
    visibility_bounds: str = VISIBILITY_BOUNDS_ROLE_NOT_GROUP
    narrowing_observed: bool = False
    findings: tuple[str, ...] = ()


def verify_admin_preset_readback(
    *,
    pages: Sequence[Mapping[str, Any]],
    admin_preset_id: str,
    mcp_server_id: str,
    live_tool_names: Sequence[str],
    admin_group_id: str,
    positive_control_preset_id: str,
    connection_tool_names: Optional[Sequence[str]] = None,
    user_preset_id: Optional[str] = None,
    connection_filter_list: Optional[Sequence[str]] = None,
) -> PresetReadbackVerdict:
    """Score one ``/api/v1/models/list`` readback of the admin preset.

    TWO assertions, because one of them cannot hold on its own:

    (i) the admin preset's ``meta.toolIds`` equal exactly the expected
    ``server:mcp:<server_id>`` entries, and the non-admin preset's carry
    none of them. Individual tool names never appear here.

    (ii) the tool set reachable THROUGH that server id equals
    :data:`SOURCE_ADMIN_TOOLS` intersected with the live ``tools/list``.
    These are TWO INDEPENDENT READS — ``connection_tool_names`` is what
    the instance's own MCP connection advertises, ``live_tool_names`` is
    the authenticated readback of the deployment's write-enabled
    endpoint — and both are narrowed by the connection's
    ``function_name_filter_list``, the only per-connection lever
    (utils/middleware.py:2237-2243). Comparing the live readback to
    itself would be a tautology, so an UNREAD instance-side set scores
    this arm False with the reason named, never True by default. A
    connection quietly pointed at the read-only endpoint is exactly what
    it catches.

    The admitted subset must be NON-EMPTY, and its member count is
    recorded. An empty intersection is REFUSED rather than scored: it is
    the read-only-endpoint reading, and `empty == empty` would report an
    admin surface binding zero tools as correct.

    Every absence claim carries its own positive control:
    ``positive_control_preset_id`` must come back with a NON-EMPTY grant
    set, or the readback itself is broken and an empty grant set
    elsewhere means nothing.
    """
    rows = parse_workspace_model_listing(pages)
    by_id = {str(row.get("id")): row for row in rows}
    findings: list[str] = []

    admin_row = by_id.get(admin_preset_id)
    if admin_row is None:
        raise ChatSourceAdminError(
            f"the admin preset {admin_preset_id!r} is absent from a "
            f"COMPLETE {WORKSPACE_MODEL_LIST_ROUTE} read of "
            f"{len(rows)} row(s). This criterion asserts what the preset "
            "binds, so an absent preset is a failed readback, not a "
            "passing one.",
            data={
                "reason": "admin_preset_absent",
                "preset_id": admin_preset_id,
                "observed": sorted(by_id),
            },
            fix_hint="run `okg chat sync` for this deployment first",
        )

    control_row = by_id.get(positive_control_preset_id)
    if control_row is None:
        raise ChatSourceAdminError(
            f"the positive control preset {positive_control_preset_id!r} "
            f"is absent from the {WORKSPACE_MODEL_LIST_ROUTE} read, so no "
            "absence observed through this query may be read as a result.",
            data={
                "reason": "positive_control_absent",
                "preset_id": positive_control_preset_id,
            },
        )
    if "access_grants" not in control_row:
        raise ChatSourceAdminError(
            f"the positive control row {positive_control_preset_id!r} "
            "carries no `access_grants` FIELD. The readback is reading the "
            "wrong route or the wrong shape; an empty grant set anywhere "
            "in this run proves nothing.",
            data={
                "reason": "positive_control_field_missing",
                "observed_keys": sorted(control_row),
            },
            fix_hint=(
                f"read {WORKSPACE_MODEL_LIST_ROUTE}; `access_control` does "
                "not exist at v0.11.0"
            ),
        )
    control_grants = [
        grant
        for grant in (control_row.get("access_grants") or ())
        if isinstance(grant, Mapping)
    ]
    if not control_grants:
        raise ChatSourceAdminError(
            f"the positive control preset {positive_control_preset_id!r} "
            "came back with an EMPTY grant set. It is supposed to be a "
            "preset KNOWN to carry a grant, so the readback itself is "
            "broken and no 'no grant' result from this run is a result.",
            data={
                "reason": "positive_control_empty",
                "preset_id": positive_control_preset_id,
            },
        )

    expected_meta = (mcp_tool_id(mcp_server_id),)
    meta = admin_row.get("meta")
    if not isinstance(meta, Mapping) or "toolIds" not in meta:
        observed_meta: tuple[str, ...] = ()
        meta_ok = False
        findings.append(
            f"the admin preset row carries no `meta.toolIds`, so what it "
            f"binds was NOT READ (keys: {sorted(admin_row)})"
        )
    else:
        observed_meta = tuple(str(entry) for entry in (meta.get("toolIds") or ()))
        meta_ok = set(observed_meta) == set(expected_meta)
        if not meta_ok:
            findings.append(
                f"the admin preset's meta.toolIds are {list(observed_meta)}, "
                f"expected {list(expected_meta)}"
            )

    user_clean = True
    if user_preset_id is not None:
        user_row = by_id.get(user_preset_id)
        if user_row is None:
            user_clean = False
            findings.append(
                f"the non-admin preset {user_preset_id!r} is absent from "
                "the listing, so 'it binds none of them' was not read"
            )
        else:
            user_meta = user_row.get("meta")
            if not isinstance(user_meta, Mapping) or "toolIds" not in user_meta:
                user_clean = False
                findings.append(
                    f"the non-admin preset {user_preset_id!r} carries no "
                    "`meta.toolIds`, so its binding was NOT READ"
                )
            else:
                shared = sorted(
                    set(str(e) for e in (user_meta.get("toolIds") or ()))
                    & set(expected_meta)
                )
                user_clean = not shared
                if shared:
                    findings.append(
                        f"the non-admin preset {user_preset_id!r} binds the "
                        f"admin MCP server {shared}"
                    )

    observed_live = tuple(str(name) for name in live_tool_names)
    if not observed_live:
        raise ChatSourceAdminError(
            "the live tools/list readback is EMPTY, so there is no "
            "reachable tool set to compare the binding against.",
            data={"reason": "empty_readback", "endpoint": mcp_server_id},
        )
    narrowing = [
        str(entry) for entry in (connection_filter_list or ())
        if str(entry).strip()
    ]
    if connection_filter_list is None:
        findings.append(
            "the connection's function_name_filter_list was NOT READ, so "
            "the narrowing arm of this assertion was not exercised; the "
            "reachable set is the whole advertised set"
        )

    def _narrow(names: Sequence[str]) -> tuple[str, ...]:
        if not narrowing:
            return tuple(str(name) for name in names)
        allowed = set(narrowing)
        return tuple(str(name) for name in names if str(name) in allowed)

    admitted = tuple(
        name for name in SOURCE_ADMIN_TOOLS if name in set(_narrow(observed_live))
    )
    if not admitted:
        raise ChatSourceAdminError(
            "the admitted source-admin subset is EMPTY: none of "
            f"{list(SOURCE_ADMIN_TOOLS)} is reachable through MCP server "
            f"{mcp_server_id!r}. An empty admitted subset does NOT satisfy "
            "this criterion -- it is the read-only-endpoint reading, and "
            "closing on it would let an admin surface binding ZERO tools "
            "pass as correct.",
            data={
                "reason": "empty_admitted_subset",
                "server_id": mcp_server_id,
                "reachable": sorted(_narrow(observed_live)),
            },
            fix_hint=(
                "point the admin preset's connection at the endpoint "
                "launched with --allow-writes"
            ),
        )

    if connection_tool_names is None:
        reachable_ok = False
        findings.append(
            "the tool set the INSTANCE's connection advertises was NOT "
            "READ, so assertion (ii) was not made. The admitted subset "
            f"{list(admitted)} comes from the live tools/list alone, and "
            "comparing that read to itself proves nothing -- a connection "
            "quietly pointed at the read-only endpoint would pass."
        )
    else:
        through_server = tuple(
            name
            for name in SOURCE_ADMIN_TOOLS
            if name in set(_narrow(connection_tool_names))
        )
        reachable_ok = set(through_server) == set(admitted)
        if not reachable_ok:
            findings.append(
                f"the tools reachable through MCP server {mcp_server_id!r} "
                f"are {list(through_server)}, which is not the admitted "
                f"subset {list(admitted)} of the live tools/list readback"
            )

    grant_ok = False
    if "access_grants" not in admin_row:
        findings.append(
            "the admin preset row carries no `access_grants` FIELD, so its "
            "grant was NOT READ. `access_control` does not exist at "
            "v0.11.0 and a body naming it is dropped by ModelForm's "
            "extra='ignore'."
        )
    else:
        want = {
            "principal_type": ACCESS_GRANT_PRINCIPAL_TYPE,
            "principal_id": str(admin_group_id),
            "permission": ACCESS_GRANT_PERMISSION,
        }
        observed_grants = [
            {
                "principal_type": str(grant.get("principal_type")),
                "principal_id": str(grant.get("principal_id")),
                "permission": str(grant.get("permission")),
            }
            for grant in (admin_row.get("access_grants") or ())
            if isinstance(grant, Mapping)
        ]
        grant_ok = want in observed_grants
        if not grant_ok:
            findings.append(
                f"the admin preset's grants are {observed_grants}, which do "
                f"not include the admin group grant {want}"
            )

    findings.append(VISIBILITY_BOUNDS_ROLE_NOT_GROUP)
    return PresetReadbackVerdict(
        admin_preset_id=admin_preset_id,
        meta_tool_ids=observed_meta,
        expected_meta_tool_ids=expected_meta,
        meta_tool_ids_ok=meta_ok,
        admitted_tools=admitted,
        admitted_count=len(admitted),
        reachable_tools_ok=reachable_ok,
        user_preset_clean=user_clean,
        grant_ok=grant_ok,
        positive_control_grants=len(control_grants),
        ok=bool(meta_ok and reachable_ok and user_clean and grant_ok),
        narrowing_observed=connection_filter_list is not None,
        findings=tuple(findings),
    )


# ---------------------------------------------------------------------------
# The projection
# ---------------------------------------------------------------------------

#: What the interim buildable on ``mcp-http-auth`` alone actually proves.
#: Never "admin-only enforcement".
ENFORCEMENT_INTERIM_LABEL = "transport-plus-visibility"

#: The child that owns the per-caller half. Named so the blocked arm is
#: attributable rather than merely absent.
PER_CALLER_ARM_BLOCKED_ON = "mcp-principal-mapping"


@dataclass(frozen=True)
class AdminPresetProjection:
    """A complete admin preset. There is no partial one.

    Constructed only by :func:`project_admin_preset`, which is the only
    place the transport precondition and the forbidden-surface check
    run. Nothing stops a caller from instantiating this dataclass
    directly — it is a value, not a capability — so the guarantee is
    "every projection this module produces is checked", not "no such
    value can exist".
    """

    deployment: str
    preset_id: str
    #: The source-admin tool NAMES the endpoint admits. NOT the wire's
    #: ``meta.toolIds``, which carry ``server:mcp:<server_id>`` and never
    #: an individual tool name — see :func:`mcp_tool_id`.
    bound_tools: tuple[str, ...]
    access_group_names: tuple[str, ...]
    endpoint: str
    transport: TransportDecision
    #: The instance group NAME -> ID mapping, resolved by the renderer
    #: against the live instance. ``None`` means UNRESOLVED: the grant
    #: set cannot be rendered, and :meth:`to_dict` says so by name rather
    #: than emitting names where the instance stores uuids.
    access_group_ids: Optional[Mapping[str, str]] = None
    #: The MCP connection's ``info.id``. ``None`` means the wire form of
    #: the binding is unresolved; it is never guessed.
    mcp_server_id: Optional[str] = None
    live_tools_read_back_at: Optional[str] = None
    enforcement_label: str = ENFORCEMENT_INTERIM_LABEL
    #: Always False in the interim. The per-caller typed refusal is a
    #: separate arm; visibility evidence never closes it.
    enforcement_sufficient: bool = False
    blocked_arms: tuple[str, ...] = (
        f"per-caller typed refusal (blocked on {PER_CALLER_ARM_BLOCKED_ON})",
    )

    def to_dict(self) -> dict[str, Any]:
        """The readback-comparable shape. Pure data; nothing persisted.

        Every key here names a field the instance ACTUALLY has at
        v0.11.0, because a payload naming one it does not is accepted
        with HTTP 200 and dropped: ``ModelForm`` is declared
        ``ConfigDict(extra='ignore')``. The earlier form of this method
        emitted ``access_control``, which does not exist at this version
        (per-model access is ``access_grants``, models/access_grants.py:
        48-57) — a readback comparing against it would have found the
        field missing on every run and read that as "no grant declared".

        Two fields are emitted ONLY when they can be rendered truthfully:

        * ``access_grants`` needs the instance's group IDs. Group NAMES
          are what the manifest declares (``chat.access.groups``); the
          instance stores uuids, so emitting names as ``principal_id``
          would produce a comparison that can never match. Unresolved,
          the key is ABSENT and ``access_grants_unresolved`` says why —
          absent is NOT-READ, which is how the renderer's own readback
          distinguishes it from "no grants".
        * ``meta_tool_ids`` needs the MCP connection's ``info.id``.
        """
        payload: dict[str, Any] = {
            "deployment": self.deployment,
            "preset_id": self.preset_id,
            "bound_tools": list(self.bound_tools),
            "bound_tool_count": len(self.bound_tools),
            "access_group_names": list(self.access_group_names),
            "endpoint": self.endpoint,
            "transport": {
                "host": self.transport.host,
                "authenticated": self.transport.authenticated,
                "loopback": self.transport.loopback,
                "loopback_warning": self.transport.warning,
            },
            "enforcement_label": self.enforcement_label,
            "enforcement_sufficient": self.enforcement_sufficient,
            "visibility_bounds": VISIBILITY_BOUNDS_ROLE_NOT_GROUP,
            "blocked_arms": list(self.blocked_arms),
        }
        if self.access_group_ids is None:
            payload["access_grants_unresolved"] = (
                "the instance group IDs for "
                f"{list(self.access_group_names)} were not resolved, so no "
                "access_grants entry can be rendered. principal_id is a "
                "group ID at v0.11.0; a name there never matches."
            )
        else:
            payload["access_grants"] = access_grant_set(self.access_group_ids)
        if self.mcp_server_id is None:
            payload["meta_tool_ids_unresolved"] = (
                "the MCP connection's info.id was not supplied, so the "
                "wire form (`server:mcp:<server_id>`) of this binding is "
                "unknown. It is never guessed: `server:mcp:None` looks "
                "configured and resolves to nothing."
            )
        else:
            payload["meta_tool_ids"] = [mcp_tool_id(self.mcp_server_id)]
        return payload


def require_admin_transport(
    *,
    deployment: str,
    preset_id: str,
    endpoint: str,
    auth_token_env: Optional[str] = None,
    auth_observed: bool = False,
    environ: Optional[Mapping[str, str]] = None,
    host: Optional[str] = None,
) -> TransportDecision:
    """The transport precondition as a REFUSAL, not a boolean.

    Called before anything dials the endpoint, so a non-loopback
    unauthenticated endpoint is refused for the reason it is actually
    refused for — not for whatever the connection attempt happened to
    return. Called again inside :func:`project_admin_preset`; the
    dialing functions enforce the narrower :func:`_guard_dial` rule (no
    uncredentialed dial to a routable host), so no entry point in this
    module reaches the network unguarded.
    """
    resolved_host = host if host is not None else _host_of(endpoint)
    decision = admin_transport_precondition(
        host=resolved_host,
        auth_token_env=auth_token_env,
        auth_observed=auth_observed,
        environ=environ,
    )
    if decision.permitted:
        return decision
    raise ChatSourceAdminError(
        "refusing to project the admin preset for deployment "
        f"{deployment!r}: {decision.unmet_precondition} The admin "
        "preset binds write and admin tools, so it requires either an "
        "authenticated transport (--auth-token-env NAME, PACT "
        "mcp-http-auth) or an endpoint loopback-local to the instance. "
        "No degraded admin preset is projected: a preset that quietly "
        "omitted the write tools while still presenting itself as the "
        "admin surface would be a silent fallback.",
        data={
            "deployment": deployment,
            "preset_id": preset_id,
            "endpoint": endpoint,
            "host": resolved_host,
            "unmet_precondition": "authenticated_or_loopback_transport",
        },
        fix_hint=(
            "serve the admin endpoint with --auth-token-env "
            "OKG_MCP_AUTH_TOKEN, or bind --host 127.0.0.1 and run the "
            "chat instance in the host network namespace"
        ),
    )


def project_admin_preset(
    *,
    deployment: str,
    preset_id: str,
    live: LiveToolSurface,
    access_group_names: Sequence[str],
    auth_token_env: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    requested_tools: Sequence[str] = (),
    host: Optional[str] = None,
    access_group_ids: Optional[Mapping[str, str]] = None,
    mcp_server_id: Optional[str] = None,
) -> AdminPresetProjection:
    """Render the admin preset, or raise.

    Pure: builds a value, touches no filesystem, holds no source state.
    The deployment manifest remains the only source of registry truth.

    Order matters. The transport precondition is checked FIRST, so a
    non-loopback unauthenticated endpoint is refused before any tool
    binding is computed — there is no code path that produces a
    tool-bearing preset and then discards part of it.

    ``auth_observed`` comes from :attr:`LiveToolSurface.authenticated`,
    which the readback sets only when the endpoint was OBSERVED to
    refuse an unauthenticated request. A preset cannot be projected on
    the strength of a credential the operator merely holds.
    """
    decision = require_admin_transport(
        deployment=deployment,
        preset_id=preset_id,
        endpoint=live.endpoint,
        auth_token_env=auth_token_env,
        auth_observed=live.authenticated,
        environ=environ,
        host=host,
    )

    groups = tuple(str(name) for name in access_group_names if str(name).strip())
    if not groups:
        raise ChatSourceAdminError(
            f"refusing to project the admin preset {preset_id!r} with no "
            "group grant. Open WebUI per-model access control naming no "
            "group is not 'admins only'; an empty declared set is not a "
            "restriction, and the preset would be visible to everyone.",
            data={"deployment": deployment, "preset_id": preset_id},
            fix_hint="declare chat.access.groups with the admin group",
        )

    bound_tools = resolve_admin_tool_binding(
        live=live, requested=requested_tools,
    )
    _assert_no_forbidden_surface(bound_tools)
    resolved_ids: Optional[Mapping[str, str]] = None
    if access_group_ids is not None:
        unresolved = sorted(set(groups) - set(access_group_ids))
        if unresolved:
            raise ChatSourceAdminError(
                f"the declared admin group(s) {unresolved} have no instance "
                "group ID in the supplied mapping, so their grant cannot be "
                "rendered. A grant naming a group NAME where the instance "
                "stores a uuid never matches anything and reads as no "
                "grant at all -- which is not 'admins only', it is "
                "everyone.",
                data={
                    "deployment": deployment,
                    "preset_id": preset_id,
                    "unresolved_groups": unresolved,
                    "reason": "unresolved_group_ids",
                },
                fix_hint=(
                    "resolve the names against GET /api/v1/groups/ before "
                    "projecting, or create the group on the instance"
                ),
            )
        resolved_ids = {name: str(access_group_ids[name]) for name in groups}
    return AdminPresetProjection(
        deployment=deployment,
        preset_id=preset_id,
        bound_tools=bound_tools,
        access_group_names=groups,
        endpoint=live.endpoint,
        transport=decision,
        access_group_ids=resolved_ids,
        mcp_server_id=mcp_server_id,
        live_tools_read_back_at=live.read_back_at,
    )


def _assert_no_forbidden_surface(tool_ids: Iterable[str]) -> None:
    if not FORBIDDEN_SOURCE_ADMIN_SURFACE:  # pragma: no cover - defensive
        raise ChatSourceAdminError(
            "the forbidden-surface set is EMPTY, so intersecting the "
            "rendered tool set against it proves nothing.",
            data={"reason": "empty_forbidden_set"},
        )
    bound_forbidden = sorted(set(tool_ids) & FORBIDDEN_SOURCE_ADMIN_SURFACE)
    if bound_forbidden:
        raise ChatSourceAdminError(
            f"the rendered tool set binds {bound_forbidden}, which "
            "design.md §6 records as deliberately not replicated.",
            data={"forbidden": bound_forbidden},
        )


def _host_of(endpoint: str) -> str:
    """The host of an MCP endpoint URL, for the transport precondition."""
    from urllib.parse import urlsplit

    parsed = urlsplit(endpoint)
    host = parsed.hostname
    if not host:
        raise ChatSourceAdminError(
            f"the MCP endpoint {endpoint!r} has no host, so its transport "
            "posture cannot be decided. A posture that cannot be decided "
            "is refused, never assumed safe.",
            data={"endpoint": endpoint, "reason": "no_host"},
            fix_hint="declare the endpoint as an absolute http(s) URL",
        )
    return host


# ---------------------------------------------------------------------------
# Admin gating: the interim evidence, honestly scored
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GatingEvidence:
    """Everything the transport-plus-visibility interim can observe."""

    #: The non-admin session's GET /api/v1/models response, as model ids.
    #: That route is the OpenAI-compat one and returns ``{"data": [...]}``
    #: (main.py:840,887) — see :func:`parse_openai_compat_model_ids`, which
    #: is how this tuple is meant to be produced.
    non_admin_model_ids: tuple[str, ...]
    admin_preset_id: str
    #: THE POSITIVE CONTROL for the visibility absence claim: a preset the
    #: non-admin IS entitled to see. Without it an empty, errored, or
    #: unauthenticated listing scores as "the admin preset is hidden",
    #: which is an absence observed through a query that could not have
    #: shown presence.
    non_admin_entitled_preset_id: str
    #: An AUTHENTICATED tools/list readback of the READ-ONLY endpoint.
    readonly_surface: LiveToolSurface
    #: HTTP status the admin endpoint returned for the non-admin token.
    admin_endpoint_status: Optional[int]
    #: sha256 of source_registry.yaml before / after, taken at the path
    #: the SERVER reports (list_sources' registry_path), not one the test
    #: recomputes.
    registry_digest_before: str
    registry_digest_after: str
    registry_path_reported_by_server: str
    label: str = ENFORCEMENT_INTERIM_LABEL


@dataclass(frozen=True)
class GatingVerdict:
    label: str
    visibility_ok: bool
    tools_absent_on_readonly_endpoint: bool
    transport_refusal_ok: bool
    registry_unchanged: bool
    #: ALWAYS False while the per-caller arm is blocked. Visibility and
    #: transport evidence never close it.
    requirement_satisfied: bool
    per_caller_arm: str
    #: Was the visibility absence observed through a query that could
    #: have shown presence? False makes ``visibility_ok`` meaningless.
    visibility_positive_control_ok: bool = False
    #: What a passing visibility arm bounds, carried on the verdict so a
    #: reader cannot cite it against non-group INSTANCE admins.
    visibility_bounds: str = VISIBILITY_BOUNDS_ROLE_NOT_GROUP
    findings: tuple[str, ...] = ()


def evaluate_gating_evidence(evidence: GatingEvidence) -> GatingVerdict:
    """Score the interim evidence without overclaiming.

    Every arm is checked against its own absence case: an EMPTY
    read-only ``tools/list`` readback is "unavailable", not "the nine are
    absent"; a missing HTTP status is "not observed", not "refused"; a
    digest taken at a path the server did not report is not evidence
    about the registry the server writes.
    """
    findings: list[str] = []

    if evidence.label != ENFORCEMENT_INTERIM_LABEL:
        raise ChatSourceAdminError(
            f"gating evidence must be labeled "
            f"{ENFORCEMENT_INTERIM_LABEL!r}; observed "
            f"{evidence.label!r}. Mislabeling the interim as full "
            "enforcement is the failure this requirement exists to "
            "prevent.",
            data={"label": evidence.label},
        )

    listed = set(evidence.non_admin_model_ids)
    # THE POSITIVE CONTROL, checked BEFORE the absence is scored. An
    # empty, errored, or unauthenticated listing contains no preset id at
    # all, so "the admin preset is not in it" would be true of a response
    # that could never have shown any preset.
    control_ok = evidence.non_admin_entitled_preset_id in listed
    if not control_ok:
        findings.append(
            "the non-admin model listing does not contain the entitled "
            f"preset {evidence.non_admin_entitled_preset_id!r}, so this "
            "listing could not have shown the admin preset either. An "
            "absence observed through such a query is not a result."
        )
    hidden = evidence.admin_preset_id not in listed
    if not hidden:
        findings.append(
            f"the non-admin model listing contains the admin preset "
            f"{evidence.admin_preset_id!r}"
        )
    visibility_ok = hidden and control_ok

    observed = set(evidence.readonly_surface.tool_names)
    if not observed:
        tools_absent = False
        findings.append(
            "the read-only endpoint's tools/list readback is EMPTY, which "
            "is an unavailable readback, not proof the nine are absent"
        )
    else:
        present = sorted(observed & set(SOURCE_ADMIN_TOOLS))
        tools_absent = not present
        if present:
            findings.append(
                f"the read-only endpoint registers source-admin tool(s) "
                f"{present}"
            )

    transport_refusal_ok = evidence.admin_endpoint_status == 401
    if evidence.admin_endpoint_status is None:
        findings.append(
            "no HTTP status was observed at the admin endpoint, so the "
            "transport refusal was not exercised"
        )
    elif not transport_refusal_ok:
        findings.append(
            f"the admin endpoint answered {evidence.admin_endpoint_status} "
            "to the non-admin token, not 401"
        )

    if not evidence.registry_path_reported_by_server.strip():
        registry_unchanged = False
        findings.append(
            "the registry digests were taken at no server-reported path, "
            "so they are not evidence about the registry the server writes"
        )
    else:
        registry_unchanged = (
            evidence.registry_digest_before == evidence.registry_digest_after
        )
        if not registry_unchanged:
            findings.append(
                "the source registry changed across the refused call: "
                f"{evidence.registry_digest_before} -> "
                f"{evidence.registry_digest_after}"
            )

    findings.append(
        "the per-caller typed refusal (an authenticated but unauthorized "
        f"PERSON refused by name and reason) requires "
        f"{PER_CALLER_ARM_BLOCKED_ON}; this evidence does not close it"
    )
    findings.append(VISIBILITY_BOUNDS_ROLE_NOT_GROUP)
    return GatingVerdict(
        label=evidence.label,
        visibility_ok=visibility_ok,
        tools_absent_on_readonly_endpoint=tools_absent,
        transport_refusal_ok=transport_refusal_ok,
        registry_unchanged=registry_unchanged,
        # Never True here. The requirement's second layer is per-caller
        # enforcement, and hiding a control is not denying it.
        requirement_satisfied=False,
        per_caller_arm=f"blocked:{PER_CALLER_ARM_BLOCKED_ON}",
        visibility_positive_control_ok=control_ok,
        findings=tuple(findings),
    )


def digest_registry_file(path: str | Path) -> str:
    """sha256 of the registry file at a path the SERVER reported."""
    resolved = Path(path)
    if not resolved.is_file():
        raise ChatSourceAdminError(
            f"no source registry file at {resolved}. A missing file has "
            "no digest; digesting it as the empty string would make two "
            "missing files compare equal and read as 'unchanged'.",
            data={"path": str(resolved), "reason": "missing"},
        )
    return hashlib.sha256(resolved.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# The two-phase setup flow
# ---------------------------------------------------------------------------

#: ``commit_setup`` returns this when the in-process plan is gone (30
#: minute TTL, or an MCP-server restart). It MUST surface as a re-propose
#: prompt.
COMMIT_PLAN_NOT_FOUND = "plan_not_found"

_COMMIT_SUCCESS_STATUSES = frozenset({
    "ingest_complete",
    "ingest_complete_no_rows",
    "ok_already_configured",
})
_COMMIT_FAILURE_STATUSES = frozenset({
    "activation_blocked",
    "ingest_failed",
    "rollback_failed",
})


@dataclass(frozen=True)
class CommitOutcome:
    """What ``commit_setup`` actually did, read without guessing."""

    status: str
    succeeded: bool
    re_propose_required: bool
    prompt: Optional[str] = None
    #: None means UNKNOWN, never zero. ``add_source`` returns a
    #: count-less envelope when it cannot attribute rows to this run.
    nodes_added: Optional[int] = None
    edges_added: Optional[int] = None
    counts_authoritative: bool = False
    zero_rows: bool = False
    per_source: tuple[Mapping[str, Any], ...] = ()
    findings: tuple[str, ...] = ()


def interpret_commit_envelope(envelope: Mapping[str, Any]) -> CommitOutcome:
    """Read a ``commit_setup`` (or bare ``add_source``) envelope.

    The ``plan_not_found`` arm is the one that must never be swallowed:
    plans live in-process with a 30-minute TTL, so an operator who
    approved a plan five minutes after reading it can land here, and a
    swallowed ``plan_not_found`` reads as "nothing to do".
    """
    status = str(envelope.get("status") or "")
    if status == COMMIT_PLAN_NOT_FOUND:
        return CommitOutcome(
            status=status,
            succeeded=False,
            re_propose_required=True,
            prompt=(
                "That setup plan is gone (plans are held in the MCP "
                "server's memory for 30 minutes and do not survive a "
                "restart). Nothing was wired. Re-run `propose_setup` and "
                "I will read you the fresh plan to approve."
            ),
            findings=(
                f"commit_setup returned {COMMIT_PLAN_NOT_FOUND}; the plan "
                f"id {envelope.get('plan_id')!r} expired or the server "
                "restarted",
            ),
        )

    rows = tuple(
        row for row in (envelope.get("results") or ())
        if isinstance(row, Mapping)
    )
    if not rows:
        # A bare add_source envelope is a single row.
        rows = (envelope,)

    findings: list[str] = []
    nodes: Optional[int] = None
    edges: Optional[int] = None
    authoritative = True
    any_failure = False
    any_success = False
    any_other = False
    zero_rows = False
    for row in rows:
        row_status = str(row.get("status") or "")
        if row_status in _COMMIT_FAILURE_STATUSES:
            any_failure = True
            findings.append(
                f"{row.get('source_id') or row.get('short')}: {row_status}"
                f" -- {row.get('summary')}"
            )
            continue
        if row_status in _COMMIT_SUCCESS_STATUSES:
            any_success = True
        else:
            any_other = True
            findings.append(
                f"{row.get('source_id') or row.get('short')}: "
                f"{row_status or 'unknown status'} -- neither wired nor "
                "failed; the tool answered without doing the work"
            )
            continue
        if row_status == "ok_already_configured":
            continue
        row_nodes = row.get("nodes_added")
        row_edges = row.get("edges_added")
        if row_nodes is None or row_edges is None:
            # Count-less envelope: the runner could not attribute rows to
            # THIS run. Unknown, not zero.
            authoritative = False
            findings.append(
                f"{row.get('source_id') or row.get('short')}: the ingest "
                "returned no run-attributed counts, so the row delta is "
                "UNKNOWN (not zero)"
            )
            continue
        nodes = int(row_nodes) + (nodes or 0)
        edges = int(row_edges) + (edges or 0)
        if row_status == "ingest_complete_no_rows":
            zero_rows = True

    # `any_other` (needs_connection, awaiting_params, unknown_source, ...)
    # disqualifies success on purpose: a commit that wired one source and
    # left another merely ANSWERED is not a completed setup, and reporting
    # it as one is how "the tool replied, so it worked" gets believed.
    succeeded = any_success and not any_failure and not any_other
    return CommitOutcome(
        status=str(envelope.get("status") or (rows[0].get("status") or "")),
        succeeded=succeeded,
        re_propose_required=False,
        nodes_added=nodes,
        edges_added=edges,
        counts_authoritative=authoritative and nodes is not None,
        zero_rows=zero_rows,
        per_source=rows,
        findings=tuple(findings),
    )


# ---------------------------------------------------------------------------
# Generation diagnosis (the catalog hop)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GenerationDiagnosis:
    generation_id: Optional[str]
    status: Optional[str]
    published: bool
    #: A narrowing violation is a CATALOG GAP, diagnosable and distinct
    #: from a chat-surface defect.
    catalog_gap: bool = False
    violation_class: Optional[str] = None
    violating_triples: tuple[str, ...] = ()
    catalog_version_id: Optional[str] = None
    findings: tuple[str, ...] = ()


def diagnose_generation(describe: Mapping[str, Any]) -> GenerationDiagnosis:
    """Classify an ``okg generation describe`` payload.

    ``published`` vs ``blocked`` is read from ``status``; a blocked
    generation whose reason names ``narrowing_violation`` is reported as
    a DIAGNOSED catalog gap (naming the violating triple where the
    payload carries it), not as a failure of the chat surface.
    """
    status = describe.get("status")
    status_text = str(status or "")
    published = status_text == "published"
    blob = repr(describe.get("blocked_reason")) + repr(
        describe.get("invariant_summary")
    )
    catalog_gap = (not published) and "narrowing_violation" in blob
    triples: tuple[str, ...] = ()
    reason = describe.get("blocked_reason")
    if isinstance(reason, Mapping):
        raw = reason.get("violating_triples") or reason.get("triples") or ()
        if isinstance(raw, (list, tuple)):
            triples = tuple(str(item) for item in raw)
    findings: list[str] = []
    if not status:
        findings.append(
            "the describe payload carries no status, so the generation's "
            "publish state is UNKNOWN, not published"
        )
    if catalog_gap:
        findings.append(
            "blocked on narrowing_violation: the applied narrowing "
            "catalog does not admit a (parent_subtype, edge_type, "
            "child_subtype) this source emits. This is a catalog gap "
            "-- run `okg catalog load --deployment <dir> --apply` and "
            "record the catalog_version_id -- not a chat-surface defect."
        )
    return GenerationDiagnosis(
        generation_id=(
            str(describe["generation_id"])
            if describe.get("generation_id") is not None
            else None
        ),
        status=status_text or None,
        published=published,
        catalog_gap=catalog_gap,
        violation_class="narrowing_violation" if catalog_gap else None,
        violating_triples=triples,
        catalog_version_id=(
            str(describe["catalog_version_id"])
            if describe.get("catalog_version_id") is not None
            else None
        ),
        findings=tuple(findings),
    )


# ---------------------------------------------------------------------------
# The failed-document retry affordance
# ---------------------------------------------------------------------------

#: What a source with NO current DLQ row gets instead. Named so the two
#: paths cannot be confused: this one advances a watermark and adds rows,
#: and cannot clear a DLQ row.
RESYNC_COMMAND_HINT = (
    "okg ingest --include <source> --deployment <d> --enqueue"
)


@dataclass(frozen=True)
class RetryPlan:
    """The exact command chain a retry runs, or the refusal to run one."""

    deployment: str
    source: str
    applicable: bool
    failed_workflow_ids: tuple[str, ...] = ()
    commands: tuple[tuple[str, ...], ...] = ()
    refusal: Optional[str] = None

    @property
    def rendered(self) -> tuple[str, ...]:
        return tuple(" ".join(argv) for argv in self.commands)


def retry_plan(
    *,
    deployment: str,
    source: str,
    dlq_payload: Mapping[str, Any],
    okg_argv_prefix: Sequence[str] = ("okg",),
) -> RetryPlan:
    """Build the retry chain from a ``current-runtime-dlq`` payload.

    ``okg jobs enqueue-source --supersedes-workflow-id <failed>`` is the
    ONLY chain that satisfies the DLQ's supersession predicate
    (``_explicitly_supersedes`` in ``okg.substrate.runtime_dlq``): a row
    leaves ``current_failures`` only when a later SUCCESS explicitly
    supersedes the exact failed responsibility. ``okg ingest --enqueue``
    carries no supersession flag, so it can advance the watermark and add
    rows while the DLQ row stays current forever — naming it as the
    retry path would make the affordance report success while the
    failure persists.
    """
    failures = _failures_for_source(dlq_payload, deployment=deployment, source=source)
    if not failures:
        return RetryPlan(
            deployment=deployment,
            source=source,
            applicable=False,
            refusal=(
                f"`{source}` has no current DLQ row in this deployment, so "
                "there is nothing to supersede and the retry affordance "
                "does not apply. The re-sync affordance for a source with "
                f"no current failure is `{RESYNC_COMMAND_HINT}`, which is "
                "NOT the retry path: it carries no supersession flag and "
                "cannot clear a DLQ row."
            ),
        )
    workflow_ids = tuple(
        str(row.get("workflow_id"))
        for row in failures
        if str(row.get("workflow_id") or "").strip()
    )
    if len(workflow_ids) != len(failures):
        return RetryPlan(
            deployment=deployment,
            source=source,
            applicable=False,
            refusal=(
                f"a current DLQ failure for `{source}` carries no "
                "workflow_id, so no command can name the responsibility it "
                "supersedes. Retrying without it would enqueue work that "
                "leaves the failure current."
            ),
        )
    commands = tuple(
        (
            *okg_argv_prefix, "jobs", "enqueue-source",
            "--deployment", deployment,
            "--source", source,
            "--supersedes-workflow-id", workflow_id,
            # `jobs enqueue-source` is an active write command, which the
            # CLI REFUSES without --apply under OKG_AGENT=1 (main.py's
            # write_cmd gate). Omitting it would make the affordance fail
            # in exactly the environment an agent-driven chat surface
            # runs in, and report the failure as "DLQ row still current".
            "--apply",
        )
        for workflow_id in workflow_ids
    )
    return RetryPlan(
        deployment=deployment,
        source=source,
        applicable=True,
        failed_workflow_ids=workflow_ids,
        commands=commands,
    )


def _failures_for_source(
    dlq_payload: Mapping[str, Any],
    *,
    deployment: str,
    source: str,
) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for row in dlq_payload.get("current_failures") or ():
        if not isinstance(row, Mapping):
            continue
        responsibility = row.get("responsibility")
        if not isinstance(responsibility, Mapping):
            continue
        if str(responsibility.get("deployment") or "") != deployment:
            continue
        sources = responsibility.get("sources") or ()
        if source in {str(name) for name in sources}:
            out.append(row)
    return out


@dataclass(frozen=True)
class RetryObservation:
    """One before/after observation around a retry.

    Every field is Optional and an absent field is UNKNOWN. Two unknown
    observations must not compare equal and read as "unchanged" — see
    :func:`retry_verdict`, which treats an unknown as not-proven-changed
    AND not-proven-unchanged.
    """

    dlq_payload: Mapping[str, Any] = field(default_factory=dict)
    watermark: Optional[str] = None
    queue_state: Optional[Mapping[str, Any]] = None
    row_count: Optional[int] = None


@dataclass(frozen=True)
class RetryVerdict:
    succeeded: bool
    dlq_row_still_current: bool
    effect_free: bool
    still_current_workflow_ids: tuple[str, ...] = ()
    unobserved_signals: tuple[str, ...] = ()
    failed_commands: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()


def retry_verdict(
    *,
    plan: RetryPlan,
    before: RetryObservation,
    after: RetryObservation,
    command_results: Sequence[Mapping[str, Any]] = (),
) -> RetryVerdict:
    """Did the retry actually clear the failure?

    Four independent NOT-succeeded conditions, in order:

    0. A retry command exited non-zero. The recovery command did not
       run, so nothing downstream is evidence about a retry.
    1. The DLQ row is still in ``current_failures`` afterwards —
       regardless of watermark, queue, or row-count change. A retry that
       added rows and left the failure current did not do what it was
       invoked to do.
    2. A before/after signal was not OBSERVED. An unmeasured effect is
       not a proven one, and treating two unobserved reads as equal
       would let a failed observation masquerade as either verdict.
    3. Watermark, queue state, and row count are ALL unchanged — a
       success message with no substrate change is a silent fallback.
    """
    reasons: list[str] = []
    if not plan.applicable:
        return RetryVerdict(
            succeeded=False,
            dlq_row_still_current=False,
            effect_free=False,
            reasons=(
                "no retry ran: " + (plan.refusal or "the plan was not "
                                    "applicable"),
            ),
        )

    failed = tuple(
        str(row.get("command"))
        for row in command_results
        if int(row.get("returncode") or 0) != 0
    )
    if failed:
        detail = "; ".join(
            f"{row.get('command')} -> exit "
            f"{row.get('returncode')}: {row.get('stderr') or ''}"[:400]
            for row in command_results
            if int(row.get("returncode") or 0) != 0
        )
        reasons.append(
            "NOT succeeded: the recovery command itself failed, so no "
            f"retry was performed. {detail}"
        )
        return RetryVerdict(
            succeeded=False,
            dlq_row_still_current=False,
            effect_free=False,
            failed_commands=failed,
            reasons=tuple(reasons),
        )

    after_failures = _failures_for_source(
        after.dlq_payload,
        deployment=plan.deployment,
        source=plan.source,
    )
    still_current = tuple(
        str(row.get("workflow_id"))
        for row in after_failures
        if str(row.get("workflow_id")) in set(plan.failed_workflow_ids)
    )
    if still_current:
        reasons.append(
            f"NOT succeeded: DLQ row(s) {list(still_current)} are still in "
            "current_failures after the retry. A DLQ row leaves the "
            "current set only when a later success explicitly supersedes "
            "the exact failed responsibility; row delta, watermark "
            "movement, and queue drain do not substitute for that."
        )
        return RetryVerdict(
            succeeded=False,
            dlq_row_still_current=True,
            effect_free=False,
            still_current_workflow_ids=still_current,
            reasons=tuple(reasons),
        )

    unobserved = [
        name
        for name, pair in (
            ("watermark", (before.watermark, after.watermark)),
            ("queue_state", (before.queue_state, after.queue_state)),
            ("row_count", (before.row_count, after.row_count)),
        )
        if pair[0] is None or pair[1] is None
    ]
    if unobserved:
        # An UNOBSERVED signal is neither changed nor unchanged, so the
        # "all three unchanged" test cannot be run -- and skipping it
        # would let a failed OBSERVATION be reported as a successful
        # RETRY. Fail closed: an effect that was not measured was not
        # proven.
        reasons.append(
            "NOT succeeded (effect unproven): the DLQ row did leave "
            f"current_failures, but {unobserved} could not be observed "
            "before and after, so the substrate change this retry claims "
            "was never measured. An unmeasured effect is not a proven "
            "one."
        )
        return RetryVerdict(
            succeeded=False,
            dlq_row_still_current=False,
            effect_free=False,
            unobserved_signals=tuple(unobserved),
            reasons=tuple(reasons),
        )
    unchanged = (
        before.watermark == after.watermark
        and dict(before.queue_state or {}) == dict(after.queue_state or {})
        and before.row_count == after.row_count
    )
    if unchanged:
        reasons.append(
            "NOT succeeded: watermark, queue state, and row count are all "
            "unchanged across the retry. A success message with no "
            "substrate change is a silent fallback."
        )
        return RetryVerdict(
            succeeded=False,
            dlq_row_still_current=False,
            effect_free=True,
            reasons=tuple(reasons),
        )
    return RetryVerdict(
        succeeded=True,
        dlq_row_still_current=False,
        effect_free=False,
        reasons=tuple(reasons),
    )


# ---------------------------------------------------------------------------
# One status vocabulary (design.md §5)
# ---------------------------------------------------------------------------

#: The fields chat MUST report with the same NAMES and the same VALUES as
#: `okg dogfood status` and the console Health view.
SHARED_STATUS_FIELDS: tuple[str, ...] = (
    "generation_id",
    "trust_level",
    "source_freshness",
    "queues",
    "dlq_count",
    "doctor",
)


def shared_status_fields(
    dogfood_status: Mapping[str, Any],
    *,
    dlq_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Project the shared status vocabulary OUT of the dogfood payload.

    Chat renders FROM this, so the field-by-field diff against
    ``okg dogfood status --json`` is empty by construction rather than by
    two implementations happening to agree.

    ``dlq_count`` has no top-level home in the dogfood payload: it rides
    the ``sources.sync_liveness`` check detail. When neither that detail
    nor an explicit ``current-runtime-dlq`` payload supplies it, the
    value is ``None`` — UNAVAILABLE, never 0. A zero would read as "no
    dead letters".
    """
    read_authority = dogfood_status.get("read_authority")
    generation_id = (
        read_authority.get("generation_id")
        if isinstance(read_authority, Mapping)
        else None
    )
    dlq_count: Optional[int] = None
    if dlq_payload is not None and dlq_payload.get("current_count") is not None:
        dlq_count = int(dlq_payload["current_count"])
    else:
        for bucket in ("blockers", "warnings"):
            for check in dogfood_status.get(bucket) or ():
                if not isinstance(check, Mapping):
                    continue
                if str(check.get("id") or "") != "sources.sync_liveness":
                    continue
                details = check.get("details")
                if isinstance(details, Mapping) and (
                    details.get("dlq_count") is not None
                ):
                    dlq_count = int(details["dlq_count"])
    return {
        "generation_id": generation_id,
        "trust_level": dogfood_status.get("trust_level"),
        "source_freshness": dogfood_status.get("source_freshness"),
        "queues": dogfood_status.get("queues"),
        "dlq_count": dlq_count,
        "doctor": dogfood_status.get("doctor"),
    }


def status_field_diff(
    chat_rendered: Mapping[str, Any],
    dogfood_status: Mapping[str, Any],
    *,
    dlq_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, dict[str, Any]]:
    """Field-by-field diff of the shared status vocabulary.

    Empty means chat and ``okg dogfood status`` agree on both the name
    and the value of every shared field. A field chat does not carry at
    all is a disagreement, not a pass: a missing field is not an
    agreeing one.
    """
    expected = shared_status_fields(dogfood_status, dlq_payload=dlq_payload)
    diff: dict[str, dict[str, Any]] = {}
    for name in SHARED_STATUS_FIELDS:
        if name not in chat_rendered:
            diff[name] = {
                "chat": "<absent>",
                "dogfood": expected[name],
                "reason": "chat does not carry this field",
            }
            continue
        if chat_rendered[name] != expected[name]:
            diff[name] = {
                "chat": chat_rendered[name],
                "dogfood": expected[name],
                "reason": "values disagree",
            }
    return diff


# ---------------------------------------------------------------------------
# The typed chat: manifest block, consumed rather than restated
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AdminBindingInputs:
    """What the deployment manifest declares about the admin surface."""

    endpoint: Optional[str]
    access_group_names: tuple[str, ...]
    auth_token_env: Optional[str]
    port: Optional[int]


def admin_binding_inputs(
    chat_block: Any, *, host: str = "127.0.0.1",
) -> AdminBindingInputs:
    """Read the admin surface's inputs off ``chat-manifest-block``'s block.

    The manifest is the declaration; restating it on a command line is
    how a manifest/CLI divergence becomes undetectable. ``chat.mcp.port``
    plus ``host`` compose the endpoint (the manifest deliberately carries
    no URL: the host is instance wiring, the port is deployment
    identity).

    ``chat.mcp.tools`` is NOT returned as the admin tool set. That field
    is the USER preset's surface, validated at load against the read
    operators; the admin set is derived from the live tools/list
    readback, which is the whole point of the operational definition.
    """
    if chat_block is None or not getattr(chat_block, "enabled", False):
        raise ChatSourceAdminError(
            "the deployment declares no enabled chat: block, so there is "
            "no admin surface to project. An absent declaration is not a "
            "default-configured chat.",
            data={"reason": "chat_block_absent_or_disabled"},
            fix_hint="declare chat.enabled: true with mcp and access blocks",
        )
    mcp = getattr(chat_block, "mcp", None)
    access = getattr(chat_block, "access", None)
    port = getattr(mcp, "port", None) if mcp is not None else None
    endpoint = f"http://{host}:{port}/mcp" if port else None
    return AdminBindingInputs(
        endpoint=endpoint,
        access_group_names=tuple(
            getattr(access, "groups", ()) if access is not None else ()
        ),
        auth_token_env=(
            getattr(mcp, "auth_token_env", None) if mcp is not None else None
        ),
        port=port,
    )


def okg_mcp_tool_universe() -> frozenset[str]:
    """Every tool the OKG MCP server can register.

    Exposed so the non-replication claim is checkable against the
    real universe rather than against the preset alone.
    """
    return frozenset(ALL_BUILTIN_TOOLS)
