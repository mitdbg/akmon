"""Plan, apply and readback for ``okg chat sync`` (slice S2 of #1183).

``okg chat sync`` writes six surfaces and reads every one of them back.
What it had no way to do was say, BEFORE it ran, what it was going to
write — so its readback compared the instance against a desired state
that only existed inside the same process that had just applied it. That
is a self-referential check: nothing outside the run could disagree with
it. This module supplies the missing outside term.

Three verbs, one document family:

``okg chat plan --out <dir>``
    Derives the desired state from the deployment's ``chat:`` block alone
    and writes ``chat-plan.json``. It contacts NO instance: no admin API
    call, no MCP probe, no container. The only thing it touches outside
    the manifest and the deployment directory is the ENVIRONMENT, and
    only to check that a declared credential REFERENCE resolves — the
    value is never read into the plan, the digest, or a refusal.

``okg chat sync`` (apply)
    Unchanged in what it writes. It now recomputes the plan from the same
    manifest, records ``plan_digest`` in its report, and — when the
    operator passes ``--plan`` — refuses before touching the instance if
    the two disagree. That refusal is the drift case the design note
    missed: a plan reviewed on Monday and applied on Friday against an
    edited manifest.

``okg chat readback --plan <file> --out <dir>``
    Reads the live instance and writes ``chat-readback.json``: per
    surface, ``match`` or ``deviation``, in the vocabulary ``sync``
    already uses (``dropped``, ``no_consumer``, ``not_declared``), plus
    S1's instance-side verify probe. It never writes to the instance. The
    one non-GET it issues is the vendor's own
    ``POST /api/v1/configs/tool_servers/verify``, which dials outward and
    stores nothing.

**What a plan CANNOT determine, stated rather than guessed.** Several
values ``sync`` applies are settled only by the running instance, and the
plan lists each one under ``undetermined`` with its reason instead of
printing a number that would deviate on every healthy site:

* ``preset:params.system`` — the composed prompt carries standing
  guidance rendered from the LIVE advertised tool set.
* ``preset:grants`` and ``connection:config.function_name_filter_list``
  — live group ids, and the filter bound against the names the endpoint
  actually advertises.
* ``connection:url`` — operator topology (the address the container
  reaches ``mcp-serve`` at), never manifest content.
* ``connection:auth_key_digest`` — an HMAC under a per-run salt, so it is
  not stable across processes by construction. Readback measures it live
  instead, within one process: ``verify_probe["stored_credential"]``.
* ``features.context_compaction`` — gated by the observability verdict
  for the RUNNING Open WebUI version.
* census admission — ``sync`` refuses a declared knob the recorded census
  does not admit at the running version, and the running version is not
  knowable here.

**Feeding #1185's chat conformance arm.** This writes no conformance
receipt. Honestly stated, the mapping it can support today is:

* ``plan["plan_digest"]`` -> the ``plan_digest`` fence, the same role
  ``install_plan`` plays for the distribution arm.
* ``readback["plan_digest"]`` -> the tie proving a readback was taken
  against THAT plan; a readback whose digest differs is evidence about a
  different desired state.
* ``readback["surfaces"][*]["status"]`` and ``["deviations"]`` -> the
  per-surface observed signal.
* ``readback["verify_probe"]`` -> the credential half, from S1. Its
  ``state`` proves the OPERATOR's credential reaches ``mcp-serve``; its
  ``stored_credential`` says whether the site stored that same credential
  (``mcp_connection_credential_stale`` when not, #2287).

What it does NOT supply, and #1185 must not read into it: any claim about
the surfaces listed in ``undetermined``, and any claim that the site can
answer a question — no model provider is exercised anywhere here.
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "CHAT_PLAN_SCHEMA",
    "CHAT_READBACK_SCHEMA",
    "PLAN_FILENAME",
    "PROBE_ERROR",
    "PROBE_NOT_PROBED",
    "PROBE_NOT_REGISTERED",
    "READBACK_FILENAME",
    "ChatPlanDrift",
    "ChatPlanError",
    "ChatPlanOutputPathExists",
    "ChatReadbackDeviation",
    "chat_readback",
    "check_plan_matches_manifest",
    "load_chat_plan",
    "plan_digest",
    "resolve_chat_plan",
    "write_chat_plan",
    "write_chat_readback",
]


#: Verify-probe states this module adds to S1's three. `not_registered` and
#: `probe_error` are both "the credential was NOT measured", and they are
#: separate from each other and from S1's `unavailable` because the operator
#: action differs: register the connection, fix the admin route, or upgrade.
PROBE_NOT_REGISTERED = "not_registered"
PROBE_ERROR = "probe_error"
PROBE_NOT_PROBED = "not_probed"

CHAT_PLAN_SCHEMA = "okg.deployment_product.chat_plan/v1"
CHAT_READBACK_SCHEMA = "okg.deployment_product.chat_readback/v1"
PLAN_FILENAME = "chat-plan.json"
READBACK_FILENAME = "chat-readback.json"


class ChatPlanError(SubstrateError):
    """A chat plan, apply-gate or readback refusal. Nothing was written."""

    code = "chat_plan_refused"


class ChatPlanOutputPathExists(ChatPlanError):
    """The named output path already holds something."""

    code = "chat_plan_output_path_exists"


class ChatPlanDrift(ChatPlanError):
    """The passed plan does not describe this manifest's desired state."""

    code = "chat_plan_drift"


class ChatReadbackDeviation(ChatPlanError):
    """The live instance disagrees with the plan on at least one surface."""

    code = "chat_readback_deviation"


def plan_digest(plan: Mapping[str, Any]) -> str:
    """The digest OVER the desired state, with ``plan_digest`` itself removed.

    Secrets cannot enter it: the only credential the plan carries is a
    reference (``{"kind": "env", "variable": ...}``), and whether that
    reference resolves is deliberately NOT part of the digested body — so
    a plan reviewed on a machine that holds the token and one that does
    not carry the same digest, and the apply-time comparison stays a
    statement about the manifest rather than about the environment.
    """
    body = {k: v for k, v in plan.items() if k != "plan_digest"}
    return sha256_digest(canonical_json_bytes(body))


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------

def _instance_ui_desired(chat: Any) -> tuple[dict[str, Any], list[dict[str, str]]]:
    from okg.substrate.chat.sync import (
        GOVERNED_TOGGLES,
        SURFACE_INSTANCE_UI,
        _toggle_desired,
        expected_site_name,
        site_hygiene_desired,
    )

    ui = chat.ui
    declared = dict((ui.features or {}) if ui is not None else {})
    values: dict[str, Any] = {}
    undetermined: list[dict[str, str]] = []
    if ui is not None and ui.site_name:
        # F8: the vendor appends its own suffix, so the manifest value is
        # not what any readback will ever show.
        values["site_name"] = expected_site_name(ui.site_name)
    if ui is not None and ui.banner:
        # NOT the `banner` key `sync` reconciles: that one is a composed
        # LIST (declared + operator banners carried forward + at most one
        # owned health banner), and only a live read settles it. What the
        # manifest determines is that a banner carrying this content must
        # be present, which is what the readback checks.
        values["banner:declared_content"] = ui.banner
    effective: dict[str, bool] = {}
    for toggle in GOVERNED_TOGGLES:
        if toggle.policy_value is not None:
            effective[toggle.name] = toggle.policy_value
        elif toggle.name == "context_compaction":
            undetermined.append({
                "key": "features.context_compaction",
                "reason": (
                    "the observability verdict gate reads the verdict for the "
                    "RUNNING Open WebUI version, which no plan can know; an "
                    "absent verdict makes the effective value off"
                ),
            })
        elif toggle.name in declared:
            effective[toggle.name] = bool(declared[toggle.name])
        elif toggle.default_value is not None:
            effective[toggle.name] = toggle.default_value
    # The same expression `sync` uses, so a toggle that gains or loses a
    # switch moves the plan with it rather than needing a second list here.
    for item in _toggle_desired(SURFACE_INSTANCE_UI, effective):
        values[item.key] = item.value
    # The cut-list settings `sync` writes beside the toggles, from the same
    # list, so `chat readback` reports drift in any of them.
    for item in site_hygiene_desired(SURFACE_INSTANCE_UI):
        values[item.key] = item.value
    return values, undetermined


def _preset_desired(chat: Any, *, preset_id: str, server_id: str) -> dict[str, Any]:
    from okg.substrate.chat.sync import MCP_TOOL_ID_PREFIX

    preset = chat.preset
    suggestions = list(preset.suggestions) if preset else []
    return {
        "preset:id": preset_id,
        "preset:name": (preset.display_name if preset else None) or preset_id,
        "preset:meta.description": preset.description if preset else None,
        "preset:meta.suggestion_prompts": [{"content": s} for s in suggestions],
        "preset:meta.toolIds": [f"{MCP_TOOL_ID_PREFIX}{server_id}"],
        "preset:meta.knowledge": [],
        "preset:meta.builtinTools.knowledge": False,
        "preset:base_model_id": chat.models.default if chat.models else None,
        "connection:info.id": server_id,
        "connection:type": "mcp",
        "connection:config.enable": True,
        # S1's contract: the entry is `bearer` exactly when the manifest
        # names a credential variable, and EXPLICITLY `none` otherwise --
        # unless chat.oidc_login is declared, in which case the entry forwards
        # each person's own token and the plan must say so, exactly as the
        # apply writes it.
        "connection:auth_type": (
            "system_oauth"
            if getattr(chat, "oidc_login", None) is not None
            else "bearer"
            if (chat.mcp is not None and chat.mcp.auth_token_env)
            else "none"
        ),
    }


_PRESET_UNDETERMINED: tuple[tuple[str, str], ...] = (
    (
        "preset:params.system",
        "the composed prompt carries standing guidance rendered from the "
        "LIVE advertised tool set, which only the apply run observes",
    ),
    (
        "preset:grants",
        "group ids are minted by the instance; the manifest names groups",
    ),
    (
        "connection:config.function_name_filter_list",
        "the filter is bound against the names the endpoint actually "
        "advertises, and both vendor traps are handled against those names",
    ),
    (
        "connection:url",
        "the address the INSTANCE dials mcp-serve at is operator topology, "
        "never manifest content",
    ),
    (
        "connection:auth_key_digest",
        "an HMAC under a per-run salt, so it is not stable across processes "
        "by construction",
    ),
    (
        "write_connection:*",
        "the admin connection's existence depends on the live advertised "
        "write tools and on the observed auth posture of the endpoint",
    ),
)


def _live_tools_surface(slot: Any) -> dict[str, Any]:
    """The live-tool connection's desired state, or its declared absence.

    ``live_connection:present`` is desired on EVERY plan, including as
    ``False``. A plan that simply omitted the surface where no slot is
    declared could not tell "this deployment grants no live tools" apart
    from "nobody looked", and the second is what an operator needs the
    readback to rule out: an unwanted live connection registered by hand is
    exactly the drift this surface exists to catch.
    """
    desired: dict[str, Any] = {
        "live_connection:present": bool(slot is not None and slot.registered),
    }
    undetermined: list[dict[str, str]] = []
    if slot is not None and slot.registered:
        desired.update({
            "live_connection:info.id": slot.server_id,
            "live_connection:url": slot.url,
            "live_connection:type": "mcp",
            "live_connection:config.enable": True,
            "live_connection:auth_type": (
                "bearer" if slot.auth_token_env else "none"
            ),
            "live_connection:config.function_name_filter_list": ",".join(
                sorted(slot.granted)
            ),
        })
    if slot is not None:
        undetermined.append({
            "key": "live_connection:grants",
            "reason": (
                "the slot's audience grants name Open WebUI GROUPS, whose ids "
                "the instance mints; the plan records the names it will "
                "resolve, never ids it cannot know"
            ),
        })
        undetermined.append({
            "key": "live_connection:advertised_tools",
            "reason": (
                "what the live endpoint advertises is measured against the "
                "running server, not declared -- the slot names what the "
                "deployment ALLOWS, which is a ceiling either way"
            ),
        })
    return {
        "desired": desired,
        "undetermined": undetermined,
        # Recorded and DIGESTED, never compared. These are declarations, not
        # instance state: the allowed set is a ceiling no instance holds, and
        # the grants resolve to ids the plan cannot know. Putting them in
        # `desired` would make the readback echo the plan back at itself and
        # report a match it could not have failed -- which is not a
        # measurement. What the instance actually serves is the filter list
        # above, and that IS compared.
        "slot": (
            {
                "allowed": sorted(slot.allowed),
                "granted": sorted(slot.granted),
                "audiences": sorted(slot.audiences),
            }
            if slot is not None
            else {}
        ),
    }


def resolve_chat_plan(
    inputs: Any,
    *,
    require_credential: bool = True,
    environ: Optional[Mapping[str, str]] = None,
    bundle: Any = None,
) -> dict[str, Any]:
    """The composed desired state for all six surfaces.

    Contacts no instance. ``require_credential`` resolves the reference
    named by ``chat.mcp.auth_token_env`` and refuses when it is declared
    but unset — the same refusal ``sync`` makes, moved earlier so a plan
    is never written for a configuration that cannot be applied. It is
    off when ``sync`` itself computes the digest, because ``sync`` owns
    that refusal at its own site and duplicating it here would reorder
    its failures.
    """
    from okg.substrate.chat.live_tool_slot import resolve_live_tool_slot
    from okg.substrate.deployments.chat_block import ui_default_model

    chat = inputs.chat
    mcp = chat.mcp
    slot = resolve_live_tool_slot(
        chat, deployment=inputs.deployment, mcp_server_id=inputs.mcp_server_id,
    )
    env_name = mcp.auth_token_env if mcp is not None else None
    # Only where a connection is actually registered. A slot nobody is
    # granted registers nothing, so its variable is never read, and refusing
    # on it here would refuse a manifest `sync` applies without complaint --
    # the plan and the apply disagreeing about one manifest, which is the
    # defect this whole verb exists to prevent.
    live_env_name = (
        slot.auth_token_env if slot is not None and slot.registered else None
    )
    for declared, where in (
        (env_name, "chat.mcp.auth_token_env"),
        (live_env_name, "chat.mcp.live_tools.auth_token_env"),
    ):
        if not (declared and require_credential):
            continue
        env = environ if environ is not None else os.environ
        if not (env.get(str(declared)) or "").strip():
            raise ChatPlanError(
                f"{where} names {declared!r}, which is unset "
                "or empty. The plan records the REFERENCE, never the value, "
                "but a reference that resolves nowhere describes a state no "
                "apply can reach — so it refuses here rather than writing a "
                "plan whose only possible outcome is a refusal later.",
                data={
                    "reason": "credential_reference_unresolved",
                    "variable": str(declared),
                    "deployment": inputs.deployment,
                },
                fix_hint=f"export {declared} with the token the server reads",
            )

    ui_values, ui_undetermined = _instance_ui_desired(chat)
    deep = chat.deep_links
    surfaces: dict[str, Any] = {
        "instance_ui": {
            "desired": ui_values,
            "undetermined": ui_undetermined,
        },
        "models": {
            "desired": {
                # Resolved through the SAME helper `sync` applies, so a plan
                # and the run that applies it cannot hold two opinions of
                # which model the browser opens on (#1183 finding 1). It is
                # the preset unless `chat.models.ui_default` opts out --
                # never `chat.models.default`, which is the preset's raw
                # base model and carries no tools.
                "ui.default_models": ui_default_model(
                    chat.models, preset_id=inputs.preset_id,
                ),
                "ui.model_order_list": list(chat.models.builtin) if chat.models else [],
            },
            "undetermined": [],
        },
        "preset": {
            "desired": _preset_desired(
                chat, preset_id=inputs.preset_id, server_id=inputs.mcp_server_id,
            ),
            "undetermined": [
                {"key": key, "reason": reason} for key, reason in _PRESET_UNDETERMINED
            ],
        },
        "prompts": {
            "desired": {
                "prompt:command": f"/okg-{inputs.deployment}",
                "prompt:name": f"OKG {inputs.deployment}",
            },
            "undetermined": [{
                "key": "prompt:content",
                "reason": (
                    "the prompt body is the composed system prompt, and the "
                    "surface is driven at all only where a write-plus-readback "
                    "proof exists for the RUNNING Open WebUI version"
                ),
            }],
        },
        "deep_links": {
            "desired": (
                {"okg_review_queue.openwebui_api_base": deep.chat_site_base}
                if deep is not None and deep.chat_site_base
                else {}
            ),
            "undetermined": [],
        },
        "mcp": {
            "desired": {
                "mcp:port": mcp.port if mcp is not None else None,
                "mcp:tools": sorted(mcp.tools) if mcp is not None else [],
            },
            "undetermined": [{
                "key": "mcp:advertised_tools",
                "reason": "what the endpoint advertises is measured, not declared",
            }],
        },
        # S4. Its OWN surface, not a corner of `mcp`: it is a different
        # connection carrying a different authority, and a reader who has to
        # ask which half of one surface deviated cannot act on the answer.
        "live_tools": _live_tools_surface(slot),
    }
    # S3. `inputs.chat` is ALREADY the composed block when a bundle was
    # selected -- `load_inputs` folds the bundle's defaults into it and
    # refuses a manifest that widens -- so every surface above is composed
    # by construction. What is added here is the RECORD of how it was
    # composed, which the digest then covers along with the rest.
    # What the record does NOT claim is recorded too: the live-tool names,
    # the instruction playbooks and the logo are declared, resolved and
    # digested, and no path in this slice writes any of them onto the site.
    # Each is listed under the surface that would consume it, so a reader
    # cannot mistake a declaration for a contribution the apply makes.
    bundle = bundle if bundle is not None else getattr(inputs, "bundle_chat", None)
    if bundle is not None:
        from okg.substrate.chat.bundle_composition import (
            INSTRUCTIONS_UNDETERMINED,
            LIVE_TOOL_UNDETERMINED,
            LOGO_UNDETERMINED,
        )

        if slot is None:
            # S3 recorded the bundle's live-tool names as undetermined because
            # nothing selected one. A declared slot IS the selection, so the
            # entry is dropped and the `live_tools` surface above carries a
            # determined desired state the readback compares.
            surfaces["live_tools"]["undetermined"].append(
                dict(LIVE_TOOL_UNDETERMINED)
            )
        surfaces["preset"]["undetermined"].append(dict(INSTRUCTIONS_UNDETERMINED))
        surfaces["instance_ui"]["undetermined"].append(dict(LOGO_UNDETERMINED))

    plan: dict[str, Any] = {
        "schema": CHAT_PLAN_SCHEMA,
        "deployment": inputs.deployment,
        "preset_id": inputs.preset_id,
        "mcp_server_id": inputs.mcp_server_id,
        # References only. `resolves` is deliberately absent: see
        # `plan_digest`.
        "credential_references": [
            {"kind": "env", "variable": str(name), "used_by": used_by}
            for name, used_by in (
                (env_name, "connection:key"),
                (live_env_name, "live_connection:key"),
            )
            if name
        ],
        "surfaces": surfaces,
    }
    if bundle is not None:
        plan["bundle"] = dict(bundle)
    plan["plan_digest"] = plan_digest(plan)
    return plan


def write_chat_plan(inputs: Any, output_dir: Path | str) -> dict[str, Any]:
    """Resolve, then write ``chat-plan.json`` under ``output_dir``.

    A directory that already holds a ``chat-plan.json`` refuses; one that
    exists holding other things is written into. There is no ``--force``: a plan is
    the reviewed term the apply and the readback are compared against, and
    silently rewriting one under a reviewer would defeat the entire point
    of having it.
    """
    output_dir = Path(output_dir)
    target = output_dir / PLAN_FILENAME
    if target.exists():
        raise ChatPlanOutputPathExists(
            f"{target} already exists. This verb never overwrites a plan: it "
            "is the reviewed term an apply and a readback are compared "
            "against, so replacing one in place would silently retarget both.",
            data={"reason": "output_path_exists", "path": str(target)},
            fix_hint="name an unused output directory, or remove the old plan",
        )
    plan = resolve_chat_plan(inputs)
    output_dir.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_json_bytes(plan) + b"\n")
    return {"plan_path": str(target), "plan_digest": plan["plan_digest"], "plan": plan}


def load_chat_plan(path: Path | str) -> dict[str, Any]:
    """Read a plan and check that its recorded digest matches its own body."""
    path = Path(path)
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ChatPlanError(
            f"the plan at {path} could not be read ({exc}). An unreadable "
            "plan is no plan, and is never treated as an absent one.",
            data={"reason": "plan_unreadable", "path": str(path)},
            fix_hint="re-run `okg chat plan`",
        ) from exc
    if not isinstance(loaded, Mapping) or loaded.get("schema") != CHAT_PLAN_SCHEMA:
        raise ChatPlanError(
            f"the document at {path} is not a {CHAT_PLAN_SCHEMA}.",
            data={
                "reason": "plan_schema_unknown",
                "path": str(path),
                "schema": (
                    loaded.get("schema") if isinstance(loaded, Mapping) else None
                ),
            },
            fix_hint="pass the chat-plan.json `okg chat plan` wrote",
        )
    plan = dict(loaded)
    recorded = plan.get("plan_digest")
    computed = plan_digest(plan)
    if recorded != computed:
        raise ChatPlanError(
            f"the plan at {path} does not digest to its own recorded value: "
            f"recorded {recorded!r}, computed {computed!r}. The file was "
            "edited after it was written.",
            data={
                "reason": "plan_digest_self_mismatch",
                "path": str(path),
                "recorded": recorded,
                "computed": computed,
            },
            fix_hint="re-run `okg chat plan` rather than editing the plan",
        )
    return plan


def check_plan_matches_manifest(inputs: Any, plan: Mapping[str, Any]) -> str:
    """Refuse when the passed plan is not this manifest's desired state.

    Returns the digest both sides agree on, which the report records.
    Called by ``sync`` BEFORE the first admin API call, so drift applies
    nothing at all.
    """
    resolved = resolve_chat_plan(inputs, require_credential=False)
    expected = resolved["plan_digest"]
    recorded = plan.get("plan_digest")
    if recorded != expected:
        raise ChatPlanDrift(
            "the plan passed to this apply does not describe the deployment's "
            f"current chat: block (plan {recorded!r}, manifest {expected!r}). "
            "The manifest changed after the plan was written, or the plan "
            "belongs to another deployment. Applying it would write a state "
            "nobody reviewed; nothing was applied.",
            data={
                "reason": "plan_manifest_drift",
                "deployment": inputs.deployment,
                "plan_digest": recorded,
                "manifest_digest": expected,
            },
            fix_hint="re-run `okg chat plan`, review it, then apply that plan",
        )
    return expected


# ---------------------------------------------------------------------------
# The readback
# ---------------------------------------------------------------------------

def _observe(
    client: Any,
    inputs: Any,
    surface: str,
    keys: Sequence[str],
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Live values for the plan's keys on one surface. Reads only.

    ``_read_toggles`` and ``tool_servers`` are ``sync``'s own readers, so a
    surface whose reading changes there changes here too rather than
    drifting into a second, quietly different opinion of the same value.
    """
    from okg.substrate.chat.sync import Desired, Observed, _read_toggles

    expected = expected or {}
    seen: dict[str, Any] = {}
    if surface == "instance_ui":
        if "site_name" in keys:
            config = client.public_config()
            if "name" in config:
                seen["site_name"] = config["name"]
        if "banner:declared_content" in keys:
            seen["banner:declared_content"] = [
                b.get("content") for b in client.banners() if isinstance(b, Mapping)
            ]
        toggles = [Desired(surface, key, None) for key in keys if ":" in key
                   and key != "banner:declared_content"]
        for key, observed in _read_toggles(client, toggles).items():
            if isinstance(observed, Observed) and observed.readable:
                seen[key] = observed.value
    elif surface == "models":
        # The vendor's own key names, taken from `_sync_models`'s reader
        # rather than derived from the plan key by a string transform that
        # would silently observe nothing if either name changed.
        config = client.models_config()
        for key, vendor in (
            ("ui.default_models", "DEFAULT_MODELS"),
            ("ui.model_order_list", "MODEL_ORDER_LIST"),
        ):
            if key in keys and vendor in config:
                seen[key] = (
                    list(config[vendor] or [])
                    if vendor == "MODEL_ORDER_LIST"
                    else config[vendor]
                )
    elif surface == "preset":
        row = client.model_row(inputs.preset_id)
        if row is not None:
            meta = row.get("meta") or {}
            seen["preset:id"] = row.get("id")
            seen["preset:name"] = row.get("name")
            seen["preset:base_model_id"] = row.get("base_model_id")
            for leaf in ("description", "toolIds", "knowledge", "suggestion_prompts"):
                if leaf in meta:
                    seen[f"preset:meta.{leaf}"] = meta[leaf]
            builtin = meta.get("builtinTools")
            if isinstance(builtin, Mapping) and "knowledge" in builtin:
                seen["preset:meta.builtinTools.knowledge"] = builtin["knowledge"]
        entry = _registered(client, inputs.mcp_server_id)
        if entry is not None:
            seen["connection:info.id"] = (entry.get("info") or {}).get("id")
            seen["connection:type"] = entry.get("type")
            seen["connection:config.enable"] = (entry.get("config") or {}).get("enable")
            seen["connection:auth_type"] = entry.get("auth_type")
    elif surface == "live_tools":
        # A surface of its OWN connection. `present` is answered from the
        # LIST, so it is readable either way -- which is what lets the
        # readback report a live connection nobody planned as a deviation
        # rather than as an unreadable key.
        from okg.substrate.chat.live_tool_slot import LIVE_CONNECTION_SUFFIX

        # Derived from the deployment, NOT only from the plan. A plan that
        # declares no slot still knows the id a live connection would carry,
        # so one registered out of band is found and reported present against
        # a desired `False` -- the "never by default" half, measured.
        server_id = str(
            expected.get("live_connection:info.id")
            or f"{inputs.mcp_server_id}{LIVE_CONNECTION_SUFFIX}"
        )
        entry = _registered(client, server_id)
        if entry is None:
            # The earlier release's live id (``okg-<d>-live``, #3960). Sync
            # keeps it when no live slot is declared (it may be foreign), so
            # readback must still see it, or a live connection registered out
            # of band would read as "absent" after the rename.
            from okg.substrate.chat.sync import legacy_mcp_server_ids

            legacy_live = legacy_mcp_server_ids(inputs.deployment)["live_connection"]
            if legacy_live != server_id:
                entry = _registered(client, legacy_live)
        seen["live_connection:present"] = entry is not None
        if entry is not None:
            config = entry.get("config") or {}
            seen["live_connection:info.id"] = (entry.get("info") or {}).get("id")
            seen["live_connection:url"] = entry.get("url")
            seen["live_connection:type"] = entry.get("type")
            seen["live_connection:auth_type"] = entry.get("auth_type")
            if "enable" in config:
                seen["live_connection:config.enable"] = config["enable"]
            if "function_name_filter_list" in config:
                seen["live_connection:config.function_name_filter_list"] = (
                    config["function_name_filter_list"]
                )
    elif surface == "prompts":
        command = f"/okg-{inputs.deployment}"
        for row in client.prompts():
            if row.get("command") == command:
                seen["prompt:command"] = row.get("command")
                seen["prompt:name"] = row.get("name")
    elif surface == "deep_links":
        for key in keys:
            function_id, _, valve = key.rpartition(".")
            valves = client.function_valves(function_id)
            if valve in valves:
                seen[key] = valves[valve]
    return seen


def _registered_all(client: Any, server_id: str) -> list[Mapping[str, Any]]:
    """Every redacted connection row carrying ``server_id``, in list order."""
    return [
        entry for entry in client.tool_servers()
        if (entry.get("info") or {}).get("id") == server_id
    ]


def _registered(client: Any, server_id: str) -> Optional[Mapping[str, Any]]:
    """The redacted connection row for ``server_id``, or None.

    ``tool_servers`` is S1's REDACTED accessor: the credential is already a
    digest before it reaches here, so no readback path in this module can
    put a clear key into a comparison, a note, or the written document.
    """
    for entry in client.tool_servers():
        if (entry.get("info") or {}).get("id") == server_id:
            return entry
    return None


def _compare(expected: Mapping[str, Any], seen: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Deviations, in ``sync``'s vocabulary. Unreadable is never a match."""
    from okg.substrate.chat.sync import _normalize

    out: list[dict[str, Any]] = []
    for key in sorted(expected):
        if key not in seen:
            out.append({
                "key": key,
                "expected": expected[key],
                "observed": None,
                "readable": False,
                "reason": "readback_unreadable",
            })
            continue
        observed = seen[key]
        want = expected[key]
        # The declared banner must be PRESENT in the composed list; the list
        # itself carries operator and health banners the manifest never named.
        if key == "banner:declared_content":
            if want in (observed or []):
                continue
            out.append({
                "key": key, "expected": want, "observed": observed,
                "readable": True, "reason": "readback_mismatch",
            })
            continue
        if _normalize(want) != _normalize(observed):
            out.append({
                "key": key, "expected": want, "observed": observed,
                "readable": True, "reason": "readback_mismatch",
            })
    return out


def chat_readback(
    inputs: Any,
    client: Any,
    plan: Mapping[str, Any],
    *,
    verify: bool = True,
) -> dict[str, Any]:
    """Compare the live instance against ``plan``. Writes nothing to it.

    Every surface gets one of ``match``, ``deviation``, ``dropped``,
    ``no_consumer`` or ``not_declared`` — ``sync``'s own words, so an
    operator reading a readback and a sync report is reading one
    vocabulary. ``ok`` is false whenever any surface deviates; a dropped
    or unconsumed surface is NOT a deviation, because it is the specified
    outcome and reporting it as drift would send an operator to fix a
    working site.
    """
    from okg.substrate.chat.live_tool_slot import resolve_live_tool_slot
    from okg.substrate.chat.sync import (
        VERIFY_HEALTHY,
        VERIFY_REJECTED,
        VERIFY_UNAVAILABLE,
        _prompts_proof,
    )

    if plan.get("plan_digest") != plan_digest(plan):
        raise ChatPlanError(
            "the plan handed to this readback does not digest to its recorded "
            "value, so the comparison would name a desired state nobody wrote.",
            data={"reason": "plan_digest_self_mismatch"},
            fix_hint="re-run `okg chat plan`",
        )
    running_version = client.version()
    prompts_driven, prompts_reason = _prompts_proof(
        data_dir=inputs.chat_data_dir, running_version=running_version,
    )
    installed = {str(f.get("id")) for f in client.functions()}

    surfaces: dict[str, Any] = {}
    deviations: list[dict[str, Any]] = []
    for name, block in sorted((plan.get("surfaces") or {}).items()):
        desired = dict(block.get("desired") or {})
        entry: dict[str, Any] = {
            "surface": name,
            "compared_keys": sorted(desired),
            "undetermined": list(block.get("undetermined") or []),
        }
        if name == "prompts" and not prompts_driven:
            entry.update(status="dropped", ok=True, refusal=prompts_reason)
            surfaces[name] = entry
            continue
        if name == "deep_links":
            if not desired:
                entry.update(
                    status="not_declared", ok=True,
                    refusal="the manifest declares no consumed deep-link base",
                )
                surfaces[name] = entry
                continue
            missing = [
                key for key in desired if key.rpartition(".")[0] not in installed
            ]
            if missing:
                entry.update(
                    status="no_consumer", ok=True,
                    refusal=(
                        "no installed component declares a valve reading the "
                        f"declared base(s): {sorted(missing)}"
                    ),
                )
                surfaces[name] = entry
                continue
        if name == "mcp":
            # The manifest half of this surface is the port and the tool
            # list, and neither is READABLE off the instance: the plan's
            # own `undetermined` says what the endpoint advertises is
            # measured. What IS measurable here is S1's probe, below.
            entry.update(status="probe_only", ok=True, refusal=None)
            surfaces[name] = entry
            continue
        found = _compare(
            desired, _observe(client, inputs, name, sorted(desired), desired),
        )
        entry.update(
            status="deviation" if found else "match",
            ok=not found,
            deviations=found,
        )
        deviations.extend({"surface": name, **item} for item in found)
        surfaces[name] = entry

    probe: dict[str, Any] = {
        "state": PROBE_NOT_PROBED, "detail": None, "auth_type": None,
        "stored_credential": None,
    }
    if verify:
        probe = _verify_probe(inputs, client)
        _stored_credential_deviation(probe, "mcp", "stored_credential", deviations, surfaces)
        if probe["state"] not in (VERIFY_HEALTHY, VERIFY_UNAVAILABLE):
            deviations.append({
                "surface": "mcp",
                "key": "verify_probe",
                "expected": VERIFY_HEALTHY,
                "observed": probe["state"],
                "readable": True,
                "reason": (
                    "mcp_connection_credential_rejected"
                    if probe["state"] == VERIFY_REJECTED
                    else "verify_probe_unmeasured"
                ),
            })
            surfaces.setdefault("mcp", {"surface": "mcp"}).update(
                status="deviation", ok=False,
            )

    live_probe: dict[str, Any] = {
        "state": PROBE_NOT_PROBED, "detail": None, "auth_type": None,
        "stored_credential": None,
    }
    live_slot = resolve_live_tool_slot(
        inputs.chat,
        deployment=inputs.deployment,
        mcp_server_id=inputs.mcp_server_id,
    )
    if verify and live_slot is not None and live_slot.registered:
        # Its OWN measurement. The graph probe says nothing about whether the
        # live connection's credential reaches the live endpoint -- different
        # URL, different variable, different server -- and reporting one
        # connection's health as both is the shape of defect #1738 was.
        live_probe = _verify_probe(inputs, client, slot=live_slot)
        _stored_credential_deviation(
            live_probe, "live_tools", "live_stored_credential", deviations, surfaces,
        )
        if live_probe["state"] not in (VERIFY_HEALTHY, VERIFY_UNAVAILABLE):
            deviations.append({
                "surface": "live_tools",
                "key": "live_verify_probe",
                "expected": VERIFY_HEALTHY,
                "observed": live_probe["state"],
                "readable": True,
                "reason": (
                    "mcp_connection_credential_rejected"
                    if live_probe["state"] == VERIFY_REJECTED
                    else "verify_probe_unmeasured"
                ),
            })
            surfaces.setdefault("live_tools", {"surface": "live_tools"}).update(
                status="deviation", ok=False,
            )

    return {
        "schema": CHAT_READBACK_SCHEMA,
        "deployment": inputs.deployment,
        "plan_digest": plan.get("plan_digest"),
        "openwebui_version": running_version,
        "ok": not deviations,
        "code": None if not deviations else ChatReadbackDeviation.code,
        "surfaces": surfaces,
        "deviations": deviations,
        "verify_probe": probe,
        "live_verify_probe": live_probe,
    }


def _stored_credential_deviation(
    probe: Mapping[str, Any],
    surface: str,
    key: str,
    deviations: list[dict[str, Any]],
    surfaces: dict[str, Any],
) -> None:
    """Record any stored-credential state but ``matches`` as a deviation.

    Fails closed: a credential readback could not compare (``masked``) or
    could not attribute to one connection (``duplicate``) has not been shown
    current, so it is a deviation, as ``verify_probe_unmeasured`` is for a
    verify route that failed. Field names and counts only, never a value.
    """
    stored = probe.get("stored_credential") or {}
    state = stored.get("state")
    if state == "stale":
        item = {
            "expected": "the credential the environment names",
            "observed": "a different stored credential",
            "differing": list(stored.get("differing") or []),
            "reason": "mcp_connection_credential_stale",
            "detail": (
                "the site stores a different credential from the one the "
                "environment names (for example a token rotated without "
                "`okg chat sync`); chat turns send the stored one"
            ),
        }
    elif state == "masked":
        item = {
            "expected": "a stored credential readback can compare",
            "observed": "the stored key is masked",
            "differing": [],
            "reason": "mcp_connection_credential_unmeasured",
            "detail": (
                "this Open WebUI version hides the stored key, so readback "
                "cannot verify that the site holds the current credential; "
                "run `okg chat sync` after any token rotation"
            ),
        }
    elif state == "duplicate":
        item = {
            "expected": "one connection with this server id",
            "observed": f"{stored.get('entries')} connections with this server id",
            "differing": [],
            "reason": "mcp_connection_duplicate_id",
            "detail": (
                "more than one tool-server connection carries this id, so "
                "readback cannot tell which credential a chat turn uses; "
                "remove the extra entries, then run `okg chat sync`"
            ),
        }
    else:
        return
    deviations.append({"surface": surface, "key": key, "readable": True, **item})
    surfaces.setdefault(surface, {"surface": surface}).update(
        status="deviation", ok=False,
    )


def _verify_probe(
    inputs: Any,
    client: Any,
    *,
    slot: Any = None,
) -> dict[str, Any]:
    """S1's instance-side verify, run read-only, plus a stored-credential check.

    The verify route dials with the credential this process resolves, as
    the apply does, because the readback's own view of the connection is
    redacted and a digest cannot be dialled. So a healthy answer proves the
    OPERATOR's credential reaches ``mcp-serve``; it says nothing about the
    credential the site stored, which is the one a chat turn sends. After a
    token rotation that was never synced the two differ (#2287), and
    ``stored_credential`` reports it by comparing digests of the stored
    and the resolved credential. ``unavailable`` — a version with no verify
    route — is an ABSENT measurement, never a bad credential.
    """
    from okg.substrate.chat.sync import (
        _INHERIT,
        _connection_payload,
        _mcp_connection_auth_type,
        _resolve_connection_credential,
    )

    server_id = slot.server_id if slot is not None else inputs.mcp_server_id
    matching = _registered_all(client, server_id)
    stored = matching[0] if matching else None
    if stored is None:
        return {
            "state": PROBE_NOT_REGISTERED,
            "detail": f"no connection carries the server id {server_id!r}",
            "auth_type": None,
            "stored_credential": None,
        }
    if slot is None and _mcp_connection_auth_type(inputs.chat) is not None:
        # The graph connection forwards each signed-in person's own token, so
        # no operator credential can stand in for it here. Not measured,
        # and said so -- never dialled with a credential it will not carry.
        return {
            "state": PROBE_NOT_PROBED,
            "detail": (
                "the connection forwards each signed-in person's token "
                "(chat.oidc_login); no operator credential can verify it"
            ),
            "auth_type": "system_oauth",
            "stored_credential": None,
        }
    connection = _connection_payload(
        url=slot.url if slot is not None else inputs.mcp_instance_url,
        server_id=server_id,
        deployment=inputs.deployment,
        filter_list=(),
        grants=(),
        # The GRAPH connection (no slot) inherits `chat.mcp.auth_token_env`;
        # the LIVE connection resolves the slot's own variable, and a slot
        # that declares none dials with NO credential. Passing the slot's
        # `None` through as "inherit" made this probe dial the live endpoint
        # with the graph bearer token, so a live connection the plan says is
        # unauthenticated verified under a credential it will never carry.
        auth_token=_resolve_connection_credential(
            inputs.chat,
            env_name=slot.auth_token_env if slot is not None else _INHERIT,
        ),
    )
    # The verify route dials with the credential in THIS request, not the one
    # the site stored, so after a token rotation it answers healthy for a
    # site whose every chat turn gets 401 (#2287). The stored credential is
    # compared here instead, digest against digest, inside this process.
    stored_credential = (
        {"state": "duplicate", "differing": [], "entries": len(matching)}
        if len(matching) > 1
        else _stored_credential_state(stored, connection)
    )
    try:
        state, detail = client.verify_tool_server(connection)
    except SubstrateError as exc:
        # NOT `unavailable`. `verify_tool_server` already answers 404 with
        # that state; everything it RAISES is the route itself failing -- a
        # rejected admin token, a 500 -- and folding those into "this
        # version has no verify route" would report an unmeasured
        # credential as a measured absence. Its own state, and a deviation,
        # because a readback that could not run its one credential
        # measurement has not shown the site healthy.
        return {
            "state": PROBE_ERROR,
            "detail": f"the verify route failed: {exc.code}",
            "auth_type": connection.get("auth_type"),
            "stored_credential": stored_credential,
        }
    return {
        "state": state, "detail": detail, "auth_type": connection.get("auth_type"),
        "stored_credential": stored_credential,
    }


def _stored_credential_state(
    stored: Mapping[str, Any], connection: Mapping[str, Any],
) -> dict[str, Any]:
    """Whether the site STORED the credential this process resolved.

    ``stored`` came through the redacted accessor, so its ``key`` and any
    credential-named header are already HMAC digests under this process's
    salt (``sync.connection_key_digest``). Digesting the resolved credential
    with the same function makes the two comparable without either value
    leaving this function; the answer carries field NAMES only, never a
    value or a digest.

    ``state`` is one of:

    * ``matches`` -- the site holds the credential the environment names.
    * ``stale`` -- it holds a different one (a rotation that was never
      synced, a hand edit, a partial sync). A chat turn uses the STORED
      credential, so the verify probe's healthy answer does not apply.
    * ``masked`` -- the version masks ``key`` on readback, so the key could
      not be compared. Not measured, so readback reports it as a deviation
      (``mcp_connection_credential_unmeasured``); 0.11.4 does not mask.

    A fourth state, ``duplicate``, is set by the caller when more than one
    connection carries the server id: which one a chat turn uses is then
    not knowable from here (``mcp_connection_duplicate_id``).
    """
    from okg.substrate.chat.sync import (
        CONNECTION_KEY_MASKED,
        _names_a_credential,
        connection_key_digest,
    )

    differing: list[str] = []
    if stored.get("auth_type") != connection.get("auth_type"):
        differing.append("auth_type")
    stored_key = stored.get("key")
    masked = stored_key == CONNECTION_KEY_MASKED
    if not masked and stored_key != connection_key_digest(connection.get("key")):
        differing.append("key")
    headers = stored.get("headers")
    # The vendor merges `headers` over the header it builds from `key`, so a
    # credential-named header the payload does not carry changes what a
    # chat turn sends. Sync always writes `headers: None`.
    if isinstance(headers, Mapping) and any(
        _names_a_credential(str(name)) for name in headers
    ):
        differing.append("headers")
    if differing:
        return {"state": "stale", "differing": differing}
    if masked:
        return {"state": "masked", "differing": []}
    return {"state": "matches", "differing": []}


def write_chat_readback(
    document: Mapping[str, Any], output_dir: Path | str,
) -> dict[str, Any]:
    """Write ``chat-readback.json``. A pre-existing file refuses, as for a plan."""
    output_dir = Path(output_dir)
    target = output_dir / READBACK_FILENAME
    if target.exists():
        raise ChatPlanOutputPathExists(
            f"{target} already exists. A readback is a dated observation; "
            "overwriting one in place destroys the earlier reading.",
            data={"reason": "output_path_exists", "path": str(target)},
            fix_hint="name an unused output directory",
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_json_bytes(document) + b"\n")
    return {"readback_path": str(target), "ok": bool(document.get("ok"))}
