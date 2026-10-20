"""Compose a bundle's chat contribution with the deployment's ``chat:`` block.

Slice S3 of #1183. #1179's install plan resolves one bundle of a verified
package; S2's plan derives the chat desired state from the deployment
manifest alone. Nothing joined them: the design note records that there is
no bundle -> ``ChatBlock`` path today, because a bundle's chat content was
addressed as deployment-directory FILE PATHS, which a package cannot
supply. This is that path. A bundle may declare a **chat contribution** —
one typed asset of kind ``chat-policy`` naming branding, instruction
playbooks, the graph connections the chat may dial, and the names of live
tools — and the deployment's ``chat:`` block composes with it.

**The precedence rule, stated once.** *The bundle supplies defaults; the
deployment manifest may narrow, never widen.*

* **Branding and instructions.** Where the ``chat:`` block declares a
  value it REPLACES the bundle's default (``ui.site_name``,
  ``preset.display_name``, ``preset.description``, and
  ``preset.system_prompt_ref``, which supersedes the bundle's playbooks
  wholesale). Where the block is silent, the bundle's value stands.
* **Connections and tools.** The bundle's declared sets are CEILINGS.
  ``chat.mcp.tools`` and the connections named by ``chat.mcp.audiences``
  must be subsets of what the bundle declared; a name outside the declared
  set is REFUSED, never composed away and never quietly dropped.

The asymmetry is the safety argument. Branding is presentation, and the
deployment running the site is entitled to name it. Connections and tools
are REACH: a bundle reviewed for two connections is not a bundle reviewed
for a third the manifest adds. Narrowing needs no new review; widening
does, and the place to do it is the bundle.

**What this is NOT.** Chat roles are not graph authorization: a composed
connection says which endpoint the site may be REGISTERED against, and
Nomos still decides which rows any subject sees. Nothing here writes an
Open WebUI table, dials an instance, or executes a packaged asset; the
result is plan content, and S2's apply and readback consume it unchanged.
The live-tool names are DECLARATIONS ONLY — S4 wires the slot — so a
manifest asking for one through the graph connection is refused rather
than granted a name with no operator behind it.

The instruction playbooks and the logo are declarations too: this slice
resolves and digests them and applies neither, so the plan records each
under ``undetermined`` beside the live-tool names rather than claiming a
contribution nothing consumes.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional

from okg.substrate.errors import SubstrateError


__all__ = [
    "CONTRIBUTION_ASSET_KIND",
    "CONTRIBUTION_SCHEMA",
    "INSTRUCTIONS_UNDETERMINED",
    "LIVE_TOOL_UNDETERMINED",
    "LOGO_UNDETERMINED",
    "BundleChatSelection",
    "ChatBundleAssetUnresolved",
    "ChatBundleCompositionError",
    "ChatBundleConnectionUnresolved",
    "ChatBundleContributionInvalid",
    "ChatBundleLiveToolConflict",
    "ChatBundleManifestWidens",
    "bundle_chat_selection",
    "compose_bundle_chat",
]


CONTRIBUTION_SCHEMA = "okg.deployment_product.chat_contribution/v1"
#: The contribution rides on the CORE asset kind that already means "a
#: declarative policy governing the chat surface". A new kind would have to
#: be admitted by every registry that enumerates them; this one is already
#: first-class, and a package that carries none simply contributes nothing.
CONTRIBUTION_ASSET_KIND = "chat-policy"
#: A connection is a graph endpoint the chat may be registered against, so
#: it must be a packaged, reviewed asset — never a raw URL. `connector` is
#: the core kind for exactly that.
CONNECTION_ASSET_KIND = "connector"
INSTRUCTION_ASSET_KIND = "playbook"

_CONTRIBUTION_KEYS = frozenset({
    "schema", "branding", "instructions", "connections", "graph_tools", "live_tools",
})
_BRANDING_KEYS = frozenset({"name", "description", "logo"})


class ChatBundleCompositionError(SubstrateError):
    """A bundle chat contribution refusal. Nothing is composed or written."""

    code = "chat_bundle_composition_refused"


class ChatBundleContributionInvalid(ChatBundleCompositionError):
    """The contribution asset is unreadable, ambiguous, or not the schema."""

    code = "chat_bundle_contribution_invalid"


class ChatBundleAssetUnresolved(ChatBundleCompositionError):
    """An instruction playbook or logo the contribution names is not carried."""

    code = "chat_bundle_asset_unresolved"


class ChatBundleConnectionUnresolved(ChatBundleCompositionError):
    """A declared graph connection is not an asset the install plan resolved."""

    code = "chat_bundle_connection_unresolved"


class ChatBundleLiveToolConflict(ChatBundleCompositionError):
    """One name is declared both as a live tool and as a graph operator.

    The manifest cannot reach this: ``chat.mcp.tools`` is already closed to
    the OKG operator universe when the block is parsed, so a manifest can
    never name a live tool at all. What it CAN do is name an operator the
    bundle also marked live, and a name with two transports behind it is a
    name S4's slot could not bind without choosing one silently.
    """

    code = "chat_bundle_live_tool_conflict"


class ChatBundleManifestWidens(ChatBundleCompositionError):
    """The manifest names reach the bundle did not declare."""

    code = "chat_bundle_manifest_widens"


@dataclass(frozen=True)
class BundleChatSelection:
    """One bundle's chat contribution, already bound to its install plan.

    Every reference the contribution made has been resolved against the
    plan's own asset inventory before this exists, so a consumer holds
    identities and digests rather than strings it would have to re-check.
    """

    bundle_id: str
    bundle_digest: str
    release_digest: str
    contribution: Mapping[str, Any]
    document: Mapping[str, Any]

    @property
    def branding(self) -> Mapping[str, Any]:
        return self.document.get("branding") or {}


def _refuse(
    error: type[ChatBundleCompositionError],
    message: str,
    *,
    fix_hint: str,
    reason: str,
    **data: object,
) -> ChatBundleCompositionError:
    return error(message, data={"reason": reason, **data}, fix_hint=fix_hint)


def _strings(value: object, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise _refuse(
            ChatBundleContributionInvalid,
            f"chat contribution `{field}` must be a list of strings",
            reason="field_not_a_list",
            field=field,
            fix_hint=f"write {field} as a list",
        )
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise _refuse(
                ChatBundleContributionInvalid,
                f"chat contribution `{field}` carries a non-string entry",
                reason="field_entry_not_a_string",
                field=field,
                fix_hint=f"every {field} entry is a non-empty string",
            )
        out.append(item)
    return tuple(out)


def _read_contribution(package: Any, qualified_id: str) -> dict[str, Any]:
    """Parse and shape-check the contribution bytes the package carries."""
    try:
        raw = package.read_asset(qualified_id)
        loaded = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 — every failure is one refusal
        raise _refuse(
            ChatBundleContributionInvalid,
            f"the chat contribution asset {qualified_id} could not be read as "
            f"JSON ({type(exc).__name__}). An unreadable contribution is not "
            "an absent one, so it refuses rather than composing the "
            "deployment's block alone.",
            reason="contribution_unreadable",
            qualified_id=qualified_id,
            fix_hint="rebuild the package; the asset's bytes are not a JSON document",
        ) from exc
    if not isinstance(loaded, Mapping) or loaded.get("schema") != CONTRIBUTION_SCHEMA:
        raise _refuse(
            ChatBundleContributionInvalid,
            f"the chat contribution asset {qualified_id} is not a "
            f"{CONTRIBUTION_SCHEMA} document.",
            reason="contribution_schema_unknown",
            qualified_id=qualified_id,
            fix_hint=f"declare schema: {CONTRIBUTION_SCHEMA}",
        )
    unknown = sorted(set(loaded) - _CONTRIBUTION_KEYS)
    if unknown:
        raise _refuse(
            ChatBundleContributionInvalid,
            f"the chat contribution asset {qualified_id} carries unrecognized "
            f"keys: {', '.join(unknown)}. The key set is closed so that a "
            "bundle cannot smuggle a host, a port or a credential past it.",
            reason="contribution_unknown_keys",
            qualified_id=qualified_id,
            fix_hint=f"remove {', '.join(unknown)}",
        )
    branding = loaded.get("branding") or {}
    if not isinstance(branding, Mapping) or (set(branding) - _BRANDING_KEYS):
        raise _refuse(
            ChatBundleContributionInvalid,
            f"the chat contribution asset {qualified_id} declares a branding "
            "block that is not a mapping of name, description and logo.",
            reason="branding_shape_invalid",
            qualified_id=qualified_id,
            fix_hint="branding accepts only name, description and logo",
        )
    return {
        "schema": CONTRIBUTION_SCHEMA,
        "branding": {key: branding.get(key) for key in sorted(_BRANDING_KEYS)},
        "instructions": _strings(loaded.get("instructions"), field="instructions"),
        "connections": _strings(loaded.get("connections"), field="connections"),
        "graph_tools": _strings(loaded.get("graph_tools"), field="graph_tools"),
        "live_tools": _strings(loaded.get("live_tools"), field="live_tools"),
    }


def _resolved(
    qualified_id: str,
    index: Mapping[str, Mapping[str, Any]],
    *,
    kind: Optional[str],
    error: type[ChatBundleCompositionError],
    what: str,
) -> dict[str, str]:
    """Resolve one reference against the plan. ``kind=None`` checks presence."""
    asset = index.get(qualified_id)
    if asset is None:
        raise _refuse(
            error,
            f"the chat contribution names the {what} {qualified_id}, which the "
            "install plan does not resolve. A reference the plan cannot name "
            "is not a reference the chat may be pointed at.",
            reason=f"{what}_absent",
            qualified_id=qualified_id,
            fix_hint=f"select {qualified_id} into the bundle, or drop the reference",
        )
    if kind is not None and asset.get("kind") != kind:
        raise _refuse(
            error,
            f"the chat contribution names {qualified_id} as a {what}, but the "
            f"package carries it as kind {asset.get('kind')!r}, not {kind!r}.",
            reason=f"{what}_wrong_kind",
            qualified_id=qualified_id,
            fix_hint=f"package {qualified_id} as kind {kind}",
        )
    return {"qualified_id": qualified_id, "asset_digest": str(asset.get("asset_digest"))}


def bundle_chat_selection(
    package: Any,
    install_plan: Mapping[str, Any],
) -> Optional[BundleChatSelection]:
    """The bundle's chat contribution, resolved against its install plan.

    Returns ``None`` when the selected bundle carries no contribution — a
    package with nothing to say about chat is not an error. Every
    reference is resolved here, so a selection that exists is one whose
    playbooks, logo and connections the plan actually carries.
    """
    index = {
        f"{item.get('owner')}:{item.get('id')}": item
        for item in install_plan.get("assets") or ()
        if isinstance(item, Mapping)
    }
    candidates = sorted(
        key for key, item in index.items()
        if item.get("kind") == CONTRIBUTION_ASSET_KIND
    )
    bundle = install_plan.get("bundle") or {}
    bundle_id = str(bundle.get("bundle_id"))
    if not candidates:
        return None
    if len(candidates) > 1:
        raise _refuse(
            ChatBundleContributionInvalid,
            f"bundle {bundle_id!r} selects {len(candidates)} {CONTRIBUTION_ASSET_KIND} "
            f"assets ({', '.join(candidates)}). Two contributions would have to "
            "be merged by a rule nobody declared, so this refuses rather than "
            "picking one.",
            reason="contribution_ambiguous",
            bundle_id=bundle_id,
            qualified_ids=candidates,
            fix_hint="select exactly one chat contribution into the bundle",
        )
    qualified_id = candidates[0]
    contribution = _read_contribution(package, qualified_id)

    logo = contribution["branding"].get("logo")
    document: dict[str, Any] = {
        "contribution": {
            "qualified_id": qualified_id,
            "asset_digest": str(index[qualified_id].get("asset_digest")),
        },
        "branding": {
            "name": contribution["branding"].get("name"),
            "description": contribution["branding"].get("description"),
            "logo": (
                None if logo is None
                else _resolved(
                    str(logo), index, kind=None,
                    error=ChatBundleAssetUnresolved, what="logo",
                )
            ),
        },
        "instructions": [
            _resolved(
                ref, index, kind=INSTRUCTION_ASSET_KIND,
                error=ChatBundleAssetUnresolved, what="playbook",
            )
            for ref in contribution["instructions"]
        ],
        "connections": [
            _resolved(
                ref, index, kind=CONNECTION_ASSET_KIND,
                error=ChatBundleConnectionUnresolved, what="connection",
            )
            for ref in contribution["connections"]
        ],
        "graph_tools": sorted(contribution["graph_tools"]),
        "live_tools": sorted(contribution["live_tools"]),
    }
    return BundleChatSelection(
        bundle_id=bundle_id,
        bundle_digest=str(bundle.get("bundle_digest")),
        release_digest=str((install_plan.get("package") or {}).get("release", {}).get(
            "release_digest",
        )),
        contribution=contribution,
        document=document,
    )


#: What the composed block records instead of guessing. The live-tool slot
#: is S4's; declaring the names here does NOT grant them, and saying so in
#: the plan is what keeps a reader from reading a declaration as a grant.
LIVE_TOOL_UNDETERMINED = {
    "key": "bundle.live_tools.selected",
    "reason": (
        "the bundle DECLARES live-tool names; the connection that would carry "
        "them is S4's slot, so nothing selects one yet and an empty selection "
        "is the measured state rather than an omission"
    ),
}
#: The same honesty for the two references this slice RESOLVES but does not
#: apply. The playbooks and the logo are named, carried and digested, and
#: nothing writes them onto the composed block: `preset.system_prompt_ref`
#: and `ui.logo_ref` stay whatever the manifest said. A record that claimed
#: the bundle supplied the site's instructions would describe a contribution
#: no apply consumes, which is the same defect as a plan value no apply
#: produces -- so it is recorded as undetermined instead.
INSTRUCTIONS_UNDETERMINED = {
    "key": "bundle.instructions.selected",
    "reason": (
        "the bundle DECLARES instruction playbooks; composing packaged text "
        "into the preset's system prompt is not this slice, so the deployment "
        "manifest's own system_prompt_ref is what the apply writes and no "
        "playbook is selected yet"
    ),
}
LOGO_UNDETERMINED = {
    "key": "bundle.branding.logo",
    "reason": (
        "the bundle DECLARES a logo asset; serving packaged BYTES to the "
        "instance is not this slice, so the deployment manifest's own logo "
        "reference is what the apply writes"
    ),
}


def compose_bundle_chat(
    chat: Any, selection: BundleChatSelection,
) -> tuple[Any, dict[str, Any]]:
    """``(effective chat block, the record the plan carries)``.

    Branding and instructions take the manifest's value where it declares
    one and the bundle's where it does not. Connections and tools are
    narrowed by the manifest against the bundle's declared ceiling, and a
    manifest naming reach the bundle never declared is refused here —
    before any plan is written, so a widened selection has no digest and
    no apply.

    A block whose ``mcp`` is ``None`` requests no tools, and that is NOT
    a request for none: every consumer reads an absent block as "the
    deployment named nothing" and applies ``CHAT_DEFAULT_TOOLS``
    (:func:`plan_tool_curation`). The ceiling is therefore charged against
    THAT set, and a default the bundle never declared refuses
    ``chat_bundle_manifest_widens`` on ``mcp:tools`` like any other
    widening. No verb reaches this today -- ``chat.mcp`` is mandatory
    wherever ``chat.enabled`` is true, and ``load_inputs`` refuses a chat
    that is not enabled -- so the arm guards the function rather than a
    live path: this is a PUBLIC entry point, and a ceiling that holds only
    because of a check two modules away is a ceiling one edit removes.

    The defaults are folded into a REPLACEMENT ``ChatBlock`` rather than
    into the plan document, and ``load_inputs`` puts that block on the
    inputs every consumer already reads. That is the whole reason a
    bundle default cannot become a plan that no apply produces: the plan,
    the apply and the readback are three readings of one block, not one
    reading of a composed plan and two of the manifest.
    """
    from dataclasses import replace

    from okg.substrate.chat.sync import DECLARATION_SUBSTRATE_DEFAULT
    from okg.substrate.deployments.chat_block import ChatUi
    from okg.substrate.mcp.operators import CHAT_DEFAULT_TOOLS

    document = selection.document
    mcp = chat.mcp
    declared_tools = tuple(document["graph_tools"])
    declared_live = frozenset(document["live_tools"])
    requested = tuple(mcp.tools) if mcp is not None else ()
    for tool in sorted(declared_live.intersection(declared_tools)):
        raise _refuse(
            ChatBundleLiveToolConflict,
            f"bundle {selection.bundle_id!r} declares {tool!r} both as a graph "
            "operator and as a live tool. One name cannot carry two "
            "transports: whichever the live-tool slot bound, the other would "
            "be reachable under the same name with different behavior.",
            reason="live_tool_shadows_graph_tool",
            tool=tool,
            fix_hint=f"rename the live tool, or drop {tool} from graph_tools",
        )
    # S4's slot. `chat.mcp.live_tools.names` is deliberately NOT closed to
    # the OKG operator universe -- a live tool is not an OKG operator -- so
    # this is the surface on which a manifest CAN widen a live-tool set, and
    # the arm S3 recorded as unreachable is reachable here. The bundle's
    # declared live tools are the ceiling; the manifest narrows.
    requested_live = tuple(
        mcp.live_tools.names
        if mcp is not None and getattr(mcp, "live_tools", None) is not None
        else ()
    )
    for tool in requested_live:
        if tool in declared_live:
            continue
        raise _refuse(
            ChatBundleManifestWidens,
            f"chat.mcp.live_tools.names names {tool!r}, which bundle "
            f"{selection.bundle_id!r} does not declare as a live tool. A live "
            "tool reaches an operational system, and the review that admitted "
            "the bundle covered only the live tools the bundle declared.",
            reason="manifest_widens_live_tools",
            surface="mcp:live_tools",
            undeclared=tool,
            declared=sorted(declared_live),
            fix_hint=(
                f"declare {tool} under the bundle contribution's live_tools, "
                "or drop it from chat.mcp.live_tools.names"
            ),
        )
    for tool in requested:
        if tool in declared_tools:
            continue
        raise _refuse(
            ChatBundleManifestWidens,
            f"chat.mcp.tools names {tool!r}, which bundle "
            f"{selection.bundle_id!r} does not declare. The manifest may narrow "
            "the bundle's declared tools; it may not add to them, because the "
            "review that admitted the bundle covered only what the bundle "
            "declared.",
            reason="manifest_widens_tools",
            surface="mcp:tools",
            undeclared=tool,
            declared=list(declared_tools),
            fix_hint=f"declare {tool} in the bundle's contribution, or drop it here",
        )

    if mcp is None:
        # No MCP block. `plan_tool_curation` reads that as "the deployment
        # named nothing" and applies CHAT_DEFAULT_TOOLS, so a block with no
        # `mcp` is served SIX operators -- outside a bundle that may have
        # declared one. Materialising the bundle's set onto the block is not
        # available: a ChatMcp needs a port and a credential the manifest
        # never gave. So the widening is refused, and the record below names
        # the default set in the curation's own vocabulary rather than the
        # bundle's, so the two readings cannot disagree.
        #
        # Unreachable through the verbs TODAY (chat.mcp is mandatory when
        # chat.enabled is true, and load_inputs refuses a chat that is not
        # enabled), and written anyway: this function is exported, and a
        # ceiling that holds only because of a check in another module is
        # one an unrelated edit can remove without touching this file.
        for tool in sorted(set(CHAT_DEFAULT_TOOLS) - set(declared_tools)):
            raise _refuse(
                ChatBundleManifestWidens,
                "the deployment declares no chat.mcp block, so the chat site "
                f"is served the substrate default tools "
                f"{sorted(CHAT_DEFAULT_TOOLS)}, which include {tool!r} — a "
                f"tool bundle {selection.bundle_id!r} does not declare. A "
                "default is a widening like any other: the review that "
                "admitted the bundle covered only what the bundle declared.",
                reason="manifest_widens_tools",
                surface="mcp:tools",
                undeclared=tool,
                declared=list(declared_tools),
                fix_hint=(
                    f"declare {tool} in the bundle's contribution, or declare "
                    "chat.mcp.tools naming only what the bundle declares"
                ),
            )

    #: An audience selects the declared connection whose ASSET ID it names,
    #: so the narrowing is written in the vocabulary the manifest already
    #: has rather than in a new key nobody declared.
    by_asset_id = {
        item["qualified_id"].split(":", 1)[1]: item for item in document["connections"]
    }
    audiences = tuple(mcp.audiences) if mcp is not None else ()
    selected_connections: list[dict[str, str]] = []
    for audience in audiences:
        item = by_asset_id.get(audience.name)
        if item is None:
            raise _refuse(
                ChatBundleManifestWidens,
                f"chat.mcp.audiences names {audience.name!r}, which is not one "
                f"of the graph connections bundle {selection.bundle_id!r} "
                "declares. An audience that names no declared connection would "
                "either reach an endpoint nobody reviewed or reach nothing at "
                "all, and both are refused rather than composed.",
                reason="manifest_widens_connections",
                surface="mcp:connections",
                undeclared=audience.name,
                declared=sorted(by_asset_id),
                fix_hint=(
                    f"declare a connector asset {audience.name} in the bundle, "
                    "or rename the audience to one it declares"
                ),
            )
        selected_connections.append(item)

    preset = chat.preset
    branding = document["branding"]
    ui = chat.ui
    name = branding.get("name")
    description = branding.get("description")
    site_from = "deployment" if (ui is not None and ui.site_name) else "bundle"
    preset_name_from = (
        "deployment" if (preset is not None and preset.display_name) else "bundle"
    )
    description_from = (
        "deployment" if (preset is not None and preset.description) else "bundle"
    )
    # What `plan_tool_curation` will apply, named in ITS vocabulary. With no
    # `chat.mcp` block the substrate default set applies (checked against the
    # bundle's ceiling above); with one, `chat.mcp.tools` is mandatory and
    # non-empty, so the manifest's set is the selection. There is no third
    # arm: "the bundle's declared set" is a state no curation ever reads.
    # "deployment" is the word the branding record already uses for the
    # manifest's side; `DECLARATION_SUBSTRATE_DEFAULT` is `plan_tool_curation`'s
    # own token for the other one, so a reader can line the record up against
    # the curation the apply performs.
    tools_from = DECLARATION_SUBSTRATE_DEFAULT if mcp is None else "deployment"
    selected_tools = (
        sorted(CHAT_DEFAULT_TOOLS) if mcp is None else sorted(requested)
    )

    effective = chat
    if name and site_from == "bundle":
        effective = replace(
            effective,
            ui=(ChatUi(site_name=name) if ui is None else replace(ui, site_name=name)),
        )
    if preset is not None and (
        (name and preset_name_from == "bundle")
        or (description and description_from == "bundle")
    ):
        effective = replace(
            effective,
            preset=replace(
                preset,
                display_name=(
                    name if (name and preset_name_from == "bundle")
                    else preset.display_name
                ),
                description=(
                    description if (description and description_from == "bundle")
                    else preset.description
                ),
            ),
        )

    record = {
        "bundle_id": selection.bundle_id,
        "bundle_digest": selection.bundle_digest,
        "release_digest": selection.release_digest,
        "contribution": dict(document["contribution"]),
        "branding": {
            "declared": dict(branding),
            # Which side each composed value came from, recorded rather than
            # left to be re-derived by comparing two documents.
            "site_name": site_from,
            "preset_name": preset_name_from,
            "preset_description": description_from,
        },
        "instructions": {
            "declared": [dict(item) for item in document["instructions"]],
            # A manifest system prompt supersedes the bundle's playbooks
            # WHOLESALE. Composing the two would produce an instruction text
            # neither side wrote and neither side reviewed. Where the manifest
            # names none, the source is UNDETERMINED rather than "bundle": the
            # playbooks are resolved and digested, and nothing in this slice
            # writes them onto the composed block, so a record naming the
            # bundle would claim a contribution no apply consumes.
            "selected": [],
            "source": (
                "deployment_override"
                if (preset and preset.system_prompt_ref)
                else "undetermined"
            ),
        },
        "connections": {
            "declared": [dict(item) for item in document["connections"]],
            "selected": (
                [dict(item) for item in selected_connections]
                if audiences
                else [dict(item) for item in document["connections"]]
            ),
        },
        "graph_tools": {
            "declared": list(declared_tools),
            "selected": selected_tools,
            "source": tools_from,
        },
        # S4. The manifest's slot IS the selection, and it has already been
        # checked against the ceiling above -- so this is a determined value,
        # not the `undetermined` placeholder S3 recorded. Empty where the
        # deployment declares no slot, which is a narrowing to nothing and
        # the default.
        "live_tools": {
            "declared": list(document["live_tools"]),
            "selected": sorted(requested_live),
        },
        "precedence": (
            "bundle supplies defaults; the deployment manifest narrows or "
            "overrides branding and instructions and may narrow connections "
            "and tools, but may never widen them"
        ),
    }
    return effective, record
