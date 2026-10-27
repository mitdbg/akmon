"""One declaration of what an agent may call, and the checks that keep it true.

PACT `effective-surface-declaration` (issue #2012). The agent contract used to
be spread across the server launch profile, the chat tool filter, Nomos
posture, the MCP instructions file, the chat system prompt and the description
overrides. They drifted, measurably: `deployments/okg-workspace` declared seven
chat tools, `okg chat sync` removed `query` because the deployment runs under
Nomos enforcement, and the server registered all seven anyway — so the
manifest stated a set that was true on no transport, and the system prompt
matched the projection only because someone kept it matching by hand.

An `agent_surface:` block is the single declaration. The server's registered
set, the chat projection, the instructions, the response budget and the
`inspect` kind list all derive from it, and anything that names an operator it
does not serve is REFUSED rather than warned about.

Three rules run through this module, and they are the same three that run
through `tool_profiles.py`, because this is the same problem one level up.

**A projection only narrows.** `projections.<client>.tools` must be a subset
of `served`. A projection naming a tool the server does not register is a
refusal, not a widening and not a silent drop.

**Nothing is absorbed.** A legacy key alongside the block, a budget outside
the substrate bounds, a live binding that nothing implements: each refuses by
name. A block that accepted a declaration it could not honor would leave the
deployment believing it had bounded something it had not.

**The block is OPTIONAL this change** (operator decision, 2026-09-20). A
manifest that declares no `agent_surface:` keeps today's behaviour, frozen.
Issue #2067 makes the block mandatory and deletes the frozen path. The freeze
is bounded rather than open-ended because the two paths cannot coexist in one
manifest: declaring the block refuses every legacy key.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.images.config import ImagesConfig, ImagesConfigInvalid
from okg.substrate.mcp.operators import (
    ALL_ADMITTED_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)
from okg.substrate.mcp.operator_models import (
    INSPECT_TARGET_KINDS,
    INSPECT_KIND_ALIASES,
)
from okg.substrate.mcp.response_bounds import (
    MAX_RESPONSE_BYTES,
    MIN_RESPONSE_BYTES,
)


class AgentSurfaceInvalid(SubstrateError):
    """The `agent_surface:` block cannot mean what it says."""

    code = "agent_surface_invalid"


class AgentSurfaceProjectionWiderThanServed(AgentSurfaceInvalid):
    """A client projection names a tool the server does not register."""

    code = "agent_surface_projection_wider_than_served"


class AgentSurfaceResponseBytesOutOfRange(AgentSurfaceInvalid):
    """A declared response budget is outside the substrate's own bounds."""

    code = "agent_surface_response_bytes_out_of_range"


class AgentSurfaceLegacyKeyPresent(AgentSurfaceInvalid):
    """A manifest declares both `agent_surface:` and a key it replaces.

    Refused rather than merged or preferred: the whole point of the block is
    that one place states the surface, and a manifest holding both would make
    the answer depend on which loader asked.
    """

    code = "agent_surface_legacy_key_present"


class AgentSurfaceConflictsWithMcpBlock(AgentSurfaceLegacyKeyPresent):
    """A manifest declares both `agent_surface:` and a top-level `mcp:`.

    A subclass so anything catching the legacy-key refusal catches this too,
    and a distinct code because the fix differs: `mcp.tool_profiles` is a
    structure to move, not a single key to delete.
    """

    code = "agent_surface_conflicts_with_mcp_block"


class WriteLaunchUnservedByDeclaration(SubstrateError):
    """`--allow-writes` on a deployment whose `served` names no write tool.

    A NAMED launch-time refusal, not a silently narrower start. Chosen to
    match what this area already does one level up: a `--tools` naming
    an unserved tool raises `ToolProfileToolAbsent` rather than narrowing, and
    an unreachable expansion ceiling raises rather than starting a server that
    advertises something it cannot honor. The reading is the same here — the
    operator asked for a write surface and would have got a read one, with no
    signal anywhere that the flag did nothing.

    Not a manifest-load refusal: `served` alone is legal, and a deployment
    that never intends to serve writes is a normal deployment. It is the
    COMBINATION of the flag and the declaration that cannot mean anything, and
    only the launch knows about the flag.
    """

    code = "agent_surface_write_launch_unserved"


class LiveBindingsNotImplemented(AgentSurfaceInvalid):
    """RETIRED by issue #2013; kept so the code string stays resolvable.

    #2075 reserved `agent_surface.live_bindings` and refused every non-empty
    declaration with this error, because an accepted-but-inert binding is the
    silent fallback this repository forbids. #2013 implements the dispatch,
    so a well-formed binding now parses.

    The refusal was NARROWED, not deleted: `external_backed/live_relations.py`
    still fails deployment load by name for a binding with an unknown adapter
    kind, a malformed shape, a relation no subtype declares, or a declaration
    asking for `principal_mode: caller`. Nothing is accepted-but-inert.

    Nothing raises this any more. It is not removed because
    `live_bindings_not_implemented` may appear in an operator's notes or in a
    logged refusal from a #2075-era deployment, and a code that resolves to
    nothing is worse than one that resolves to this explanation.
    """

    code = "live_bindings_not_implemented"


class InspectKindNotServed(SubstrateError):
    """An `inspect` call named a target kind this surface does not serve.

    Raised by the router BEFORE the kind dispatch, so the caller is told the
    kind is withheld and what would serve it. Letting the narrowed input model
    refuse instead would produce a schema-shaped message that cannot tell
    "this kind does not exist" from "this launch withholds it" — the same
    distinction `ToolProfileToolAbsent` preserves at the tool level.
    """

    code = "agent_surface_inspect_kind_unserved"


class SurfaceTextNamesUnservedOperator(SubstrateError):
    """A prompt or instructions file tells a model to call a missing tool."""

    code = "agent_surface_text_names_unserved_operator"


class ServedSetMismatch(SubstrateError):
    """A live `tools/list` and the declared served set are not equal."""

    code = "agent_surface_served_set_mismatch"


# ---------------------------------------------------------------------------
# inspect kinds
# ---------------------------------------------------------------------------
#
# The split is by what a kind LETS A CALLER DO, and the condition for each
# admin kind is DERIVED rather than named after a profile. A profile name
# would be the wrong key now that `served` is a deployment-declared list: a
# deployment can serve `record` without being "the write profile".

#: Served whenever `inspect` is served. Graph, schema, lineage, history
#: (`node_history`, `subtype_diff`: PACT okg-attribute-history-read, decided
#: by the same Nomos read filter as `node`) and the caller's own session
#: state.
READ_INSPECT_KINDS: frozenset[str] = frozenset({
    "graph", "coverage", "ontology", "session_start",
    "subtype", "view", "node", "nodes", "text", "texts", "attr",
    "session", "generation", "generation_delta",
    "node_history", "subtype_diff",
})

#: Served on a write/admin launch. Admission ledger and curation queue.
WRITE_ADMIN_INSPECT_KINDS: frozenset[str] = frozenset({
    "admission", "record", "alias_suggestions",
})

#: Served when `record` is served. Both describe what a `record` call may
#: write, so on a surface without `record` they document a tool that is not
#: there — the same defect as instructions naming a pruned tool.
RECORD_INSPECT_KINDS: frozenset[str] = frozenset({
    "record_prefixes", "record_templates",
})

#: Served on a write/admin launch, OR whenever a session can widen itself.
#:
#: Two conditions, not one. `surface` reads the served tool surface and, with
#: `action="expand"`, MUTATES this session's tool list, which puts it with the
#: other admin kinds. But it is also the expansion door —
#: `inspect(target={"kind": "surface", "action": "expand"})` is how a narrowed
#: session widens — so a launch that configures a ceiling must serve it
#: whatever else it serves, or the ceiling is advertised and unreachable.
#:
#: The design's first draft named only the ceiling condition. That would have
#: taken `surface` off the write/admin launch, which serves every kind today
#: and is the control for this change's byte work: the write baseline must not
#: move, or a shrink in the read surface cannot be told from a leak into the
#: write one.
CEILING_INSPECT_KINDS: frozenset[str] = frozenset({"surface"})

#: Served when the deployment declares an image source (#3046).
#:
#: Conditional, not a read kind, for two reasons. It is the one kind that
#: returns bytes a node points at, so a deployment that has said nothing
#: about images must not grow the ability. And every other deployment's
#: published schema stays byte-identical: the surface baselines are the
#: control that shows the kind did not leak.
IMAGE_INSPECT_KINDS: frozenset[str] = frozenset({"image"})

assert (
    READ_INSPECT_KINDS
    | WRITE_ADMIN_INSPECT_KINDS
    | RECORD_INSPECT_KINDS
    | CEILING_INSPECT_KINDS
    | IMAGE_INSPECT_KINDS
) == frozenset(INSPECT_TARGET_KINDS), (
    "every inspect kind must be classified; an unclassified kind would be "
    "routed by the body and absent from every published schema"
)


def served_inspect_kinds(
    served_tools: Iterable[str],
    *,
    allow_writes: bool,
    has_expansion_ceiling: bool = False,
    has_image_source: bool = False,
) -> frozenset[str]:
    """Which `inspect` target kinds this surface publishes and routes.

    One function, used twice: to build the published union before
    registration, and by the router guard at call time. They cannot disagree
    because they are the same call.
    """
    tools = {str(t) for t in served_tools}
    kinds = set(READ_INSPECT_KINDS)
    if allow_writes:
        kinds |= WRITE_ADMIN_INSPECT_KINDS
    if "record" in tools:
        kinds |= RECORD_INSPECT_KINDS
    # `surface` is served on every launch since #3666 (M10): a read launch
    # answers it read-only, with the forms it accepts and does not publish;
    # `action="expand"` still needs a ceiling and is refused without one, so
    # `has_expansion_ceiling` no longer changes the kind set.
    kinds |= CEILING_INSPECT_KINDS
    if has_image_source:
        kinds |= IMAGE_INSPECT_KINDS
    return frozenset(kinds)


def kind_serving_condition(kind: str) -> str:
    """Plain words for what would serve `kind`. Used in the refusal."""
    key = INSPECT_KIND_ALIASES.get(kind, kind)
    if key in WRITE_ADMIN_INSPECT_KINDS:
        return "a write/admin launch (--allow-writes)"
    if key in RECORD_INSPECT_KINDS:
        return "a surface that serves the `record` tool"
    if key in CEILING_INSPECT_KINDS:
        return "any launch that serves `inspect`"
    if key in IMAGE_INSPECT_KINDS:
        return "a deployment that declares an image source"
    if key in READ_INSPECT_KINDS:
        return "any launch that serves `inspect`"
    return "no known launch: this is not an inspect target kind"


def assert_inspect_kind_served(
    kind: Any, *, served_kinds: Iterable[str],
) -> None:
    """Refuse an unserved `inspect` kind by name, before the kind dispatch."""
    served = frozenset(served_kinds)
    key = INSPECT_KIND_ALIASES.get(str(kind), str(kind))
    if key in served:
        return
    known = key in set(INSPECT_TARGET_KINDS)
    raise InspectKindNotServed(
        f"inspect target kind {str(kind)!r} is not served by this surface. "
        + (
            f"It is a real kind, withheld here; it is served by "
            f"{kind_serving_condition(key)}."
            if known else
            f"It is not an inspect target kind at all. This surface serves "
            f"{sorted(served)}; the full kind list is "
            f"{sorted(INSPECT_TARGET_KINDS)}."
        )
        + " Refusing by name rather than as a schema mismatch: a caller has "
        "to be able to tell a kind that does not exist from one this launch "
        "withholds, because the fix differs.",
        data={
            "kind": str(kind),
            "served_kinds": sorted(served),
            "known_kind": known,
            "serves_it": kind_serving_condition(key) if known else None,
        },
        fix_hint=(
            f"call a served kind ({sorted(served)}), or relaunch with "
            f"{kind_serving_condition(key)}" if known else
            f"use one of {sorted(served)}"
        ),
    )


# ---------------------------------------------------------------------------
# the affirmative text scan
# ---------------------------------------------------------------------------
#
# `tools_named_in` (tool_profiles.py) is a word-boundary scan over a whole
# file, and it WARNS. It cannot be promoted to a refusal as it stands: `map`,
# `filter`, `search`, `record` and `query` are ordinary English words, and a
# sentence such as "filter the results yourself" would refuse a deployment for
# prose. Worse, the honest case reads the other way round:
# `deployments/okg-ci/mcp-instructions.md:20` says "do not use `query`", which
# is exactly what a good instructions file says about an unserved tool, and a
# naive scan would refuse it for saying so.
#
# So the refusal fires only at AFFIRMATIVE positions, and a do-not-use line is
# exempt. The loose scan stays, as a warning, to catch what this misses.

#: A prohibition: a negator DIRECTLY governing a calling verb, in the clause
#: that carries the name.
#:
#: Deliberately small and explicit. A scan for any negative word anywhere
#: nearby got both directions wrong at once — it exempted "Do not hesitate to
#: use `query`" and "You cannot avoid using `query`", which are instructions
#: to call the tool, while refusing "Use `search` instead of `query`", which
#: is not. Requiring the negator to sit immediately in front of the verb
#: (`do not use`, `never call`) separates the two, because that is what a real
#: prohibition looks like.
_PROHIBITION = re.compile(
    r"\b(?:do\s+not|don't|never|must\s+not|may\s+not|should\s+not|"
    r"cannot|can't)\s+(?:ever\s+)?"
    r"(?:use|call|invoke|run|issue)\b[^.;:,]*$",
    re.IGNORECASE,
)

#: Where a clause ends, commas included, so a prohibition in an earlier clause
#: cannot excuse a call in a later one.
_CLAUSE_END = re.compile(r"(?<=[.:;!?,])\s")

#: A list-item head: the name is the first thing on a bulleted line.
_LIST_HEAD = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s*$")

@dataclass(frozen=True)
class ToolMention:
    """One occurrence of a tool name in authored surface text."""

    line: int
    tool: str
    position: str
    text: str

    @property
    def refusable(self) -> bool:
        """Only a REFUSAL POSITION can fail a deployment check.

        `list_head` and `call` are the two shapes that tell a model to call
        something: a bulleted tool listing, and a written-out call. A name in
        the middle of a sentence is prose — "there is no `query` tool", "use
        `search` instead of `query`" — and prose is reported as a warning,
        never refused. Getting this wrong in either direction is worse than
        not checking: a false refusal teaches authors to write around the
        checker.
        """
        return self.position in {"list_head", "call"}


def scan_tool_mentions(
    text: str, *, candidates: Iterable[str] | None = None,
) -> tuple[ToolMention, ...]:
    """Every backticked or call-shaped tool name, classified by position.

    `position` is one of:

    * `list_head` — the name heads a bulleted item (``- `search` — ...``).
    * `call` — the name is written as a call (`` `query(` ``).
    * `prohibited` — a `list_head` or `call` that a prohibition governs
      ("never call `query(...)`"). Reported, not refused.
    * `mention` — anywhere else. Prose, reported as a warning only.

    Only `list_head` and `call` are refusable. The earlier version refused
    `mention` too, unless a negative word appeared somewhere in the clause,
    and that was wrong in both directions at once: it refused "Use `search`
    instead of `query`" and exempted "Do not hesitate to use `query`".
    """
    pool = tuple(candidates) if candidates is not None else ALL_ADMITTED_TOOLS
    out: list[ToolMention] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        for name in pool:
            pattern = rf"`{re.escape(name)}[`(]"
            for match in re.finditer(pattern, line):
                before = line[: match.start()]
                # The clause since the last sentence boundary. Scoped rather
                # than whole-line so a later negation does not excuse an
                # earlier instruction to call something.
                clause = _CLAUSE_END.split(before)[-1] if before else ""
                if _LIST_HEAD.match(before):
                    position = "list_head"
                elif line[match.start():].startswith(f"`{name}("):
                    position = "call"
                else:
                    position = "mention"
                if position != "mention" and _PROHIBITION.search(clause):
                    position = "prohibited"
                out.append(
                    ToolMention(
                        line=lineno,
                        tool=name,
                        position=position,
                        text=line.strip(),
                    )
                )
    return tuple(sorted(out, key=lambda m: (m.line, m.tool)))


def assert_text_serves_only(
    text: str,
    *,
    served: Iterable[str],
    source: str,
    deployment: str,
    channel_note: str = "",
) -> None:
    """Refuse authored surface text that tells a model to call a missing tool.

    Refusal positions only — a bulleted tool listing or a written-out call.
    Prose that merely names an unserved tool, and a prohibition against
    calling one, are both left alone: telling a model NOT to call a withheld
    tool is what a good instructions file does.
    """
    served_set = {str(t) for t in served}
    offending = [
        m for m in scan_tool_mentions(text)
        if m.refusable and m.tool not in served_set
    ]
    if not offending:
        return
    names = sorted({m.tool for m in offending})
    where = "; ".join(
        f"{m.tool!r} at line {m.line} ({m.position})" for m in offending
    )
    raise SurfaceTextNamesUnservedOperator(
        f"deployment {deployment!r}: {source} names operator(s) {names} that "
        f"this surface does not serve — {where}. Served: "
        f"{sorted(served_set)}."
        + (f" {channel_note}" if channel_note else "")
        + " Only a bulleted tool listing or a written-out call is refused; "
        "prose that merely names a tool, and a prohibition against calling "
        "one, are not.",
        data={
            "deployment": deployment,
            "source": source,
            "unserved": names,
            "served": sorted(served_set),
            "mentions": [
                {"line": m.line, "tool": m.tool, "position": m.position}
                for m in offending
            ],
        },
        fix_hint=(
            f"add {names} to agent_surface.served (or to the projection this "
            f"text describes), or remove the names from {source}"
        ),
    )


def assert_served_sets_equal(
    *,
    declared: Iterable[str],
    observed: Iterable[str],
    deployment: str,
    endpoint: str,
    allow_writes: bool,
    projected: Iterable[str] | None = None,
) -> None:
    """Refuse when a live `tools/list` is not EQUAL to what that launch must
    register.

    `declared` is `agent_surface.served`, the whole surface. `allow_writes` is
    the mode the CALLER bound this endpoint in, and it is required rather than
    inferred from the listing: a read-bound endpoint accidentally launched
    `--allow-writes` lists the write set, and inferring the mode from that
    would call the mistake correct. The comparison is against
    `registered_tools_for_launch` for the bound mode, not against `declared`
    itself — a read-only endpoint of a deployment that also
    declares write tools legitimately lists seven of eighteen, and comparing
    to `declared` refused every real launch.

    The pre-existing check in `chat/sync.py` asserts the live listing is a
    SUPERSET of the bound tools, which is a different question: it catches a
    projection pointing at a tool that does not exist. It cannot catch the
    defect this PACT exists for — a server serving seven while the deployment
    declares six passes a superset check, and that was the measured state of
    `okg-workspace`.

    `projected` is the chat projection, which must be a subset of what the
    endpoint actually registers. The manifest already proves it is a subset of
    `declared`; this proves it against the live listing, which is the set the
    client will really meet.
    """
    declared_set = registered_tools_for_launch(
        declared, allow_writes=allow_writes, deployment=deployment,
    )
    observed_set = {str(t) for t in observed}
    if projected is not None:
        missing = sorted({str(t) for t in projected} - observed_set)
        if missing:
            raise ServedSetMismatch(
                f"deployment {deployment!r}: the chat projection names "
                f"{missing}, which the MCP endpoint at {endpoint} does not "
                f"list. A projection may only narrow what the endpoint "
                f"registers; binding a client to a tool that is not there "
                f"makes every call fail at the wire.",
                data={"deployment": deployment, "endpoint": endpoint,
                      "projected_not_listed": missing,
                      "observed": sorted(observed_set)},
                fix_hint=(
                    "remove those tools from the chat projection, or serve "
                    "them from this endpoint"
                ),
            )
    if declared_set == observed_set:
        return
    served_not_listed = sorted(declared_set - observed_set)
    listed_not_served = sorted(observed_set - declared_set)
    raise ServedSetMismatch(
        f"deployment {deployment!r}: the MCP endpoint at {endpoint} lists a "
        f"tool set that is not what a "
        f"{'write/admin' if allow_writes else 'read-only'} launch of this "
        f"declared surface must register. "
        f"Declared but not listed: {served_not_listed}. Listed but not "
        f"declared: {listed_not_served}. Refusing rather than accepting a "
        "superset: a server serving more than the deployment declares is the "
        "drift this block exists to remove, and every session pays for the "
        "extra schemas.",
        data={
            "deployment": deployment,
            "endpoint": endpoint,
            "allow_writes": allow_writes,
            "expected": sorted(declared_set),
            "observed": sorted(observed_set),
            "served_not_listed": served_not_listed,
            "listed_not_served": listed_not_served,
        },
        fix_hint=(
            "restart mcp-serve so it registers the declared set, or change "
            "agent_surface.served to match what the server actually serves"
        ),
    )


# ---------------------------------------------------------------------------
# the block
# ---------------------------------------------------------------------------

#: Top-level keys `agent_surface:` replaces. Declaring the block and one of
#: these is a refusal, which is what keeps the frozen legacy path (issue
#: #2067) from being a second way to say the same thing in ONE manifest.
LEGACY_TOP_LEVEL_KEYS: Mapping[str, str] = {
    "mcp_instructions": "agent_surface.instructions",
    "tool_description_overrides":
        "agent_surface.projections.chat.description_overrides",
    "mcp": "agent_surface.served",
}

#: `inspect_kinds` was here and is deliberately NOT: it was parsed, stored
#: and never read, because which kinds a surface publishes is DERIVED from
#: the tools that launch registers (`served_inspect_kinds`), not chosen. A
#: key that cannot change any behaviour is worse than no key — an operator
#: would set it and believe it did something.
_BLOCK_KEYS = frozenset({
    "served", "response_bytes", "instructions",
    "projections", "live_bindings", "images", "live_types",
})
_PROJECTION_KEYS = frozenset({
    "tools", "system_prompt_ref", "description_overrides",
})
_RESPONSE_BYTES_KEYS = frozenset({"default", "ceiling"})
_KNOWN_PROJECTIONS = frozenset({"chat"})


@dataclass(frozen=True)
class ClientProjection:
    """What one client is given, always a narrowing of `served`."""

    name: str
    tools: tuple[str, ...]
    system_prompt_ref: str | None = None
    description_overrides: str | None = None


@dataclass(frozen=True)
class AgentSurface:
    """A deployment's one declaration of its agent surface."""

    served: tuple[str, ...]
    response_default_bytes: int | None = None
    response_ceiling_bytes: int | None = None
    instructions: str | None = None
    projections: Mapping[str, ClientProjection] = field(default_factory=dict)
    #: Relation name -> binding, parsed by `live_relations.parse_live_bindings`.
    #: Cross-checked against what the enrolled subtypes DECLARE at server
    #: build, not here: this parser sees the manifest, not the catalog.
    live_bindings: Mapping[str, Any] = field(default_factory=dict)
    #: The live types this surface serves (issue #3666, PACT
    #: live-graph-rucio-sites): subtype names declared with `okg_live_type`.
    #: Empty serves none. Like `live_bindings`, a name is checked against the
    #: catalog at server build (`build_live_type_registry`), not here.
    live_types: tuple[str, ...] = ()
    #: The parsed `images:` block (#3046), or None when the block does not
    #: declare one. None is not "images off": ingest still renders file
    #: images at `ImagesConfig.default()`, and no URL is fetched. Use
    #: `images_config` for the settings that apply either way.
    images: ImagesConfig | None = None

    @property
    def images_config(self) -> ImagesConfig:
        return self.images if self.images is not None else ImagesConfig.default()

    @property
    def served_set(self) -> frozenset[str]:
        return frozenset(self.served)

    def serves_live_type(self, subtype: str) -> bool:
        """True when `agent_surface.live_types` names `subtype`.

        The surface half of "is this live type served". A live read also
        needs `expand` served and the caller's own permission; the resolver
        checks those.
        """
        return subtype in self.live_types

    def projection(self, name: str) -> ClientProjection | None:
        return self.projections.get(name)

    @property
    def effective_response_default(self) -> int:
        from okg.substrate.mcp.response_bounds import DEFAULT_RESPONSE_BYTES

        if self.response_default_bytes is not None:
            return self.response_default_bytes
        if self.response_ceiling_bytes is not None:
            return min(DEFAULT_RESPONSE_BYTES, self.response_ceiling_bytes)
        return DEFAULT_RESPONSE_BYTES

    @property
    def effective_response_ceiling(self) -> int:
        return self.response_ceiling_bytes or MAX_RESPONSE_BYTES


def _where(manifest_path: Any) -> str:
    return f" (Manifest: {manifest_path})" if manifest_path else ""


def _strict_keys(
    body: Mapping[str, Any], allowed: frozenset[str], *, label: str, where: str,
) -> None:
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise AgentSurfaceInvalid(
            f"deployment.yaml `{label}` has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(allowed)}.{where}",
            data={"unknown_keys": unknown, "accepted_keys": sorted(allowed)},
            fix_hint=f"fix the key spelling under `{label}`",
        )


def _tool_list(
    value: Any, *, label: str, where: str,
) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise AgentSurfaceInvalid(
            f"deployment.yaml `{label}` must be a non-empty list of tool "
            f"names; got {type(value).__name__}. An empty surface cannot be "
            f"told apart from a declaration that was ignored.{where}",
            fix_hint=f"name at least one tool under `{label}`",
        )
    names = [str(v).strip() for v in value]
    if any(not n for n in names):
        raise AgentSurfaceInvalid(
            f"deployment.yaml `{label}` contains an empty tool name.{where}",
            fix_hint=f"remove the blank entry from `{label}`",
        )
    unknown = sorted(set(names) - set(ALL_ADMITTED_TOOLS))
    if unknown:
        raise AgentSurfaceInvalid(
            f"deployment.yaml `{label}` names tool(s) {unknown} that the "
            f"substrate MCP server never registers; the known tool universe "
            f"is {list(ALL_ADMITTED_TOOLS)}.{where}",
            data={"unknown_tools": unknown,
                  "known_tools": list(ALL_ADMITTED_TOOLS)},
            fix_hint=f"fix the tool spelling under `{label}`",
        )
    return tuple(dict.fromkeys(names))


def _response_bytes(
    body: Any, *, where: str,
) -> tuple[int | None, int | None]:
    if body is None:
        return None, None
    if not isinstance(body, dict):
        raise AgentSurfaceInvalid(
            f"deployment.yaml `agent_surface.response_bytes` must be a "
            f"mapping with `default` and/or `ceiling`; got "
            f"{type(body).__name__}.{where}",
            fix_hint="declare `response_bytes: {default: N, ceiling: M}`",
        )
    _strict_keys(
        body, _RESPONSE_BYTES_KEYS,
        label="agent_surface.response_bytes", where=where,
    )
    values: dict[str, int | None] = {}
    for key in ("default", "ceiling"):
        raw = body.get(key)
        if raw is None:
            values[key] = None
            continue
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise AgentSurfaceResponseBytesOutOfRange(
                f"deployment.yaml `agent_surface.response_bytes.{key}` must "
                f"be an integer number of bytes; got "
                f"{type(raw).__name__}.{where}",
                fix_hint=f"set `response_bytes.{key}` to an integer",
            )
        values[key] = raw

    ceiling = values["ceiling"]
    default = values["default"]
    if ceiling is not None and not (
        MIN_RESPONSE_BYTES <= ceiling <= MAX_RESPONSE_BYTES
    ):
        raise AgentSurfaceResponseBytesOutOfRange(
            f"deployment.yaml `agent_surface.response_bytes.ceiling` is "
            f"{ceiling}, outside the substrate bounds "
            f"{MIN_RESPONSE_BYTES}..{MAX_RESPONSE_BYTES}. A deployment "
            f"ceiling may only LOWER the substrate maximum, never raise "
            f"it.{where}",
            data={"ceiling": ceiling, "min": MIN_RESPONSE_BYTES,
                  "max": MAX_RESPONSE_BYTES},
            fix_hint=(
                f"set `response_bytes.ceiling` between {MIN_RESPONSE_BYTES} "
                f"and {MAX_RESPONSE_BYTES}"
            ),
        )
    if default is not None:
        upper = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
        if not (MIN_RESPONSE_BYTES <= default <= upper):
            raise AgentSurfaceResponseBytesOutOfRange(
                f"deployment.yaml `agent_surface.response_bytes.default` is "
                f"{default}, outside {MIN_RESPONSE_BYTES}..{upper} "
                f"({'the declared ceiling' if ceiling is not None else 'the substrate maximum'})."
                f"{where}",
                data={"default": default, "min": MIN_RESPONSE_BYTES,
                      "upper": upper, "ceiling": ceiling},
                fix_hint=(
                    f"set `response_bytes.default` between "
                    f"{MIN_RESPONSE_BYTES} and {upper}"
                ),
            )
    return default, ceiling


def _projection(
    name: str, body: Any, *, served: Sequence[str], where: str,
) -> ClientProjection:
    label = f"agent_surface.projections.{name}"
    if name not in _KNOWN_PROJECTIONS:
        raise AgentSurfaceInvalid(
            f"deployment.yaml declares an agent-surface projection {name!r}; "
            f"the known client projections are "
            f"{sorted(_KNOWN_PROJECTIONS)}.{where}",
            data={"projection": name,
                  "known": sorted(_KNOWN_PROJECTIONS)},
            fix_hint=(
                f"rename the projection to one of "
                f"{sorted(_KNOWN_PROJECTIONS)}"
            ),
        )
    if not isinstance(body, dict):
        raise AgentSurfaceInvalid(
            f"deployment.yaml `{label}` must be a mapping; got "
            f"{type(body).__name__}.{where}",
            fix_hint=f"declare `{label}: {{tools: [...]}}`",
        )
    _strict_keys(body, _PROJECTION_KEYS, label=label, where=where)
    tools = _tool_list(body.get("tools"), label=f"{label}.tools", where=where)
    wider = sorted(set(tools) - set(served))
    if wider:
        raise AgentSurfaceProjectionWiderThanServed(
            f"deployment.yaml `{label}.tools` names {wider}, which "
            f"`agent_surface.served` does not serve (served: "
            f"{sorted(served)}). A projection may only NARROW the served "
            f"set: widening it would bind a client to tools the server never "
            f"registers, and every call would be refused at the wire.{where}",
            data={"projection": name, "wider_than_served": wider,
                  "projection_tools": list(tools), "served": sorted(served)},
            fix_hint=(
                f"add {wider} to `agent_surface.served`, or remove them from "
                f"`{label}.tools`"
            ),
        )
    return ClientProjection(
        name=name,
        tools=tools,
        system_prompt_ref=(
            str(body["system_prompt_ref"])
            if body.get("system_prompt_ref") is not None else None
        ),
        description_overrides=(
            str(body["description_overrides"])
            if body.get("description_overrides") is not None else None
        ),
    )


def parse_agent_surface(
    raw: Any, *, manifest_path: Any = None,
) -> AgentSurface | None:
    """Validate and parse a manifest `agent_surface:` block, or return None.

    Strict at every level. The top-level manifest census matches only
    top-level NAMES, so a misspelled key nested under `agent_surface:` would
    otherwise load clean and bound nothing.
    """
    if not isinstance(raw, dict):
        return None
    block = raw.get("agent_surface")
    if block is None:
        return None
    where = _where(manifest_path)
    if not isinstance(block, dict):
        raise AgentSurfaceInvalid(
            f"deployment.yaml `agent_surface:` must be a mapping; got "
            f"{type(block).__name__}.{where}",
            fix_hint="declare `agent_surface:` with a `served:` list",
        )
    _strict_keys(block, _BLOCK_KEYS, label="agent_surface", where=where)

    for key, replacement in LEGACY_TOP_LEVEL_KEYS.items():
        if raw.get(key) is None:
            continue
        error = (
            AgentSurfaceConflictsWithMcpBlock if key == "mcp"
            else AgentSurfaceLegacyKeyPresent
        )
        raise error(
            f"deployment.yaml declares both `agent_surface:` and the legacy "
            f"top-level `{key}:` that it replaces. Refusing rather than "
            f"merging or preferring one: the block exists so that ONE place "
            f"states the surface, and a manifest holding both would make the "
            f"answer depend on which loader asked.{where}",
            data={"legacy_key": key, "replacement": replacement},
            fix_hint=f"move `{key}` to `{replacement}` and delete it",
        )

    served = _tool_list(
        block.get("served"), label="agent_surface.served", where=where,
    )

    default_bytes, ceiling_bytes = _response_bytes(
        block.get("response_bytes"), where=where,
    )

    # Issue #2013 fills this slot. #2075 reserved it and refused every
    # non-empty declaration with `live_bindings_not_implemented`; that
    # refusal is NARROWED here, not removed. A binding whose adapter kind is
    # unknown, whose shape is wrong, or that names a relation no subtype
    # declares still fails deployment load by name -- see
    # `external_backed/live_relations.py`. What changed is that a WELL-FORMED
    # binding now parses and is dispatched instead of being refused wholesale.
    from okg.substrate.external_backed.live_relations import (
        parse_live_bindings,
    )

    live_bindings = parse_live_bindings(block.get("live_bindings"), where=where)
    live_types = _live_types(block.get("live_types"), served=served, where=where)

    raw_projections = block.get("projections") or {}
    if not isinstance(raw_projections, dict):
        raise AgentSurfaceInvalid(
            f"deployment.yaml `agent_surface.projections` must be a mapping "
            f"of client name to projection; got "
            f"{type(raw_projections).__name__}.{where}",
            fix_hint="declare `projections: {chat: {tools: [...]}}`",
        )
    projections = {
        str(name): _projection(
            str(name), body, served=served, where=where,
        )
        for name, body in raw_projections.items()
    }

    images = None
    if block.get("images") is not None:
        # Re-raised as the block's own refusal so every caller that handles
        # `agent_surface_invalid` (load, lint) handles this one too; the
        # image error stays chained and its `data.field` is kept.
        try:
            images = ImagesConfig.parse(block["images"], where=where)
        except ImagesConfigInvalid as exc:
            raise AgentSurfaceInvalid(
                exc.message, data=dict(exc.data), fix_hint=exc.fix_hint,
            ) from exc

    instructions = block.get("instructions")
    return AgentSurface(
        served=served,
        response_default_bytes=default_bytes,
        response_ceiling_bytes=ceiling_bytes,
        instructions=str(instructions) if instructions is not None else None,
        projections=projections,
        live_bindings=live_bindings,
        live_types=live_types,
        images=images,
    )


_LIVE_TYPE_NAME = re.compile(r"[a-z][a-z0-9_]{0,63}")


def _live_types(
    value: Any, *, served: tuple[str, ...], where: str,
) -> tuple[str, ...]:
    """Parse `agent_surface.live_types` (issue #3666).

    A non-empty list of subtype names. Absent serves no live type. Whether
    each name IS a declared live type is checked at server build, where the
    catalog is known. A list on a surface that does not serve `expand` is
    refused here: a live read needs the permission `expand` needs, so the
    types could never be read.
    """
    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise AgentSurfaceInvalid(
            f"deployment.yaml `agent_surface.live_types` must be a non-empty "
            f"list of subtype names; got {value!r}. Leave the key out to "
            f"serve no live type.{where}",
            data={"field": "live_types"},
            fix_hint="list the live types to serve, e.g. `live_types: [site, system]`",
        )
    names = []
    for item in value:
        if not isinstance(item, str) or not _LIVE_TYPE_NAME.fullmatch(item):
            raise AgentSurfaceInvalid(
                f"deployment.yaml `agent_surface.live_types` entry {item!r} "
                f"is not a subtype name (lower snake case).{where}",
                data={"field": "live_types", "entry": item},
                fix_hint="write each live type as its subtype name, e.g. `site`",
            )
        names.append(item)
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise AgentSurfaceInvalid(
            f"deployment.yaml `agent_surface.live_types` repeats "
            f"{duplicates}.{where}",
            data={"field": "live_types", "duplicates": duplicates},
            fix_hint="list each live type once",
        )
    if "expand" not in served:
        raise AgentSurfaceInvalid(
            f"deployment.yaml `agent_surface.live_types` names {names}, but "
            f"`agent_surface.served` does not serve `expand`. A live read "
            f"needs the permission `expand` needs, so these types could "
            f"never be read.{where}",
            data={"field": "live_types", "live_types": names},
            fix_hint="add `expand` to `agent_surface.served`, or drop `live_types`",
        )
    return tuple(names)


def query_withheld_by_access_rules(deployment_name: str) -> str | None:
    """The access-rule mode that takes `query` off a surface, or None.

    Jason, via orchestrator chat 2026-10-13, query-by-default: serve query
    by default without access rules (issue #3666). None when the deployment's Nomos mode
    is `off`; otherwise the mode (`audit`, `enforce`), and `unreadable` when
    it cannot be read: `query` is then withheld rather than served past rules
    that may apply.
    """
    import logging

    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    try:
        mode = deployment_nomos_enforcement_mode(deployment_name)
    except Exception:  # noqa: BLE001 - fail closed: withhold query
        logging.getLogger(__name__).warning(
            "the Nomos mode of %s could not be read; query is not served",
            deployment_name, exc_info=True,
        )
        return "unreadable"
    return None if mode == "off" else str(mode)


def registered_tools_for_launch(
    served: Iterable[str], *, allow_writes: bool, deployment: str | None = None,
) -> frozenset[str]:
    """What a launch of this declared surface actually REGISTERS.

    `served` is the whole surface across every launch mode, so it is not what
    any one launch serves: a read-only launch of a deployment declaring all
    eighteen tools registers seven. One function answers that, and both
    callers use it — registration asserts its result as a post-condition, and
    `okg chat sync` compares the live `tools/list` against it.

    Comparing a live listing against `served` itself is the mistake this
    replaces: it made `okg chat sync` refuse every real launch, because the
    endpoint legitimately lists a subset.
    """
    allowed = set(OPERATOR_TOOLS)
    if allow_writes:
        allowed |= set(WRITE_ADMIN_TOOLS)
    if deployment is not None and query_withheld_by_access_rules(deployment) is not None:
        # Access rules take `query` off every launch (issue #3666).
        allowed.discard("query")
    return frozenset(allowed & {str(t) for t in served})


def launch_mode_of(observed: Iterable[str]) -> bool:
    """Whether an observed listing LOOKS like a write launch.

    Reporting only. It must never decide which set to require: inferring the
    intended mode from the listing makes the equality check unable to fail in
    the one direction that matters. A read-bound endpoint that was launched
    `--allow-writes` by mistake lists eighteen tools, the inference says "so
    it is a write launch", and the check then approves the very drift it
    exists to catch. The caller states the mode it bound; this only helps a
    refusal say what it saw.
    """
    return bool({str(t) for t in observed} & set(WRITE_ADMIN_TOOLS))


def assert_write_launch_is_served(
    surface: AgentSurface | None,
    *,
    allow_writes: bool,
    deployment: str,
) -> None:
    """Refuse `--allow-writes` that the declaration cannot honor."""
    if surface is None or not allow_writes:
        return
    write_tools = set(surface.served) & set(WRITE_ADMIN_TOOLS)
    if write_tools:
        return
    raise WriteLaunchUnservedByDeclaration(
        f"this launch passes --allow-writes, but deployment {deployment!r} "
        f"declares an agent_surface.served set with no write or admin tool "
        f"in it ({sorted(surface.served)}). `served` is the whole surface "
        f"across every launch mode, so the flag could not add one: the "
        f"server would start read-only with nothing said. Refusing rather "
        f"than narrowing silently, the same way a --tools naming an "
        f"unserved tool refuses.",
        data={
            "deployment": deployment,
            "served": sorted(surface.served),
            "write_admin_tools": sorted(WRITE_ADMIN_TOOLS),
        },
        fix_hint=(
            f"add the write tools this deployment should serve to "
            f"`agent_surface.served` (the full set is "
            f"{sorted(WRITE_ADMIN_TOOLS)}), or drop --allow-writes"
        ),
    )


def assert_surface_files_agree(
    surface: AgentSurface,
    *,
    deployment: str,
    deployment_dir: Path,
    scan: tuple[str, ...] = ("instructions", "chat_prompt"),
) -> None:
    """Refuse authored text that names an operator the surface withholds.

    Static: it reads the declared sets and the files on disk, and needs no
    running server, so `okg deployments lint` can answer before anything is
    deployed.

    `scan` selects which files to read. `build_server` passes
    `("instructions",)` alone: the chat system prompt belongs to a different
    client, and scanning it at server build let an edit to chat prose refuse
    an unrelated stdio or streamable-http launch that never sends that prompt
    to anybody. The prompt is scanned by `okg deployments lint` and by
    `okg chat sync`, which are the two places that act on it.
    """
    if "instructions" in scan and surface.instructions:
        path = deployment_dir / surface.instructions
        if path.is_file():
            assert_text_serves_only(
                path.read_text(encoding="utf-8"),
                served=surface.served,
                source=f"the MCP instructions file {surface.instructions}",
                deployment=deployment,
            )
    chat = surface.projection("chat") if "chat_prompt" in scan else None
    if chat is not None and chat.system_prompt_ref:
        path = deployment_dir / chat.system_prompt_ref
        if path.is_file():
            assert_text_serves_only(
                path.read_text(encoding="utf-8"),
                served=chat.tools,
                source=f"the chat system prompt {chat.system_prompt_ref}",
                deployment=deployment,
                channel_note=(
                    "Open WebUI never delivers the MCP server's initialize "
                    "instructions to the model, so this prompt is the "
                    "model's only instruction channel: a name it carries is "
                    "a call the model will attempt and the connection will "
                    "refuse."
                ),
            )


__all__ = [
    "AgentSurface",
    "AgentSurfaceConflictsWithMcpBlock",
    "AgentSurfaceInvalid",
    "AgentSurfaceLegacyKeyPresent",
    "AgentSurfaceProjectionWiderThanServed",
    "AgentSurfaceResponseBytesOutOfRange",
    "CEILING_INSPECT_KINDS",
    "ClientProjection",
    "InspectKindNotServed",
    "LEGACY_TOP_LEVEL_KEYS",
    "LiveBindingsNotImplemented",
    "READ_INSPECT_KINDS",
    "RECORD_INSPECT_KINDS",
    "ServedSetMismatch",
    "SurfaceTextNamesUnservedOperator",
    "WriteLaunchUnservedByDeclaration",
    "ToolMention",
    "WRITE_ADMIN_INSPECT_KINDS",
    "assert_inspect_kind_served",
    "assert_served_sets_equal",
    "launch_mode_of",
    "registered_tools_for_launch",
    "assert_surface_files_agree",
    "assert_write_launch_is_served",
    "assert_text_serves_only",
    "kind_serving_condition",
    "parse_agent_surface",
    "scan_tool_mentions",
    "served_inspect_kinds",
]
