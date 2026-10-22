"""Named MCP tool profiles, and the instructions that follow the served set.

Per `mcp-tool-surface-profiles` (issue #1457). Two concerns live here because
they have ONE input: the set of tools this launch actually serves. Which tools
that is, and which sentences the server is entitled to say about them, must be
derived from the same value or they drift — which is exactly the measured
defect: `OKG_MCP_TOOL_ALLOWLIST=inspect,search,aggregate` served 12,433 B of
which 2,346 B told the model to call `expand`, `filter`, `map`, and `query`,
tools that had already been pruned.

Three rules run through this module.

**Nothing is absorbed.** A profile or allowlist naming a tool that is not
registered RAISES. Before this change it narrowed silently: `inspect,serach`
served only `inspect`, with no refusal and no warning, so a typo read as
agreement. An empty narrowing raises too — a server with no tools cannot be
told apart from a server whose narrowing was ignored.

**A profile only NARROWS.** Resolution never adds a tool. The admitted
boundary stays `apply_admitted_tool_surface`'s: `OPERATOR_TOOLS`, plus the
write/admin set only under `--allow-writes`. A profile naming a tool that
boundary withholds is refused, not granted.

**Two selectors at one launch is a refusal, not a merge.** `--tools`
and `OKG_MCP_TOOL_ALLOWLIST` together are refused. Intersecting them would
serve a set matching neither reading of the launch command, and preferring one
would silently discard an operator's explicit narrowing.

Static profiles are the supported mechanism BECAUSE in-session expansion is
not available: issue #1458 records that the SDK advertises
`tools.listChanged: false`, that `ToolManager._tools` is process-wide across
concurrent streamable-http sessions, and that no client has been observed
re-listing. Moving between profiles is a server restart. Nothing in this
module emits a tool-list-changed notification or claims one is possible.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.operators import (
    ALL_ADMITTED_TOOLS,
    COMPOSITION_TOOLS,
    OPERATOR_TOOLS,
)


class ToolProfileConfigInvalid(SubstrateError):
    """A deployment's `mcp:` block is malformed.

    Raised at manifest load, strict rather than tolerant: a silently-ignored
    `default_tool_profle:` would leave a deployment believing it had bounded
    its MCP surface when it had not.
    """

    code = "tool_profile_config_invalid"


class UnknownToolProfile(SubstrateError):
    """A launch named a profile no built-in and no deployment declares.

    Never degrades to the unnarrowed surface: a typo'd profile serving the
    full 18,823-byte surface is the cost this feature exists to avoid, and
    the operator would have no signal that it happened.
    """

    code = "unknown_tool_profile"


class ToolProfileToolAbsent(SubstrateError):
    """A narrowing names a tool this server does not serve.

    Two causes, deliberately distinguished in the message: the name is not an
    OKG tool at all (a typo), or it is a real tool this launch withholds (a
    write/admin tool without --allow-writes, or an unprovisioned conditional
    tool). Both refuse; the operator fix differs.
    """

    code = "tool_profile_tool_absent"


class ToolSurfaceSelectionConflict(SubstrateError):
    """More than one narrowing selector was supplied at one launch."""

    code = "tool_surface_selection_conflict"


class InstructionSurfaceMismatch(SubstrateError):
    """The served instructions were rendered for a different tool set.

    A build-time post-condition, not an operator error. The instructions are
    rendered before registration and the narrowing is enforced after it, so
    this proves the two agree instead of assuming it; a divergence would
    silently reintroduce the defect the profile work exists to fix.
    """

    code = "mcp_instruction_surface_mismatch"


#: Always means "serve everything this launch admits", and may not be
#: declared. A deployment that sets `default_tool_profile` must never be able
#: to trap an operator inside a narrowed surface, so the escape hatch is
#: reserved rather than deployment-defined. It is the analogue of the
#: reserved `all` in `search_profiles.py`.
RESERVED_FULL = "full"

#: The profile name a deployment-declared `agent_surface.served` resolves to.
#: Reserved like `full`: a deployment cannot declare a profile by this name,
#: so `--tools agent_surface` can never mean something other than "the
#: set this deployment declares".
AGENT_SURFACE_PROFILE = "agent_surface"

#: Built-in profiles. Names are reserved: a deployment redeclaring one would
#: make `--tools discovery` mean different things on different
#: deployments, and the operator reading the launch command could not tell.
#:
#: `discovery` is the minimal find-and-read set measured in issue #1457
#: (12,433 B served versus 18,823 B, a 34% reduction). It deliberately omits
#: `expand`, so a chunk hit cannot be traversed to its parent Document; that
#: limitation and its upgrade path are documented in docs/reference/mcp.md.
#: `reader` is the full seven-tool AKMON read algebra — the surface a
#: read-only launch serves today, named so it can be selected explicitly.
BUILTIN_TOOL_PROFILES: Mapping[str, tuple[str, ...]] = {
    "discovery": ("aggregate", "inspect", "search"),
    "reader": tuple(OPERATOR_TOOLS),
}

#: Strict key sets. The top-level manifest census
#: (`tests/substrate/test_no_unconsumed_manifest_fields.py`) matches only
#: top-level NAMES, and the string `mcp` occurs throughout `src/okg/`, so it
#: cannot detect a misspelled key NESTED under `mcp:`. These sets do that.
_MCP_BLOCK_KEYS = frozenset({"tool_profiles", "default_tool_profile", "dataset_card"})
_PROFILE_KEYS = frozenset({"tools", "description"})


@dataclass(frozen=True)
class ToolProfile:
    """One profile: a non-empty set of tool names to narrow to."""

    name: str
    tools: tuple[str, ...]
    description: str | None = None


@dataclass(frozen=True)
class ToolProfileConfig:
    """A deployment's `mcp:` block, already validated."""

    profiles: Mapping[str, ToolProfile]
    default_tool_profile: str | None = None
    #: `mcp.dataset_card` (PACT dataset-card-switch): False turns the dataset
    #: card off, so the server neither gathers nor serves it. Absent is on.
    dataset_card: bool = True

    @property
    def declared_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.profiles))


@dataclass(frozen=True)
class ResolvedToolSurface:
    """The narrowing a launch actually applies.

    `tools is None` means no narrowing — serve whatever the admitted boundary
    leaves. Distinct from an EMPTY set, which is refused rather than served:
    a server with no tools is indistinguishable from one whose narrowing was
    dropped on the floor.

    `source` is what selected it, so the readback can say why the surface is
    what it is instead of leaving the operator to guess between a flag, an
    environment variable, and a deployment default.
    """

    profile: str | None = None
    source: str = "unnarrowed"
    tools: frozenset[str] | None = None
    #: The launch could not read the deployment manifest, so a declared
    #: `mcp.default_tool_profile` — if there is one — could not be honored,
    #: and the surface is WIDER than the deployment may have asked for. Never
    #: true when an explicit selector was given: the operator then stated the
    #: surface themselves and no default applies.
    unhonored_manifest_default: bool = False

    @property
    def is_narrowed(self) -> bool:
        return self.tools is not None


#: The unnarrowed surface. Not "no profile was resolvable" — that raises.
UNNARROWED = ResolvedToolSurface()


def profile_names() -> tuple[str, ...]:
    """Every built-in name, plus the reserved escape hatch."""
    return tuple(sorted({*BUILTIN_TOOL_PROFILES, RESERVED_FULL}))


# ---------- manifest side ----------

def parse_tool_profile_config(
    raw: Any, *, manifest_path: Any = None,
) -> ToolProfileConfig | None:
    """Validate and parse a manifest `mcp:` block. Returns None when absent.

    NOTE for readers coming from `chat.mcp.tools`: that block is a different
    thing. It filters an Open WebUI chat preset's tool list AFTER the server
    has registered its surface; it does not affect registration. THIS block
    decides what the server registers in the first place.
    """
    where = f" (Manifest: {manifest_path})" if manifest_path else ""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ToolProfileConfigInvalid(
            f"deployment.yaml `mcp:` must be a mapping; got "
            f"{type(raw).__name__}.{where}",
            fix_hint=(
                "declare `mcp:` as a mapping with `tool_profiles:` and an "
                "optional `default_tool_profile:`"
            ),
        )
    unknown = sorted(set(raw) - _MCP_BLOCK_KEYS)
    if unknown:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml `mcp:` has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(_MCP_BLOCK_KEYS)}.{where}",
            data={"unknown_keys": unknown,
                  "accepted_keys": sorted(_MCP_BLOCK_KEYS)},
            fix_hint="fix the key spelling under `mcp:`",
        )

    raw_profiles = raw.get("tool_profiles") or {}
    if not isinstance(raw_profiles, dict):
        raise ToolProfileConfigInvalid(
            f"deployment.yaml `mcp.tool_profiles` must be a mapping of "
            f"name -> profile; got {type(raw_profiles).__name__}.{where}",
            fix_hint="declare `mcp.tool_profiles` as name -> {tools: [...]}",
        )

    profiles: dict[str, ToolProfile] = {}
    for name, body in raw_profiles.items():
        profiles[str(name)] = _parse_profile(str(name), body, where=where)

    default_profile = raw.get("default_tool_profile")
    if default_profile is not None:
        default_profile = str(default_profile)
        known = set(profiles) | set(BUILTIN_TOOL_PROFILES) | {RESERVED_FULL}
        if default_profile not in known:
            raise UnknownToolProfile(
                f"deployment.yaml `mcp.default_tool_profile` is "
                f"{default_profile!r}, which is neither a built-in profile "
                f"({sorted(BUILTIN_TOOL_PROFILES)}), the reserved "
                f"{RESERVED_FULL!r}, nor declared under "
                f"`mcp.tool_profiles` ({sorted(profiles)}).{where}",
                data={"requested": default_profile, "known": sorted(known)},
                fix_hint=(
                    f"set `mcp.default_tool_profile` to one of "
                    f"{sorted(known)}, or declare the profile under "
                    f"`mcp.tool_profiles`"
                ),
            )
    dataset_card = raw.get("dataset_card", True)
    if not isinstance(dataset_card, bool):
        raise ToolProfileConfigInvalid(
            f"deployment.yaml `mcp.dataset_card` must be true or false; got "
            f"{dataset_card!r}.{where}",
            fix_hint=(
                "write `dataset_card: false` to turn the dataset card off, or "
                "leave the key out to keep it on"
            ),
        )
    return ToolProfileConfig(
        profiles=profiles, default_tool_profile=default_profile,
        dataset_card=dataset_card,
    )


def dataset_card_enabled(config: ToolProfileConfig | None) -> bool:
    """Whether the server gathers and serves the dataset card.

    PACT dataset-card-switch: on unless the deployment's `mcp:` block says
    `dataset_card: false`. No `mcp:` block means on.
    """
    return config is None or config.dataset_card


def _parse_profile(name: str, body: Any, *, where: str) -> ToolProfile:
    if name == AGENT_SURFACE_PROFILE:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml declares an MCP tool profile named "
            f"{AGENT_SURFACE_PROFILE!r}, which is reserved: it always means "
            f"the set this deployment's `agent_surface.served` declares."
            f"{where}",
            fix_hint=(
                f"rename the profile; {AGENT_SURFACE_PROFILE!r} cannot be "
                f"declared"
            ),
        )
    if name == RESERVED_FULL:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml declares an MCP tool profile named "
            f"{RESERVED_FULL!r}, which is reserved: it always means the "
            f"unnarrowed surface, so a deployment default can never trap an "
            f"operator inside a narrowing.{where}",
            fix_hint=f"rename the profile; {RESERVED_FULL!r} cannot be declared",
        )
    if name in BUILTIN_TOOL_PROFILES:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml redeclares the built-in MCP tool profile "
            f"{name!r} (built-in contents: "
            f"{list(BUILTIN_TOOL_PROFILES[name])}). Built-in names are "
            f"reserved so `--tools {name}` means the same thing on "
            f"every deployment.{where}",
            data={"profile": name,
                  "builtin_tools": list(BUILTIN_TOOL_PROFILES[name])},
            fix_hint=(
                f"rename the profile; built-in names are "
                f"{sorted(BUILTIN_TOOL_PROFILES)}"
            ),
        )
    if not isinstance(body, dict):
        raise ToolProfileConfigInvalid(
            f"deployment.yaml MCP tool profile {name!r} must be a mapping; "
            f"got {type(body).__name__}.{where}",
            fix_hint=f"declare `{name}: {{tools: [...]}}`",
        )
    unknown = sorted(set(body) - _PROFILE_KEYS)
    if unknown:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml MCP tool profile {name!r} has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(_PROFILE_KEYS)}.{where}",
            data={"profile": name, "unknown_keys": unknown,
                  "accepted_keys": sorted(_PROFILE_KEYS)},
            fix_hint=f"fix the key spelling under `mcp.tool_profiles.{name}`",
        )
    tools = body.get("tools")
    if not isinstance(tools, list) or not tools:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml MCP tool profile {name!r} must declare a "
            f"non-empty `tools` list; a server with no tools cannot be told "
            f"apart from one whose narrowing was ignored.{where}",
            data={"profile": name},
            fix_hint=f"give `mcp.tool_profiles.{name}.tools` at least one tool",
        )
    names = [str(t).strip() for t in tools]
    if any(not t for t in names):
        raise ToolProfileConfigInvalid(
            f"deployment.yaml MCP tool profile {name!r} declares an empty "
            f"tool name.{where}",
            fix_hint=f"remove the blank entry from `mcp.tool_profiles.{name}.tools`",
        )
    unknown_tools = sorted(set(names) - set(ALL_ADMITTED_TOOLS))
    if unknown_tools:
        raise ToolProfileConfigInvalid(
            f"deployment.yaml MCP tool profile {name!r} names tool(s) "
            f"{unknown_tools} that the substrate MCP server never registers; "
            f"the known tool universe is {list(ALL_ADMITTED_TOOLS)}.{where}",
            data={"profile": name, "unknown_tools": unknown_tools,
                  "known_tools": list(ALL_ADMITTED_TOOLS)},
            fix_hint=(
                f"fix the tool spelling under `mcp.tool_profiles.{name}."
                f"tools`, or use one of {list(ALL_ADMITTED_TOOLS)}"
            ),
        )
    description = body.get("description")
    return ToolProfile(
        name=name,
        tools=tuple(dict.fromkeys(names)),
        description=str(description) if description is not None else None,
    )


# ---------- launch side ----------

def resolve_tool_surface(
    *,
    requested_profile: str | None,
    env_allowlist: set[str] | None,
    config: ToolProfileConfig | None = None,
    manifest_unresolved: bool = False,
    agent_surface_served: Iterable[str] | None = None,
) -> ResolvedToolSurface:
    """Decide the narrowing one launch applies, or refuse.

    Precedence is deliberately flat rather than layered. An EXPLICIT selector
    (`--tools`, or `OKG_MCP_TOOL_ALLOWLIST`) states what this launch
    serves and wins over a deployment `default_tool_profile`, so the surface a
    reader infers from the launch command is the surface served. Two explicit
    selectors at once is a refusal: neither reading would be true.

    `agent_surface_served` is the set a deployment's `agent_surface:` block
    declares (PACT `effective-surface-declaration`, issue #2012). It is the
    BASE surface, not a default: a deployment that declares it has stated
    what its server registers on every transport. An explicit selector still
    wins, because it is an operator statement about one launch, but it can
    only NARROW — `assert_narrowing_is_servable` runs against what the base
    admits, so a selector naming a tool outside `served` refuses. A
    deployment that declares the block has no `mcp:` block to fall back to:
    the two are refused together at manifest load.

    `manifest_unresolved` says the deployment manifest could not be read from
    this checkout — a real state for a deployment whose install root lives
    elsewhere, and for an ad-hoc `--dsn` server. Any declared
    `default_tool_profile` is then unreadable, so the result is UNNARROWED but
    carries `unhonored_manifest_default`, and the caller says so out loud.

    Why a flag rather than a refusal: refusing would break every ad-hoc and
    remote-root launch, including the many whose deployment declares no bound
    at all, punishing the common case for a state that is usually benign. But
    the widening must not be silent either — a deployment that DID declare a
    bound would otherwise serve the full surface with nothing said, which is
    the absorption this module forbids. An explicit selector clears the flag:
    the operator has stated the surface, so no unread default applies.
    """
    if requested_profile is not None and env_allowlist is not None:
        raise ToolSurfaceSelectionConflict(
            f"this launch names both `--tools "
            f"{requested_profile}` and OKG_MCP_TOOL_ALLOWLIST="
            f"{','.join(sorted(env_allowlist))}. Refusing rather than "
            f"combining them: an intersection would serve a set matching "
            f"neither, and preferring one would silently discard the other.",
            data={"requested_profile": requested_profile,
                  "env_allowlist": sorted(env_allowlist)},
            fix_hint=(
                "drop --tools, or unset OKG_MCP_TOOL_ALLOWLIST"
            ),
        )

    if requested_profile is not None:
        return _resolve_named(
            requested_profile, config=config, source="cli",
        )

    if env_allowlist is not None:
        if not env_allowlist:
            raise ToolProfileConfigInvalid(
                "OKG_MCP_TOOL_ALLOWLIST is set but names no tool, which "
                "would serve a server with no tools at all. An empty "
                "narrowing is refused rather than served: it cannot be told "
                "apart from a narrowing that was ignored.",
                data={"env_allowlist": []},
                fix_hint=(
                    "name at least one tool, unset OKG_MCP_TOOL_ALLOWLIST, "
                    f"or use --tools with one of {list(profile_names())}"
                ),
            )
        return ResolvedToolSurface(
            profile=None,
            source="env_allowlist",
            tools=frozenset(env_allowlist),
        )

    if config is not None and config.default_tool_profile is not None:
        return _resolve_named(
            config.default_tool_profile, config=config,
            source="manifest_default",
        )

    if agent_surface_served is not None:
        served = frozenset(str(t) for t in agent_surface_served)
        return ResolvedToolSurface(
            profile=AGENT_SURFACE_PROFILE,
            source="agent_surface",
            tools=served,
        )

    if manifest_unresolved:
        # `config is None` here means one of two very different things, and
        # only the caller knows which: the manifest declared no `mcp:` block,
        # or the manifest could not be read at all. Reading the second as the
        # first is absence dressed as agreement, so it is flagged.
        return ResolvedToolSurface(
            source="unnarrowed_manifest_unresolved",
            unhonored_manifest_default=True,
        )

    return UNNARROWED


def _resolve_named(
    name: str, *, config: ToolProfileConfig | None, source: str,
) -> ResolvedToolSurface:
    if name == RESERVED_FULL:
        return ResolvedToolSurface(
            profile=RESERVED_FULL, source=source, tools=None,
        )
    builtin = BUILTIN_TOOL_PROFILES.get(name)
    if builtin is not None:
        return ResolvedToolSurface(
            profile=name, source=source, tools=frozenset(builtin),
        )
    declared = config.profiles if config else {}
    declared_names = config.declared_names if config else ()
    profile = declared.get(name)
    if profile is None:
        known = sorted({*BUILTIN_TOOL_PROFILES, RESERVED_FULL, *declared_names})
        raise UnknownToolProfile(
            f"unknown MCP tool profile {name!r}; known profiles are "
            f"{known}. The reserved {RESERVED_FULL!r} always means the "
            f"unnarrowed surface.",
            data={"requested": name, "known": known,
                  "builtin": sorted(BUILTIN_TOOL_PROFILES),
                  "declared": list(declared_names)},
            fix_hint=(
                f"pass --tools with one of {known}, or declare the "
                f"profile in the deployment's `mcp.tool_profiles` block"
            ),
        )
    return ResolvedToolSurface(
        profile=profile.name, source=source, tools=frozenset(profile.tools),
    )


def assert_narrowing_is_servable(
    requested: Iterable[str],
    *,
    registered: Iterable[str],
    profile: str | None = None,
    source: str = "env_allowlist",
) -> None:
    """Refuse a narrowing that names a tool this server does not serve.

    Called AFTER boundary pruning, against what is actually registered, so
    "served" means served rather than intended. Before this existed the
    narrowing was a plain set difference: an unregistered name contributed
    nothing and the server started happily on a smaller surface than the
    operator asked for, which is absence dressed as agreement.
    """
    requested_set = {str(t) for t in requested}
    registered_set = {str(t) for t in registered}
    missing = requested_set - registered_set
    # `run` (#3666) is in the read surface (the `reader` profile, and any
    # list built from OPERATOR_TOOLS), and a deployment whose declared
    # `served` does not name it serves that surface without it, as that
    # declaration says.
    missing -= set(COMPOSITION_TOOLS)
    if not missing:
        return

    withheld = sorted(missing & set(ALL_ADMITTED_TOOLS))
    unrecognized = sorted(missing - set(ALL_ADMITTED_TOOLS))
    label = (
        f"MCP tool profile {profile!r}" if profile
        else f"MCP tool narrowing ({source})"
    )
    parts: list[str] = []
    if unrecognized:
        parts.append(
            f"{unrecognized} are not OKG MCP tools at all (known tools: "
            f"{list(ALL_ADMITTED_TOOLS)})"
        )
    if withheld:
        parts.append(
            f"{withheld} are real tools this launch does not serve — the "
            f"write/admin tools require --allow-writes, and a conditional "
            f"tool such as `upload` also requires the deployment to "
            f"provision it"
        )
    raise ToolProfileToolAbsent(
        f"{label} names tool(s) this server does not serve: "
        f"{'; '.join(parts)}. Refusing rather than narrowing silently: a "
        f"dropped name would leave the surface smaller than asked for with "
        f"no signal. Served tools are {sorted(registered_set)}.",
        data={"profile": profile, "source": source,
              "requested": sorted(requested_set),
              "served": sorted(registered_set),
              "unrecognized": unrecognized,
              "withheld_by_launch": withheld},
        fix_hint=(
            "fix the spelling, drop the name, or relaunch with "
            "--allow-writes if the tool is a write/admin tool"
        ),
    )


# ---------- instructions that follow the served set ----------
#
# The server's instructions are not a constant; they are a rendering of what
# it serves. A segment declares the tools its text NAMES, and is omitted when
# any of them is absent, so the served text can never tell a model to call a
# tool that was pruned.


@dataclass(frozen=True)
class InstructionSegment:
    """One unit of instruction text, attributable to the tools it names.

    `requires` is an ALL-of set: the text is served only when every tool it
    names is served. `{tools}` in `text` is replaced by the served tool list.
    """

    requires: frozenset[str]
    text: str


@dataclass(frozen=True)
class InstructionSlot:
    """One position in the instructions, with alternative phrasings.

    Candidates are tried in order and the FIRST whose requirements are all met
    renders; if none match the slot renders nothing. A candidate list is how a
    sentence gets an OR ("search or filter is present"), while a segment's
    `requires` set is the AND. Between them a sentence can degrade to a
    narrower true statement instead of being dropped whole.
    """

    candidates: tuple[InstructionSegment, ...]


#: Presentation order for tool lists: the AKMON read algebra in its declared
#: order (inspect, search, expand, filter, map, aggregate, query), then the
#: rest alphabetically. `ALL_ADMITTED_TOOLS` is sorted for set comparison and
#: is the wrong order to read a surface in.
CANONICAL_TOOL_ORDER: tuple[str, ...] = tuple(OPERATOR_TOOLS) + tuple(
    name for name in ALL_ADMITTED_TOOLS if name not in set(OPERATOR_TOOLS)
)


def render_tool_list(served: Iterable[str]) -> str:
    """The served tools, in canonical surface order, as English."""
    order = {name: i for i, name in enumerate(CANONICAL_TOOL_ORDER)}
    names = sorted({str(t) for t in served}, key=lambda t: order.get(t, 10**6))
    if not names:
        return "none"
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def render_instructions(
    paragraphs: Iterable[Iterable[InstructionSlot]],
    *,
    served: Iterable[str],
) -> str:
    """Render instruction paragraphs for exactly the served tool set."""
    served_set = {str(t) for t in served}
    tool_list = render_tool_list(served_set)
    rendered: list[str] = []
    for slots in paragraphs:
        chunks: list[str] = []
        for slot in slots:
            for candidate in slot.candidates:
                if candidate.requires <= served_set:
                    chunks.append(candidate.text.replace("{tools}", tool_list))
                    break
        text = "".join(chunks).rstrip()
        if text:
            rendered.append(text)
    return "\n\n".join(rendered)


def tools_named_in(text: str, *, candidates: Iterable[str] | None = None) -> tuple[str, ...]:
    """Tool names that appear as whole words in `text`.

    A substring scan, and honestly so: it is used to WARN that a
    deployment-authored instruction file names a pruned tool, not to refuse
    one. Word-boundary matched so `search` in `search_profiles` still counts
    but `mapping` does not report `map`.
    """
    import re

    pool = tuple(candidates) if candidates is not None else ALL_ADMITTED_TOOLS
    found = [
        name for name in pool
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", text)
    ]
    return tuple(sorted(set(found)))


# ---------- in-session expansion (issue #1458) ----------
#
# A profile decides what a launch REGISTERS. A ceiling decides how far one
# SESSION may widen within that registration without a restart. The two are
# separate on purpose: the ceiling is still an operator decision made at
# launch, so nothing a caller says can push a session past a surface the
# operator did not choose to serve.


#: The tool a session calls to widen itself. It has to be in the base
#: surface or the widening is unreachable, and an unreachable mechanism
#: advertised as available is worse than no mechanism at all.
EXPANSION_DOOR_TOOL = "inspect"


class ToolSurfaceExpansionInvalid(SubstrateError):
    """An expansion ceiling that cannot mean what it says.

    Every arm is a launch-time refusal rather than a degraded start: a
    ceiling that is not wider, or one a session could never reach, would
    leave the operator believing sessions can widen when they cannot.
    """

    code = "tool_surface_expansion_invalid"


class ToolSurfaceExpansionAllowlistConflict(ToolSurfaceExpansionInvalid):
    """A ceiling was requested on top of an OKG_MCP_TOOL_ALLOWLIST launch.

    A subclass so anything already catching `ToolSurfaceExpansionInvalid`
    still catches it, and a distinct code because the operator's next step is
    different: this is not an incoherent ceiling, it is two selectors that
    disagree about what the server may ever serve.

    `OKG_MCP_TOOL_ALLOWLIST` is a hard boundary an operator names tool by
    tool, and the registered set is the CEILING, not the base. Accepting the
    combination therefore registered the ceiling profile and dropped the
    allowlist entirely: `OKG_MCP_TOOL_ALLOWLIST=inspect --tools-ceiling
    reader` served all seven read tools, and the `_okg_tool_allowlist`
    readback reported the ceiling rather than what the operator set. Same
    reading as `ToolSurfaceSelectionConflict`: neither interpretation of the
    launch would be true, so neither is served.
    """

    code = "tool_surface_expansion_allowlist_conflict"


def resolve_expansion_ceiling(
    requested_ceiling: str | None,
    *,
    base: ResolvedToolSurface,
    config: ToolProfileConfig | None = None,
) -> ResolvedToolSurface | None:
    """Resolve the widest profile a session may reach, or None when unset.

    Refuses a ceiling on an UNNARROWED launch: with nothing withheld there
    is nothing to expand into, and accepting the flag anyway would advertise
    `tools.listChanged` on a server whose tool list can never change.

    Refuses a ceiling over an `OKG_MCP_TOOL_ALLOWLIST` base for the opposite
    reason: that base IS narrowed, but it is an explicit per-tool boundary
    the ceiling would silently widen past.
    """

    if requested_ceiling is None:
        return None
    if base.source == "env_allowlist":
        raise ToolSurfaceExpansionAllowlistConflict(
            f"this launch names both `--tools-ceiling "
            f"{requested_ceiling}` and OKG_MCP_TOOL_ALLOWLIST="
            f"{','.join(sorted(base.tools or ()))}. Refusing rather than "
            "combining them: the ceiling is what the server REGISTERS, so "
            "accepting it would serve every tool in "
            f"{requested_ceiling!r} and discard the allowlist the operator "
            "named tool by tool.",
            data={"requested_ceiling": requested_ceiling,
                  "base_profile": base.profile,
                  "base_source": base.source,
                  "env_allowlist": sorted(base.tools or ())},
            fix_hint=(
                "unset OKG_MCP_TOOL_ALLOWLIST and narrow with --tools "
                "instead, or drop --tools-ceiling"
            ),
        )
    if not base.is_narrowed:
        raise ToolSurfaceExpansionInvalid(
            f"an in-session tool-surface ceiling ({requested_ceiling!r}) was "
            "requested, but this launch narrows nothing, so every session "
            "already starts at the full admitted surface and no expansion is "
            "possible. Refusing rather than serving a server that advertises "
            "a tool list it can never change.",
            data={"requested_ceiling": requested_ceiling,
                  "base_profile": base.profile,
                  "base_source": base.source},
            fix_hint=(
                "narrow the launch first (--tools, a deployment "
                "mcp.default_tool_profile, or OKG_MCP_TOOL_ALLOWLIST), or "
                "drop the ceiling"
            ),
        )
    return _resolve_named(
        requested_ceiling, config=config, source="cli_ceiling",
    )


def assert_expansion_is_coherent(
    *,
    base_tools: Iterable[str],
    ceiling_tools: Iterable[str],
    base_profile: str | None,
    ceiling_profile: str | None,
) -> None:
    """Refuse a ceiling that is not reachable, or not actually wider.

    Both arms are computed against the tools the launch ADMITS, not against
    the profiles as written: `--allow-writes` and deployment provisioning
    can make a nominally wider profile resolve to the same served set, and a
    ceiling equal to the base is a no-op dressed as a capability.
    """

    base_set = {str(t) for t in base_tools}
    ceiling_set = {str(t) for t in ceiling_tools}
    outside = sorted(base_set - ceiling_set)
    if outside:
        raise ToolSurfaceExpansionInvalid(
            f"the in-session ceiling {ceiling_profile!r} does not contain "
            f"{outside}, which this launch already serves. A ceiling may only "
            "WIDEN a session; narrowing one mid-session would take away tools "
            "a caller has already been told it has.",
            data={"base_profile": base_profile, "ceiling_profile": ceiling_profile,
                  "base_tools": sorted(base_set), "ceiling_tools": sorted(ceiling_set),
                  "outside_ceiling": outside},
            fix_hint="choose a ceiling profile that contains every base tool",
        )
    if base_set == ceiling_set:
        raise ToolSurfaceExpansionInvalid(
            f"the in-session ceiling {ceiling_profile!r} serves exactly the "
            f"same tools as the base surface ({sorted(base_set)}), so no "
            "session could ever widen. Refusing rather than advertising an "
            "expansion that does nothing.",
            data={"base_profile": base_profile, "ceiling_profile": ceiling_profile,
                  "tools": sorted(base_set)},
            fix_hint=(
                "choose a wider ceiling, or drop the ceiling and serve the "
                "base profile alone"
            ),
        )
    if EXPANSION_DOOR_TOOL not in base_set:
        raise ToolSurfaceExpansionInvalid(
            f"the base tool surface {sorted(base_set)} does not serve "
            f"{EXPANSION_DOOR_TOOL!r}, which is the tool a session calls to "
            "widen itself, so the ceiling could never be reached. Refusing "
            "rather than advertising an unreachable expansion.",
            data={"base_profile": base_profile, "ceiling_profile": ceiling_profile,
                  "base_tools": sorted(base_set),
                  "door_tool": EXPANSION_DOOR_TOOL},
            fix_hint=(
                f"include {EXPANSION_DOOR_TOOL!r} in the base profile, or "
                "drop the ceiling"
            ),
        )


#: One sentence appended to rendered instructions when a session can widen.
#: Rendered from the SAME sets the projection uses, for the same reason the
#: rest of this module exists: instructions that describe a different surface
#: than the one served are the defect.
def render_expansion_instruction(
    *, base_tools: Iterable[str], ceiling_tools: Iterable[str],
) -> str:
    added = sorted(set(ceiling_tools) - set(base_tools))
    return (
        "This session starts on a reduced tool surface. Calling "
        "inspect(target={\"kind\": \"surface\", \"action\": \"expand\"}) adds "
        f"{render_tool_list(added)} to THIS session only, without a restart, "
        "and the server then sends notifications/tools/list_changed; re-list "
        "tools after it to see the wider set."
    )
