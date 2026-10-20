"""Operator-surface metadata for the substrate MCP server.

The default AKMON MCP surface is intentionally small: ``inspect``,
``search``, ``expand``, ``filter``, ``map``, ``aggregate``, and the
bounded ``query`` escape hatch. ``record`` and ``propose`` are write
effects. Source setup, connector ingest, and deployment administration
are source/admin tools. They may be exposed by an explicit
operator-supervised write/admin launch path, but they are not part of
the default graph-read algebra.

``CURRENT_TOOL_MIGRATIONS`` is the historical record of the 22
legacy "compatibility alias" tools that used to be registered behind
a ``tool_surface`` filter and were deleted by
``shrink-mcp-to-operator-surface``. It is no longer load-bearing for
runtime dispatch; it is retained so ``classify_tool`` can still
label old trace events that mention a legacy name, and so the
regression test for the canonical surface can iterate over the
known-removed names.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError


AKMON_READ_TOOLS: tuple[str, ...] = (
    "inspect",
    "search",
    "expand",
    "filter",
    "map",
    "aggregate",
)
QUERY_ESCAPE_TOOLS: tuple[str, ...] = ("query",)
WRITE_EFFECT_TOOLS: tuple[str, ...] = (
    "record",
    "propose",
)
SOURCE_ADMIN_TOOLS: tuple[str, ...] = (
    "list_sources",
    "add_source",
    "remove_source",
    "source_status",
    "discover_sources",
    "propose_setup",
    "commit_setup",
    "connector_route_status",
    "connector_ingest_batch",
)

# Admin tools whose presence depends on deployment-side provisioning, not
# only on the write/admin launch flag. `upload` exists exactly where the
# deployment registers a Nomos-classified upload source (PACT
# chat-upload-source): elsewhere the tool is absent from the surface
# rather than present and refusing, because "no registration" must not
# look like a temporarily unavailable capability. These are deliberately
# NOT in WRITE_ADMIN_TOOLS: that tuple is the set every write/admin
# launch registers, and a conditional tool would make it a lie.
CONDITIONAL_ADMIN_TOOLS: tuple[str, ...] = ("upload",)

# Backwards-compatible name for the default AKMON read surface.
OPERATOR_TOOLS: tuple[str, ...] = AKMON_READ_TOOLS + QUERY_ESCAPE_TOOLS

#: The tool set a chat assistant gets when its deployment declares NONE.
#:
#: Written out as a LITERAL here, in ONE place, on purpose (PACT
#: chat-tool-surface, req.curation-reaches-the-wire). "Whatever the MCP
#: server happens to register" is not a default, it is an accident, and an
#: unstated default is default-value-as-fallback. The six read operators;
#: ``query`` is EXCLUDED by default because it is the bounded SQL escape
#: hatch whose output ``mcp-http-auth`` measured as uncovered by Nomos
#: masking. A deployment that wants it must name it.
#:
#: Not spelled ``AKMON_READ_TOOLS`` even though the members coincide today:
#: that tuple states what the substrate SERVES, this one states what a chat
#: assistant is GIVEN. Binding them by aliasing would make a future
#: read-operator addition silently widen every chat surface in the fleet.
CHAT_DEFAULT_TOOLS: tuple[str, ...] = (
    "inspect",
    "search",
    "expand",
    "filter",
    "map",
    "aggregate",
)
WRITE_ADMIN_TOOLS: tuple[str, ...] = WRITE_EFFECT_TOOLS + SOURCE_ADMIN_TOOLS

#: Every tool name the substrate MCP server can ever register, whatever the
#: launch flags. Distinct from "what THIS launch serves": a read-only launch
#: registers a strict subset. It exists so a narrowing that names ``record``
#: can be told apart from one that names ``recrod`` — the first is a real tool
#: this launch withholds, the second is a typo, and the operator fix differs.
#: Never use it as an admission set; admission is ``apply_admitted_tool_surface``.
ALL_ADMITTED_TOOLS: tuple[str, ...] = tuple(
    sorted(set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS) | set(CONDITIONAL_ADMIN_TOOLS))
)
ALL_BUILTIN_TOOLS: tuple[str, ...] = (
    OPERATOR_TOOLS + WRITE_ADMIN_TOOLS + CONDITIONAL_ADMIN_TOOLS
)


@dataclass(frozen=True)
class OperatorMetadata:
    name: str
    stability: str
    pin_behavior: str
    boundary: str
    admission_required: bool
    description: str
    layer: str


@dataclass(frozen=True)
class ToolMigration:
    tool_name: str
    target: str
    replacement_operator: str | None
    profile: str
    reason: str


OPERATOR_METADATA: dict[str, OperatorMetadata] = {
    "inspect": OperatorMetadata(
        name="inspect",
        stability="stable",
        pin_behavior="generation-pinned where target is graph-backed",
        boundary="okg_graph_read",
        admission_required=False,
        layer="core_read",
        description=(
            "Inspect graph, ontology, subtype, node, session, generation, "
            "admission, alias-suggestion, or tool-surface state."
        ),
    ),
    "search": OperatorMetadata(
        name="search",
        stability="stable",
        pin_behavior="generation-pinned",
        boundary="okg_graph_read",
        admission_required=False,
        layer="core_read",
        description=(
            "Produce candidate sets from lexical text, aliases, semantic "
            "similarity, identifier mentions, or scoped filters."
        ),
    ),
    "expand": OperatorMetadata(
        name="expand",
        stability="stable",
        pin_behavior="generation-pinned",
        boundary="okg_graph_read",
        admission_required=False,
        layer="core_read",
        description="Follow edges from a node or set.",
    ),
    "filter": OperatorMetadata(
        name="filter",
        stability="experimental",
        pin_behavior="generation-pinned",
        boundary="okg_graph_read",
        admission_required=False,
        layer="core_read",
        description="Apply a bounded predicate tree to a set expression.",
    ),
    "map": OperatorMetadata(
        name="map",
        stability="experimental",
        pin_behavior="generation-pinned",
        boundary="okg_graph_read",
        admission_required=False,
        layer="core_read",
        description="Project, rename, or shape fields from a set expression.",
    ),
    "aggregate": OperatorMetadata(
        name="aggregate",
        stability="stable",
        pin_behavior="generation-pinned",
        boundary="okg_graph_read",
        admission_required=False,
        layer="core_read",
        description="Group, count, or summarize a set expression.",
    ),
    "record": OperatorMetadata(
        name="record",
        stability="experimental",
        pin_behavior="writes are admission-envelope scoped",
        boundary="okg_admission_write",
        admission_required=True,
        layer="write_effect",
        description=(
            "Record an agent observation, hypothesis, verification, "
            "decision, experiment run, finding, evidence item, or "
            "artifact note through typed admission templates."
        ),
    ),
    "propose": OperatorMetadata(
        name="propose",
        stability="experimental",
        pin_behavior="writes are admission-envelope scoped",
        boundary="okg_admission_write",
        admission_required=True,
        layer="write_effect",
        description=(
            "Submit or decide lower-level raw node/edge proposals through "
            "DBOS-backed admission workflows."
        ),
    ),
    "query": OperatorMetadata(
        name="query",
        stability="stable",
        pin_behavior="generation-pinned",
        boundary="okg_view_sql_escape_hatch",
        admission_required=False,
        layer="query_escape",
        description="Bounded read-only SQL over okg.v_* views.",
    ),
    "list_sources": OperatorMetadata(
        name="list_sources",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Enumerate every source in the deployment's "
            "source_registry.yaml with status / last-ingest / "
            "node-count / edge-count. Used by the setup skill to "
            "inspect what's wired."
        ),
    ),
    "add_source": OperatorMetadata(
        name="add_source",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Low-level recovery hook for one source. Normal setup "
            "uses propose_setup/commit_setup; credentialed sources "
            "return needs_connection instead of per-source credential "
            "instructions."
        ),
    ),
    "remove_source": OperatorMetadata(
        name="remove_source",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Drop one source from the registry. Existing nodes remain "
            "in the graph."
        ),
    ),
    "source_status": OperatorMetadata(
        name="source_status",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Inspect one source's connector/auth state, last ingest, "
            "and node/edge counts."
        ),
    ),
    "discover_sources": OperatorMetadata(
        name="discover_sources",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Walk the operator's filesystem + env + install "
            "answers; return ranked candidates per source so "
            "the agent doesn't have to ask the operator for "
            "paths. Read-only."
        ),
    ),
    "propose_setup": OperatorMetadata(
        name="propose_setup",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Bundle discovered candidates into a single "
            "approval-shape plan with a plan_id. Agent reads "
            "the summary aloud, gets a one-word approval, then "
            "calls commit_setup."
        ),
    ),
    "commit_setup": OperatorMetadata(
        name="commit_setup",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Execute a previously-proposed setup plan: calls "
            "the low-level source add hook per approved local/public "
            "entry and aggregates results."
        ),
    ),
    "connector_route_status": OperatorMetadata(
        name="connector_route_status",
        stability="experimental",
        pin_behavior="not generation-pinned",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Report selected/available connector route readiness for "
            "a credentialed source."
        ),
    ),
    "connector_ingest_batch": OperatorMetadata(
        name="connector_ingest_batch",
        stability="experimental",
        pin_behavior="writes fact rows then publishes on live runs",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Import normalized records fetched by an agent host "
            "connector under an approved route and allowlist."
        ),
    ),
    "upload": OperatorMetadata(
        name="upload",
        stability="experimental",
        pin_behavior="stages source input; rows appear on the next publish",
        boundary="okg_source_admin",
        admission_required=False,
        layer="source_admin",
        description=(
            "Stage an operator file or URL fetch into the deployment's "
            "registered upload source. Present only where that source is "
            "registered and Nomos-classified."
        ),
    ),
}


CURRENT_TOOL_MIGRATIONS: dict[str, ToolMigration] = {
    "describe_graph": ToolMigration(
        "describe_graph", "inspect(target={kind:'graph'})", "inspect",
        "compat", "folded into graph inspection",
    ),
    "graph_stats": ToolMigration(
        "graph_stats", "inspect(target={kind:'graph'})",
        "inspect", "compat", "deprecated graph overview alias",
    ),
    "get_ontology": ToolMigration(
        "get_ontology", "inspect(target={kind:'ontology'})", "inspect",
        "compat", "folded into ontology inspection",
    ),
    "describe_subtype": ToolMigration(
        "describe_subtype",
        "inspect(target={kind:'subtype', subtype:<name>})",
        "inspect", "compat", "folded into subtype inspection",
    ),
    "get_node": ToolMigration(
        "get_node", "inspect(target={kind:'node', node_id:<id>})",
        "inspect", "compat", "folded into node inspection",
    ),
    "find_identifier_mentions": ToolMigration(
        "find_identifier_mentions",
        "search(method='identifier_mentions', query=<text>)",
        "search", "compat", "folded into search modes",
    ),
    "list_neighbors": ToolMigration(
        "list_neighbors", "expand(input={node_id:<id>}, depth=1)",
        "expand", "compat", "folded into edge expansion",
    ),
    "traverse_path": ToolMigration(
        "traverse_path", "expand(input={node_id:<id>}, path=<path>)",
        "expand", "compat", "folded into typed expansion",
    ),
    "aggregate_neighbors": ToolMigration(
        "aggregate_neighbors",
        "aggregate(input={node_id:<id>}, by=['edge_type','dst_subtype'], "
        "metrics=['count'])",
        "aggregate", "compat", "folded into aggregate",
    ),
    "aggregate_edge_routes": ToolMigration(
        "aggregate_edge_routes",
        "aggregate(input={kind:'edges'}, "
        "by=['src_subtype','edge_type','dst_subtype'], metrics=['count'])",
        "aggregate", "compat", "folded into aggregate",
    ),
    "run_sql": ToolMigration(
        "run_sql", "query(sql=<sql>)", "query", "compat",
        "renamed SQL escape hatch",
    ),
    "preview_advance": ToolMigration(
        "preview_advance",
        "inspect(target={kind:'generation_delta', from_generation:<id>, to:<target>})",
        "inspect", "compat", "folded into generation inspection",
    ),
    "advance_session": ToolMigration(
        "advance_session", "session resource control", None,
        "compat", "session control, not graph operator",
    ),
    "start_trace": ToolMigration(
        "start_trace", "instrumentation profile/resource", None,
        "maintenance", "trace bracketing, not graph operator",
    ),
    "end_trace": ToolMigration(
        "end_trace", "instrumentation profile/resource", None,
        "maintenance", "trace bracketing, not graph operator",
    ),
    "agent_context": ToolMigration(
        "agent_context", "operator-maintenance profile", None,
        "maintenance", "coding-agent utility, not graph operator",
    ),
    "parse_document": ToolMigration(
        "parse_document", "pipeline utility profile", None,
        "maintenance", "extraction utility, not graph operator",
    ),
    "chunk_document": ToolMigration(
        "chunk_document", "pipeline utility profile", None,
        "maintenance", "extraction utility, not graph operator",
    ),
    "extract_entities": ToolMigration(
        "extract_entities", "pipeline utility profile", None,
        "maintenance", "extraction utility, not graph operator",
    ),
    "suggest_aliases": ToolMigration(
        "suggest_aliases", "inspect(target={kind:'alias_suggestions'})",
        "inspect", "compat", "folded into alias-suggestion inspection",
    ),
    "register_alias": ToolMigration(
        "register_alias", "future propose(action='alias') or admin CLI",
        None, "compat", "write helper outside default authority gate",
    ),
}


# ---------------------------------------------------------------------------
# Deployment-owned tool descriptions (PACT chat-tool-surface,
# req.descriptions-are-deployment-owned)
# ---------------------------------------------------------------------------

class ToolDescriptionError(SubstrateError):
    """A ``tool_description_overrides`` declaration could not be projected.

    One code, because the operator action is always the same shape: read the
    named tool, the declared set and the projected set out of ``data`` and fix
    the declaration. An override that cannot be projected RAISES; it is never
    dropped, because a dropped override looks identical to a deployment that
    declared none.
    """

    code = "tool_description_override_invalid"


#: Per-tool ceiling on ``substrate description + separator + override``.
#: Inherited from the mechanism this replaces (the deleted
#: ``_TOOL_DESCRIPTION_BUDGET`` of the improve-agent-navigability work). Over
#: budget REFUSES; it never truncates, because a truncated instruction reads
#: as a complete one and the model cannot tell.
TOOL_DESCRIPTION_BUDGET = 2000

#: Ceiling on the whole projected guidance block. A chat assistant pays for
#: this text on EVERY turn, so unbounded growth is a real cost, not a style
#: concern.
TOOL_DESCRIPTION_TOTAL_BUDGET = 8000

#: What separates the substrate description from the deployment's snippet.
TOOL_DESCRIPTION_SEPARATOR = "\n\n"


@dataclass(frozen=True)
class ToolDescription:
    """One tool's effective description, and WHERE it came from.

    ``source`` is never omitted and never inferred by the reader: a tool with
    no declared override reports ``substrate_default`` explicitly, so "this
    deployment said nothing about search" is a stated fact rather than the
    absence of a row.
    """

    name: str
    text: str
    source: str
    substrate_chars: int
    override_chars: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "chars": len(self.text),
            "substrate_chars": self.substrate_chars,
            "override_chars": self.override_chars,
        }


SOURCE_DEPLOYMENT_OVERRIDE = "deployment_override"
SOURCE_SUBSTRATE_DEFAULT = "substrate_default"


def substrate_tool_description(name: str) -> str:
    """The substrate's own description for ``name``.

    Raises rather than returning ``""`` for an unknown tool: an empty string
    would compose into a description that looks written and says nothing.
    """
    metadata = OPERATOR_METADATA.get(name)
    if metadata is None:
        raise ToolDescriptionError(
            f"no substrate description exists for tool {name!r}; the known "
            f"tools are {sorted(OPERATOR_METADATA)}.",
            data={"reason": "unknown_tool", "tool": name},
            fix_hint="use one of the registered operator names",
        )
    return metadata.description


def load_tool_description_overrides(path: Path | None) -> dict[str, str]:
    """Read a deployment's ``tool_description_overrides`` YAML.

    ``None`` means the deployment declared nothing and yields ``{}`` — the
    honest empty case. A DECLARED but unreadable, non-mapping, or
    non-string-valued file RAISES: the operator said the file exists, so a
    silent ``{}`` there would be an absent override wearing a declared one's
    clothes.
    """
    if path is None:
        return {}
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ToolDescriptionError(
            f"the declared tool_description_overrides file {path} cannot be "
            f"read ({exc}). A declared override file that is missing is a "
            "broken declaration, not an empty contribution.",
            data={"reason": "overrides_unreadable", "path": str(path)},
            fix_hint="fix or remove the tool_description_overrides declaration",
        ) from exc

    from okg.substrate import fastyaml

    raw = fastyaml.safe_load(text)
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise ToolDescriptionError(
            f"tool_description_overrides at {path} must be a mapping of "
            f"tool name to snippet; observed {type(raw).__name__}.",
            data={"reason": "overrides_not_a_mapping", "path": str(path)},
            fix_hint="write `<tool_name>: <snippet text>` entries",
        )
    overrides: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, str) or not value.strip():
            raise ToolDescriptionError(
                f"tool_description_overrides at {path} gives tool {key!r} a "
                f"{type(value).__name__} snippet. Every snippet must be a "
                "non-empty string; an empty one would append a separator and "
                "nothing else.",
                data={
                    "reason": "override_not_a_string",
                    "path": str(path),
                    "tool": str(key),
                },
                fix_hint=f"write a non-empty snippet for {key!r}, or remove it",
            )
        overrides[str(key)] = value.strip()
    return overrides


def compose_tool_descriptions(
    projected: Sequence[str],
    overrides: Mapping[str, str],
) -> tuple[ToolDescription, ...]:
    """The effective description of every PROJECTED tool, with its source.

    Three rules, each of which exists because its opposite is a silent
    failure:

    * An override naming a tool OUTSIDE ``projected`` RAISES, naming both
      sets. Ignoring it would leave an operator believing they had rewritten
      guidance the model never sees — and the most likely cause is that the
      tool was curated away, which the operator needs told.
    * A projected tool with no override gets the substrate description and
      says so. Absence is a stated default here, never silence.
    * Over-budget RAISES for every offender at once, after the whole set has
      been composed, so one long snippet does not hide the next.
    """
    projected_names = tuple(projected)
    if not projected_names and overrides:
        raise ToolDescriptionError(
            f"tool_description_overrides names {sorted(overrides)} but NO "
            "tool is projected onto the chat surface, so no override can "
            "reach a model. An empty projected set admits nothing, it does "
            "not admit everything.",
            data={
                "reason": "override_for_unprojected_tool",
                "declared": sorted(overrides),
                "projected": [],
                "unprojected": sorted(overrides),
            },
            fix_hint="project the tool, or remove its override",
        )
    unprojected = sorted(set(overrides) - set(projected_names))
    if unprojected:
        raise ToolDescriptionError(
            f"tool_description_overrides names {unprojected}, which the chat "
            f"surface does not project. The projected set is "
            f"{sorted(projected_names)}. An override for a tool the model "
            "never sees is refused, never ignored.",
            data={
                "reason": "override_for_unprojected_tool",
                "declared": sorted(overrides),
                "projected": sorted(projected_names),
                "unprojected": unprojected,
            },
            fix_hint=(
                f"remove {unprojected} from tool_description_overrides, or "
                "add the tool to the deployment's declared chat tool set"
            ),
        )

    composed: list[ToolDescription] = []
    over_budget: list[dict[str, Any]] = []
    for name in projected_names:
        core = substrate_tool_description(name)
        snippet = overrides.get(name)
        if snippet is None:
            composed.append(
                ToolDescription(
                    name=name,
                    text=core,
                    source=SOURCE_SUBSTRATE_DEFAULT,
                    substrate_chars=len(core),
                    override_chars=0,
                )
            )
            continue
        text = core + TOOL_DESCRIPTION_SEPARATOR + snippet
        if len(text) > TOOL_DESCRIPTION_BUDGET:
            over_budget.append({
                "tool": name,
                "substrate_chars": len(core),
                "override_chars": len(snippet),
                "total_chars": len(text),
                "budget": TOOL_DESCRIPTION_BUDGET,
            })
        composed.append(
            ToolDescription(
                name=name,
                text=text,
                source=SOURCE_DEPLOYMENT_OVERRIDE,
                substrate_chars=len(core),
                override_chars=len(snippet),
            )
        )
    if over_budget:
        raise ToolDescriptionError(
            f"{len(over_budget)} tool description(s) exceed the "
            f"{TOOL_DESCRIPTION_BUDGET}-character per-tool budget: "
            + "; ".join(
                f"{row['tool']} (substrate={row['substrate_chars']} "
                f"override={row['override_chars']} total={row['total_chars']})"
                for row in over_budget
            )
            + ". The run refuses rather than truncating: a truncated "
            "instruction reads as a complete one.",
            data={"reason": "override_over_budget", "offenders": over_budget},
            fix_hint="shorten the offending snippets",
        )
    total = sum(len(d.text) for d in composed if d.source == SOURCE_DEPLOYMENT_OVERRIDE)
    if total > TOOL_DESCRIPTION_TOTAL_BUDGET:
        raise ToolDescriptionError(
            f"the projected description overrides total {total} characters, "
            f"over the {TOOL_DESCRIPTION_TOTAL_BUDGET}-character ceiling for "
            "the whole block. This text is paid for on every chat turn.",
            data={
                "reason": "override_total_over_budget",
                "total_chars": total,
                "budget": TOOL_DESCRIPTION_TOTAL_BUDGET,
            },
            fix_hint="shorten the snippets, or override fewer tools",
        )
    return tuple(composed)


# ---------------------------------------------------------------------------
# The write ANSWER contract (PACT chat-record-affordance,
# req.chat-record-affordance.honest-affordance)
# ---------------------------------------------------------------------------
#
# A write initiated from chat is HELD. It is a pending admission envelope, it
# is on no branch, and no reader can see it -- because no fact exists yet. The
# words a user reads have to say that, and must not say the four things that
# are FALSE at this point: that the finding was recorded, saved, added, or
# published.
#
# Rendered HERE rather than in the chat projection because there must be one
# authoring source: the same text is attached to the tool RESULT (so the model
# has something true to relay) and quoted into the write preset's system prompt
# (so the model is told to relay it). Two copies would drift, and the copy that
# drifted would be the one the user reads.

#: Claims that are false about a pending envelope. Asserted against the
#: rendered answer by the affordance suite, and listed here so a future edit
#: to the wording is checked against the same list the test uses.
FORBIDDEN_WRITE_ANSWER_CLAIMS: tuple[str, ...] = (
    "recorded",
    "saved",
    "added",
    "published",
)

#: The phrase a successful write's answer must carry.
PENDING_REVIEW_PHRASE = "pending review"

#: The phrase a refused write's answer must carry. Deliberately not a
#: substring of :data:`PENDING_REVIEW_PHRASE` and vice versa, so "the two
#: answers are distinguishable" is a property of the text rather than of the
#: assertion that checks it.
REFUSED_PHRASE = "not queued"

#: The phrase for a write whose OUTCOME is not yet known -- the admission
#: workflow was enqueued and has not returned. Neither of the other two words
#: is true of it: calling it pending review would name an envelope that may
#: not exist, and calling it refused would report a failure that did not
#: happen. A third outcome gets a third sentence rather than being rounded
#: into whichever neighbour is closer.
INDETERMINATE_PHRASE = "outcome is not yet known"

#: Result statuses that mean the write did NOT land.
REFUSED_STATUSES: frozenset[str] = frozenset({
    "refused",
    "failed",
    "error",
    "rejected",
    "writes_disabled",
})

#: Result statuses that mean the submit was enqueued and has not answered.
INDETERMINATE_STATUSES: frozenset[str] = frozenset({"queued", "pending_workflow"})


class WriteAnswerError(SubstrateError):
    """A write result could not be rendered into an honest answer."""

    code = "write_answer_unrenderable"


def render_pending_review_answer(
    result: Mapping[str, Any],
    *,
    deployment: str,
    review_location: str,
) -> str:
    """The answer text for a write that landed as a PENDING envelope.

    ``result`` is the admission submit result. The envelope identifier is read
    OUT of it -- never composed from the request -- and its absence is a
    refusal, not an empty slot: an answer that says "pending review" without
    naming the envelope leaves the reviewer nothing to act on, and inventing an
    identifier is the failure mode the requirement names outright.
    """
    admission_id = str(result.get("admission_id") or "").strip()
    if not admission_id:
        raise WriteAnswerError(
            "the admission submit result carried no admission_id, so there is "
            "no envelope identifier to name. An answer that says a finding is "
            "pending review without naming the envelope is unactionable, and "
            "one that invents an identifier is worse.",
            data={"reason": "missing_admission_id", "result_keys": sorted(result)},
            fix_hint="report this: admission_submit_workflow must return admission_id",
        )
    mode = str(result.get("admission_mode") or "").strip() or "review"
    status = str(result.get("status") or "").strip() or "pending"
    return (
        f"Held for review. Your finding is {PENDING_REVIEW_PHRASE} as "
        f"admission envelope {admission_id} on deployment {deployment} "
        f"(admission_mode {mode}, status {status}). It is not in the graph and "
        "no reader can see it, because no fact exists yet -- the facts are "
        "emitted only when an operator accepts the envelope. Review it in "
        f"{review_location}."
    )


def render_write_refusal_answer(
    result: Mapping[str, Any],
    *,
    deployment: str,
) -> str:
    """The answer text for a write that was REFUSED.

    Distinguishable from the pending answer by construction: it carries
    :data:`REFUSED_PHRASE`, never :data:`PENDING_REVIEW_PHRASE`, and never an
    envelope identifier -- a refused write created none, so any identifier in
    this text would be invented.
    """
    error = result.get("error")
    error_map: Mapping[str, Any] = error if isinstance(error, Mapping) else {}
    code = (
        str(error_map.get("code") or "").strip()
        or str(result.get("status") or "").strip()
        or "refused"
    )
    message = (
        str(error_map.get("message") or "").strip()
        or str(result.get("summary") or "").strip()
    )
    reason = f"{code}: {message}" if message else code
    return (
        f"Refused. The write was {REFUSED_PHRASE} and no admission envelope "
        f"exists on deployment {deployment}. Reason -- {reason}"
    )


def render_write_indeterminate_answer(
    result: Mapping[str, Any],
    *,
    deployment: str,
) -> str:
    """The answer for a submit that was enqueued and has not answered yet."""
    workflow_id = str(result.get("workflow_id") or "").strip() or "unknown"
    return (
        f"Submitted, but the {INDETERMINATE_PHRASE}. The admission workflow "
        f"{workflow_id} on deployment {deployment} has not returned, so there "
        "is no envelope identifier to give you and no refusal to report. Ask "
        "again, or check with `okg admission list --deployment "
        f"{deployment} --status pending`."
    )


def render_write_answer(
    result: Mapping[str, Any],
    *,
    deployment: str,
    review_location: str,
) -> str:
    """Render one write result into the answer a user should read.

    Three outcomes, three sentences. The branch is taken on the RESULT, never
    on the request: a refusal can never render as a queued write, and an
    enqueued-but-unanswered submit renders as neither -- it gets its own words,
    because rounding it to "pending review" would name an envelope that may not
    exist and rounding it to "refused" would report a failure that did not
    happen.
    """
    status = str(result.get("status") or "").strip().lower()
    if status in REFUSED_STATUSES:
        return render_write_refusal_answer(result, deployment=deployment)
    if status in INDETERMINATE_STATUSES:
        return render_write_indeterminate_answer(result, deployment=deployment)
    error = result.get("error")
    if isinstance(error, Mapping) and error:
        return render_write_refusal_answer(result, deployment=deployment)
    return render_pending_review_answer(
        result, deployment=deployment, review_location=review_location,
    )


def review_location_for(deployment: str) -> str:
    """Where a pending envelope is reviewed, named in the answer.

    One string, so the answer and the preset instruction cannot disagree about
    where the user should look.
    """
    return (
        "the review inbox for this deployment (`okg admission list "
        f"--deployment {deployment} --status pending`, then `okg admission "
        f"decide <id> --deployment {deployment} --decision accept`)"
    )


#: What the write preset tells the model to do with that answer. Carried into
#: the preset's ``params.system`` by ``okg chat sync``; the model never sees the
#: MCP server's own ``instructions`` (Open WebUI drops them), so the preset is
#: the only door.
WRITE_ANSWER_CONTRACT_INSTRUCTION = (
    "Writing to the graph: `record` and `propose` do NOT write to the graph. "
    "They create a PENDING admission envelope that an operator must accept "
    "before any fact exists or any reader can see it. When you call either "
    "tool, relay the result's `answer` field to the user VERBATIM and add no "
    "claim of your own. Never tell the user their finding was recorded, "
    "saved, added, or published: none of that is true yet. If the tool "
    "refuses, say it was refused and give the reason; do not present a "
    "refused write as a queued one, and never invent an envelope identifier."
)


def classify_tool(tool_name: str) -> tuple[str | None, str | None]:
    """Return ``(operator_name, replacement_operator)`` for logging.

    For known built-in MCP tools, returns ``(tool_name, None)`` even when the
    tool is outside the default AKMON read surface. For a known legacy tool name
    (still listed in ``CURRENT_TOOL_MIGRATIONS`` as a historical reference even
    though it is no longer registered), returns ``(canonical_replacement,
    canonical_replacement)`` so tracing can label old trace events. Unknown
    names yield ``(None, None)``.
    """
    if tool_name in ALL_BUILTIN_TOOLS:
        return tool_name, None
    migration = CURRENT_TOOL_MIGRATIONS.get(tool_name)
    if migration is not None:
        return migration.replacement_operator, migration.replacement_operator
    return None, None


def operator_metadata_rows() -> list[dict[str, Any]]:
    return [asdict(OPERATOR_METADATA[name]) for name in ALL_BUILTIN_TOOLS]


def migration_rows() -> list[dict[str, Any]]:
    """Historical migration map for the 22 removed legacy tools."""
    return [
        asdict(CURRENT_TOOL_MIGRATIONS[name])
        for name in sorted(CURRENT_TOOL_MIGRATIONS)
    ]
