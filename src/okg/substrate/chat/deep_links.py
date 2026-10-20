"""Map an OKG identifier to an operator-console URL.

A chat answer names graph objects by id, and those ids are dead text until
something maps them to the console route that shows the object. This module
is that map, and it is the ONLY one in the program: the answer-rendering
outlet, the review-queue pipe, and the source-admin surface all call in
here rather than assembling a URL string of their own.

Grammar authority
-----------------
The emitted hashes are the console's real grammar, and no view this module
routes to shows any of these objects. PACT operator-ui-remove-four-views
deleted the Graph, Trace, Performance and Deployment views, and PACT
console-default-remove-old-views replaced the Home and Health pages with the
console (``#console/<view>``). No kind below is routed to a console view yet,
so every declared kind renders as PLAIN TEXT with a stated reason:

===================  ==========================================================
kind                 rendered as
===================  ==========================================================
``node``             plain text — the Graph view that showed a record is gone
``agent_session``    plain text — the Trace view that showed it is gone
``producer_run``     plain text — the Trace view that showed it is gone
``mcp_trace``        plain text — the Trace view that showed it is gone
===================  ==========================================================

The kinds stay in the vocabulary because the envelope's declaration is still
meaningful: the answer names what the identifier IS, and states why it is not
a link. Emitting a hash for a deleted view would fail SILENTLY — the app
opens the console's Home for any hash that is not a console hash, so a reader
would land on the wrong page with no error anywhere and conclude the answer
was wrong.

When a view that shows one of these objects lands, give it a row in the table
above and a branch in ``_console_hash``, and route it from here — never from a
URL assembled at a call site.

Base URL
--------
A URL, when a kind routes again, is built from
``chat.deep_links.operator_ui_base``, DEFINED as the absolute base URL at
which the console's index is served, INCLUDING any path prefix. The emitted
URL is ``{base}#{hash}`` with no invented path segment — nothing in this repo
serves ``/console`` (the app mounts at ``/`` under the vite dev server and at
``/operator/`` in the one production rewrite), so a hardcoded segment would
make every link dead while every test still passed.

This module reads no configuration of its own. The base arrives as an
argument, its schema and validation belong to ``chat-manifest-block``, and
there is deliberately no default host, no same-origin guess, and no
environment-variable fallback: default-value-as-fallback is a P0 defect in
this repo, and a plausible-looking dead link is worse than plain text.

While no kind routes, the base is never reached: the kind is refused first,
so an answer says "no view shows this" rather than "this deployment declares
no console base". The base check stays in place, unweakened, for the kinds
that will route again.

Refusals
--------
Each refusal renders PLAIN TEXT rather than a guess: an id whose kind is not
envelope-declared or is unrecognized, a kind no remaining console view shows,
an absent or empty ``operator_ui_base``, and (as a distinguishable outcome,
not an error) a render that produced no links at all. ``RenderOutcome``
tells a caller "there were no ids to link" apart from "there were ids and
none of them could be linked" — which, until a view returns, is now the
ordinary outcome whenever an answer cites anything.

Kind source
-----------
For node-shaped MCP rows the kind is read from the authoritative ``subtype``
field through the closed ``SUBTYPE_TO_TARGET_KIND`` table below. The table is
deliberately a whitelist: an unknown subtype, a malformed row, or a subtype
marked ``None`` is plain text, never a nearest-match route. Non-node callers
may continue to provide the legacy ``target_kind`` envelope field for the
session/run/trace shapes already in this module. No kind is ever inferred from
an identifier's string shape.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import quote

__all__ = [
    "ENVELOPE_KIND_FIELD",
    "ENVELOPE_GENERATION_FIELD",
    "EMPTY_ID_TEXT",
    "ID_FIELD_BY_KIND",
    "SUBTYPE_TO_TARGET_KIND",
    "PRODUCER_KEY_FIELD",
    "TargetKind",
    "Refusal",
    "RenderOutcome",
    "LinkTarget",
    "RenderedLink",
    "RenderedLinks",
    "encode_id",
    "target_kind_from_envelope",
    "link_target_from_row",
    "deep_link",
    "render_links",
]


#: The MCP envelope field that DECLARES what an id is. Never inferred from
#: the id's string shape.
ENVELOPE_KIND_FIELD = "target_kind"

#: The canonical envelope field carrying the generation an answer is pinned
#: to (``okg.substrate.mcp.envelope``).
ENVELOPE_GENERATION_FIELD = "pinned_generation_id"


class TargetKind(str, Enum):
    """The four id kinds the console's hash grammar can route.

    The values are the envelope vocabulary, not display labels.
    """

    NODE = "node"
    AGENT_SESSION = "agent_session"
    PRODUCER_RUN = "producer_run"
    MCP_TRACE = "mcp_trace"


#: Kinds the envelope still declares that no console view shows. They render
#: as plain text with a stated reason rather than as a link. Today that is
#: EVERY kind: the Graph view showed records and the Trace view showed
#: sessions, producer runs and MCP traces, and PACT
#: operator-ui-remove-four-views deleted both. Remove a kind from this set in
#: the same change that gives it a view, so the two can never disagree.
_KINDS_WITHOUT_A_VIEW: frozenset[TargetKind] = frozenset({
    TargetKind.NODE,
    TargetKind.AGENT_SESSION,
    TargetKind.PRODUCER_RUN,
    TargetKind.MCP_TRACE,
})

#: Which row field carries the id, per kind. A row is only read through the
#: kind its envelope declared, so this table is never used to guess.
ID_FIELD_BY_KIND: dict[TargetKind, str] = {
    TargetKind.NODE: "node_id",
    TargetKind.AGENT_SESSION: "session_id",
    TargetKind.PRODUCER_RUN: "run_key",
    TargetKind.MCP_TRACE: "trace_id",
}

# Closed, deployment-independent allow-list for node rows that the chat
# contract is willing to link. The MCP node envelope guarantees ``subtype``;
# it does not guarantee a ``target_kind`` field. Every entry is explicit and
# there is intentionally no default-to-node branch: a new or deployment-local
# subtype must be reviewed before it can create a URL. ``None`` is an explicit
# refusal for an internal chat record that is not a useful console destination.
SUBTYPE_TO_TARGET_KIND: dict[str, TargetKind | None] = {
    "acceptance_criterion": TargetKind.NODE,
    "acceptance_scenario": TargetKind.NODE,
    "agent_claim": TargetKind.NODE,
    "agent_decision": TargetKind.NODE,
    "agent_experiment": TargetKind.NODE,
    "agent_observation": TargetKind.NODE,
    "agent_session": TargetKind.NODE,
    "agent": TargetKind.NODE,
    "agent_assignment": TargetKind.NODE,
    "agent_chapter": TargetKind.NODE,
    "agent_message": TargetKind.NODE,
    "alternative": TargetKind.NODE,
    "arena_preference": TargetKind.NODE,
    "benchmark_measurement": TargetKind.NODE,
    "blocker_event": TargetKind.NODE,
    "branch_archive_summary": TargetKind.NODE,
    "branch_disposition": TargetKind.NODE,
    "chat_context_record": TargetKind.NODE,
    "chat_message": None,
    "chat_rating": TargetKind.NODE,
    "chat_session": TargetKind.NODE,
    "citation": TargetKind.NODE,
    "code_import": TargetKind.NODE,
    "code_quality_finding": TargetKind.NODE,
    "code_reference": TargetKind.NODE,
    "code_symbol": TargetKind.NODE,
    "cooper_alias": TargetKind.NODE,
    "consistency_claim": TargetKind.NODE,
    "conference": TargetKind.NODE,
    "conference_edition": TargetKind.NODE,
    "config_key": TargetKind.NODE,
    "coordination_event": TargetKind.NODE,
    "coordination_lane": TargetKind.NODE,
    "coordination_org_unit": TargetKind.NODE,
    "coordination_work_area": TargetKind.NODE,
    "coordination_work_item": TargetKind.NODE,
    "data_asset": TargetKind.NODE,
    "data_flow_observation": TargetKind.NODE,
    "data_flow_step": TargetKind.NODE,
    "dependency_package": TargetKind.NODE,
    "dependency_vulnerability": TargetKind.NODE,
    "design_decision": TargetKind.NODE,
    "document": TargetKind.NODE,
    "document_chunk": TargetKind.NODE,
    "entity_mention": TargetKind.NODE,
    "gate_result": TargetKind.NODE,
    "git_branch": TargetKind.NODE,
    "git_commit": TargetKind.NODE,
    "git_author": TargetKind.NODE,
    "git_tag": TargetKind.NODE,
    "goal": TargetKind.NODE,
    "heartbeat_event": TargetKind.NODE,
    "hypothesis": TargetKind.NODE,
    "insight": TargetKind.NODE,
    "instrumentation_gap": TargetKind.NODE,
    "interface_endpoint": TargetKind.NODE,
    "issue": TargetKind.NODE,
    "machine": TargetKind.NODE,
    "merge_gate": TargetKind.NODE,
    "non_goal": TargetKind.NODE,
    "observation": TargetKind.NODE,
    "open_spec_change": TargetKind.NODE,
    "operator_report": TargetKind.NODE,
    "package_manifest": TargetKind.NODE,
    "pact_change": TargetKind.NODE,
    "pact_decision": TargetKind.NODE,
    "pact_task": TargetKind.NODE,
    "paper": TargetKind.NODE,
    "paper_section": TargetKind.NODE,
    "performance_budget": TargetKind.NODE,
    "permission_event": TargetKind.NODE,
    "person": TargetKind.NODE,
    "profile_hotspot": TargetKind.NODE,
    "profile_query": TargetKind.NODE,
    "profile_run": TargetKind.NODE,
    "profile_stage": TargetKind.NODE,
    "proof_event": TargetKind.NODE,
    "proof_of_work": TargetKind.NODE,
    "proposal_task": TargetKind.NODE,
    "pull_request": TargetKind.NODE,
    "remote_host": TargetKind.NODE,
    "repo": TargetKind.NODE,
    "region": TargetKind.NODE,
    "review": TargetKind.NODE,
    "review_finding": TargetKind.NODE,
    "review_gate": TargetKind.NODE,
    "risk": TargetKind.NODE,
    "role_agent": TargetKind.NODE,
    "runtime": TargetKind.NODE,
    "runtime_event": TargetKind.NODE,
    "security_finding": TargetKind.NODE,
    "self_play_finding": TargetKind.NODE,
    "self_play_run": TargetKind.NODE,
    "self_play_scenario": TargetKind.NODE,
    "source_file": TargetKind.NODE,
    "secret_finding": TargetKind.NODE,
    "skill": TargetKind.NODE,
    "spawned_task_chip": TargetKind.NODE,
    "spec_capability": TargetKind.NODE,
    "spec_delta": TargetKind.NODE,
    "stack_frame": TargetKind.NODE,
    "stakeholder": TargetKind.NODE,
    "sub_agent_session": TargetKind.NODE,
    "subagent_rollup": TargetKind.NODE,
    "supervisor_decision": TargetKind.NODE,
    "task_claim": TargetKind.NODE,
    "task_group": TargetKind.NODE,
    "test_case": TargetKind.NODE,
    "test_report": TargetKind.NODE,
    "test_run": TargetKind.NODE,
    "tool_use_event": TargetKind.NODE,
    "venue": TargetKind.NODE,
    "verification_result": TargetKind.NODE,
    "verification_step": TargetKind.NODE,
    "visual_artifact": TargetKind.NODE,
    "work_gate": TargetKind.NODE,
    "work_order": TargetKind.NODE,
    "worker_pool": TargetKind.NODE,
    "workspace_lease": TargetKind.NODE,
    "workspace_observation": TargetKind.NODE,
}

#: A producer run needs BOTH halves: the console matches the run key first
#: and falls back to the producer key.
PRODUCER_KEY_FIELD = "producer_key"


class Refusal(str, Enum):
    """Why a target rendered as plain text instead of a link."""

    UNDECLARED_KIND = "undeclared-kind"
    UNKNOWN_KIND = "unknown-kind"
    UNKNOWN_SUBTYPE = "unknown-subtype"
    UNROUTABLE_SUBTYPE = "unroutable-subtype"
    MISSING_BASE = "missing-operator-ui-base"
    MISSING_PRODUCER_KEY = "missing-producer-key"
    NO_CONSOLE_VIEW = "no-console-view"
    EMPTY_ID = "empty-id"
    MALFORMED_ROW = "malformed-row"


#: Operator-readable reason per refusal. Deliberately free of any URL, host,
#: or scheme text — a refusal must not hand anyone a base to copy.
REFUSAL_REASONS: dict[Refusal, str] = {
    Refusal.UNDECLARED_KIND: (
        "the answer envelope declared no target kind for this identifier, "
        "so no console route can be chosen"
    ),
    Refusal.UNKNOWN_KIND: (
        "the declared target kind is not one the operator console routes"
    ),
    Refusal.UNKNOWN_SUBTYPE: (
        "the MCP node subtype is not in the closed console-route mapping"
    ),
    Refusal.UNROUTABLE_SUBTYPE: (
        "the MCP node subtype is intentionally not routed to a console link"
    ),
    Refusal.MISSING_BASE: (
        "this deployment declares no operator console base "
        "(chat.deep_links.operator_ui_base), so no console link can be built"
    ),
    Refusal.NO_CONSOLE_VIEW: (
        "the operator console has no view that shows this kind, so the "
        "identifier is left as text"
    ),
    Refusal.MISSING_PRODUCER_KEY: (
        "a producer run needs both its producer key and its run key; "
        "the producer key is missing"
    ),
    Refusal.EMPTY_ID: "the identifier is empty",
    Refusal.MALFORMED_ROW: (
        "the MCP result is not a node-shaped row with an authoritative node_id and subtype"
    ),
}

#: Rendered in place of an empty identifier. Every other refusal still shows
#: the token it refused; without this one the token would silently vanish.
EMPTY_ID_TEXT = "(empty identifier)"


class RenderOutcome(str, Enum):
    """Distinguishes an empty render from a render that refused everything.

    ``NO_TARGETS`` means nothing linkable was cited. ``ALL_REFUSED`` means
    ids WERE cited and not one could be linked — a configuration or
    envelope problem an operator should see, not an empty answer.
    """

    LINKED = "linked"
    NO_TARGETS = "no-targets"
    ALL_REFUSED = "all-refused"


@dataclass(frozen=True)
class LinkTarget:
    """One identifier plus the kind its envelope DECLARED for it.

    ``kind`` is ``None`` when the envelope declared nothing — a refusal,
    not an invitation to guess.
    """

    id: str
    kind: TargetKind | None
    generation: str | None = None
    producer_key: str | None = None
    #: A row-level refusal recorded before route construction. This keeps
    #: unknown and intentionally-unroutable subtypes distinguishable from a
    #: missing legacy envelope kind without ever guessing a route.
    refusal: Refusal | None = None
    #: Optional display text; defaults to the id itself.
    label: str | None = None


@dataclass(frozen=True)
class RenderedLink:
    """A target rendered either as a link or as plain text with a reason."""

    text: str
    url: str | None
    refusal: Refusal | None
    reason: str | None

    @property
    def linked(self) -> bool:
        return self.url is not None

    def to_markdown(self) -> str:
        """The renderable string: a markdown link, or bare plain text.

        A refused target yields the display text and nothing else — no
        scheme, no hash, no nearest-match route.
        """
        return f"[{self.text}]({self.url})" if self.url is not None else self.text


@dataclass(frozen=True)
class RenderedLinks:
    """The result of rendering a batch of targets."""

    links: tuple[RenderedLink, ...]
    outcome: RenderOutcome

    @property
    def linked_count(self) -> int:
        return sum(1 for link in self.links if link.linked)

    @property
    def refused_count(self) -> int:
        return sum(1 for link in self.links if not link.linked)


def encode_id(value: str) -> str:
    """Percent-encode an id for a console hash.

    Pinned to ``quote(value, safe="")`` so ``/`` and ``:`` — the separators
    that actually appear in OKG node ids — encode identically to the
    console's own ``encodeURIComponent`` and the two emitters agree byte
    for byte.

    SCOPE OF THE BYTE-PARITY CLAIM. It is asserted against exactly one
    console emitter: ``instancesSelectionHash``, which is exported and
    always appends ``&generation=``. The console's UNPINNED node emitter,
    ``setInstancesFocusHash`` (the old HomeApp.tsx), built the same shape without a
    generation but is not exported, so no byte-equality assertion against it
    exists and none should be implied. The unpinned shape is covered a
    weaker way — the parity vitest feeds it through ``parseGraphHash`` and
    checks view, mode, and focus — which catches a grammar drift but not a
    byte difference. Exporting ``setInstancesFocusHash`` purely to widen a
    test assertion was judged worse than recording the gap here.

    Known boundary: ``encodeURIComponent`` leaves the six sub-delimiters
    ``! * ' ( )`` literal while ``quote(safe="")`` escapes them. Both forms
    decode to the same value through ``URLSearchParams``, so navigation is
    unaffected; only byte-equality of the two emitters differs, and only
    for ids carrying those characters. ``test_deep_links.py`` pins that
    divergence explicitly rather than leaving it to be discovered.
    """
    return quote(value, safe="")


def _clean(value: Any) -> str | None:
    """Return a non-empty stripped string, or None."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def target_kind_from_envelope(envelope: Mapping[str, Any] | None) -> TargetKind | None:
    """Read the declared target kind out of an MCP envelope.

    Returns ``None`` when the envelope is absent, declares no
    ``target_kind``, or declares one the console does not route. The caller
    turns that ``None`` into plain text; it must not fall back to a shape
    heuristic.
    """
    if not isinstance(envelope, Mapping):
        return None
    declared = _clean(envelope.get(ENVELOPE_KIND_FIELD))
    if declared is None:
        return None
    try:
        return TargetKind(declared)
    except ValueError:
        return None


def link_target_from_row(
    row: Mapping[str, Any],
    envelope: Mapping[str, Any] | None,
) -> LinkTarget:
    """Build a target from an MCP result row, typed by its envelope.

    Every malformed row yields an explicit refused target so callers cannot
    silently drop a cited result. A row whose envelope declares no kind still
    yields a target with ``kind=None``, so the render refuses loudly and the
    caller can say why.
    """
    # A node-shaped row is always governed by its authoritative subtype. The
    # legacy target_kind field is retained only for non-node callers whose row
    # has no subtype and uses session/run/trace fields.
    subtype = _clean(row.get("subtype"))
    node_id = _clean(row.get("node_id"))
    # Presence of any edge field makes this row edge-shaped, even when the
    # value is malformed or empty. Never let a node id/subtype hidden beside
    # an edge marker become a guessed node route.
    has_edge_markers = any(
        field in row for field in ("edge_id", "src", "dst", "edge_type")
    )
    if has_edge_markers:
        return LinkTarget(
            id=_clean(row.get("edge_id")) or node_id or "(malformed MCP row)",
            kind=None,
            refusal=Refusal.MALFORMED_ROW,
        )
    if subtype is not None or node_id is not None:
        if subtype is None or node_id is None:
            return LinkTarget(
                id=node_id or subtype or "(malformed MCP row)",
                kind=None,
                refusal=Refusal.MALFORMED_ROW,
            )
        kind = SUBTYPE_TO_TARGET_KIND.get(subtype)
        subtype_refusal: Refusal | None = None
        if subtype not in SUBTYPE_TO_TARGET_KIND:
            subtype_refusal = Refusal.UNKNOWN_SUBTYPE
        elif kind is None:
            subtype_refusal = Refusal.UNROUTABLE_SUBTYPE
    else:
        subtype_refusal = None
        declared_kind = (
            _clean(envelope.get(ENVELOPE_KIND_FIELD))
            if isinstance(envelope, Mapping)
            else None
        )
        kind = target_kind_from_envelope(envelope) if declared_kind is not None else None
    generation = None
    if isinstance(envelope, Mapping):
        generation = _clean(envelope.get(ENVELOPE_GENERATION_FIELD))

    if kind is None:
        # No declared kind: name whatever non-node id the row carries so the
        # refusal can quote it, but never choose a route for it.
        for field in ID_FIELD_BY_KIND.values():
            value = _clean(row.get(field))
            if value is not None:
                return LinkTarget(
                    id=value,
                    kind=None,
                    generation=generation,
                    refusal=subtype_refusal,
                )
        return LinkTarget(
            id=_clean(row.get("edge_id")) or "(malformed MCP row)",
            kind=None,
            generation=generation,
            refusal=Refusal.MALFORMED_ROW,
        )

    identifier = _clean(row.get(ID_FIELD_BY_KIND[kind]))
    if identifier is None:
        return LinkTarget(
            id=_clean(row.get("edge_id")) or "(malformed MCP row)",
            kind=None,
            generation=generation,
            refusal=Refusal.MALFORMED_ROW,
        )
    return LinkTarget(
        id=identifier,
        kind=kind,
        generation=generation,
        producer_key=_clean(row.get(PRODUCER_KEY_FIELD)),
    )


def _console_hash(target: LinkTarget) -> tuple[str | None, Refusal | None]:
    """The hash fragment for a target, or the refusal that blocked it."""
    identifier = _clean(target.id)
    if identifier is None:
        return None, Refusal.EMPTY_ID
    if target.refusal is not None:
        return None, target.refusal
    if target.kind is None:
        return None, Refusal.UNDECLARED_KIND

    if target.kind in _KINDS_WITHOUT_A_VIEW:
        # The console showed records in the Graph view and sessions, producer
        # runs and traces in the Trace view. PACT operator-ui-remove-four-views
        # deleted both, so no remaining view shows any of them. Rendering the
        # identifier as text is the honest outcome: a link to a view that does
        # not exist would open Home and look like the answer was wrong.
        return None, Refusal.NO_CONSOLE_VIEW
    return None, Refusal.UNKNOWN_KIND


def deep_link(
    target: LinkTarget,
    *,
    operator_ui_base: str | None,
) -> RenderedLink:
    """Render one target as a console link, or as plain text with a reason.

    ``operator_ui_base`` is the deployment's declared console base,
    including any path prefix. There is no default: an absent or empty base
    refuses.
    """
    # An empty id still has to render as SOMETHING. Falling through to "" is
    # the one refusal that would drop the token out of the answer entirely,
    # leaving a reader with no sign that anything was refused.
    text = _clean(target.label) or _clean(target.id) or EMPTY_ID_TEXT

    fragment, refusal = _console_hash(target)
    if refusal is not None:
        return RenderedLink(text, None, refusal, REFUSAL_REASONS[refusal])

    base = _clean(operator_ui_base)
    if base is None:
        return RenderedLink(
            text, None, Refusal.MISSING_BASE, REFUSAL_REASONS[Refusal.MISSING_BASE],
        )

    return RenderedLink(
        text=text,
        url=f"{base}{fragment}",
        refusal=None,
        reason=None,
    )


def render_links(
    targets: Iterable[LinkTarget],
    *,
    operator_ui_base: str | None,
) -> RenderedLinks:
    """Render a batch, reporting an outcome the caller can act on.

    An empty batch is ``NO_TARGETS``; a batch where every target refused is
    ``ALL_REFUSED``. Collapsing those two into "no links" is what would
    hide an undeclared console base behind an answer that merely looks
    quiet.
    """
    links = tuple(
        deep_link(target, operator_ui_base=operator_ui_base) for target in targets
    )
    if not links:
        return RenderedLinks(links=(), outcome=RenderOutcome.NO_TARGETS)
    outcome = (
        RenderOutcome.LINKED
        if any(link.linked for link in links)
        else RenderOutcome.ALL_REFUSED
    )
    return RenderedLinks(links=links, outcome=outcome)
