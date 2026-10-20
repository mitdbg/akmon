"""Claim views and scope relations — the shared shapes of the (C, Σ, ≻) core.

Generalized from ``cern_twiki.currency_check.ClaimView`` (the reference
deployment for the claim-consistency model; see the v1-currency-spec §10
core/deployment split and PACT ``consistency-claim-model-core``). The one
generalization: the claim's originating document is ``source_id`` (any
document/record id), not a TWiki ``page_id``.

Pure module: dataclasses + a scope relation. No DB, no LLM, no wall-clock.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from okg.substrate.consistency.claim_model.applicability import (
    HEADING_KNOWN,
    HEADING_UNKNOWN,
    REL_DISJOINT as _REL_DISJOINT,
    REL_OVERLAP as _REL_OVERLAP,
    REL_UNKNOWN as _REL_UNKNOWN,
    Applicability,
    applicability_relation,
)

# Compatibility re-exports: external v1 callers historically import these
# constants from views.py. The authoritative definitions live in applicability.py.
REL_DISJOINT = _REL_DISJOINT
REL_OVERLAP = _REL_OVERLAP
REL_UNKNOWN = _REL_UNKNOWN

CLAIM_VIEW_SCHEMA_VERSION = "okg.claim_model.views.v1"

# The persisted graph subtype of claim-plane nodes. Defined HERE — the pure
# bottom layer — so consistency modules never import it from the derivation
# layer above them: possible_effects importing it from
# claim_effective_derivation created a package-init import cycle
# (consistency.__init__ -> reuse_scheduler -> possible_effects -> the
# mid-initialization derivation module) that broke any import order reaching
# the derivation first.
CLAIM_SUBTYPE = "consistency_claim"

SCOPE_KNOWN = "known"
SCOPE_UNKNOWN = "unknown"

# The heading-path separator scope strings use ("A > B > C").
SCOPE_PATH_SEP = " > "

CLAIM_SEMANTICS_SCHEMA_VERSION = "okg.claim_model.semantics.v2"

POLARITY_UNSPECIFIED = "unspecified"
POLARITY_AFFIRMED = "affirmed"
POLARITY_NEGATED = "negated"
_POLARITIES = frozenset(
    {POLARITY_UNSPECIFIED, POLARITY_AFFIRMED, POLARITY_NEGATED}
)

MODALITY_UNSPECIFIED = "unspecified"
MODALITY_CURRENT = "current"
MODALITY_PENDING = "pending"
MODALITY_HISTORICAL = "historical"
MODALITY_CONDITIONAL = "conditional"
_MODALITIES = frozenset(
    {
        MODALITY_UNSPECIFIED,
        MODALITY_CURRENT,
        MODALITY_PENDING,
        MODALITY_HISTORICAL,
        MODALITY_CONDITIONAL,
    }
)

# ---- Element handle (adjudication rows 1-2; notation-map row 1) ------------ #
# A claim MAY carry a typed reference to the graph element it describes (a
# node, or an edge). The reference is honest: node-existence is encoded by the
# handle's bound/unbound state — not a magic "exists" predicate — and an
# unresolvable reference is recorded UNBOUND carrying the coordinates it failed
# to resolve, never guessed by string matching (resolution itself is a later
# task). A claim with no handle is corpus-level (``element_ref is None``); the
# handle is excluded from claim identity (:mod:`.identity` never sees it).
ELEMENT_KIND_NODE = "node"
ELEMENT_KIND_EDGE = "edge"
_ELEMENT_KINDS = frozenset({ELEMENT_KIND_NODE, ELEMENT_KIND_EDGE})


def _ref_coord(payload: Mapping[str, Any], name: str) -> str:
    """A required string coordinate from a JSON payload; loud on a non-string."""
    value = payload.get(name)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(
            f"ElementRef payload {name!r} must be a string, got "
            f"{type(value).__name__}"
        )
    return value


@dataclass(frozen=True)
class ElementRef:
    """A typed handle from a claim to the graph element it describes.

    ``kind == "node"``: ``node_id`` is the referenced node (edge coordinates
    stay ``""``). ``kind == "edge"``: ``src`` / ``edge_type`` / ``dst`` name
    the referenced edge (``node_id`` stays ``""``). ``bound`` records whether
    the reference RESOLVED to a live element: an unresolvable reference is kept
    ``bound=False`` carrying the coordinates it failed to resolve — the loud,
    never-guessed shape (adjudication rows 1-2; resolution lands in a later
    task). Node-existence is this bound/unbound state, not a predicate.

    Excluded from claim identity: binding (or re-binding) a claim to an element
    never mints a new logical id.
    """

    kind: str
    node_id: str = ""
    src: str = ""
    edge_type: str = ""
    dst: str = ""
    bound: bool = False

    def __post_init__(self) -> None:
        if self.kind not in _ELEMENT_KINDS:
            raise ValueError(
                f"ElementRef.kind must be one of {sorted(_ELEMENT_KINDS)}, "
                f"got {self.kind!r}"
            )
        if self.kind == ELEMENT_KIND_NODE:
            if not self.node_id:
                raise ValueError(
                    "ElementRef(kind='node') requires a non-empty 'node_id'"
                )
            if self.src or self.edge_type or self.dst:
                raise ValueError(
                    "ElementRef(kind='node') must not set edge fields "
                    "'src'/'edge_type'/'dst'"
                )
        else:  # ELEMENT_KIND_EDGE
            missing = [
                name for name in ("src", "edge_type", "dst")
                if not getattr(self, name)
            ]
            if missing:
                raise ValueError(
                    f"ElementRef(kind='edge') requires non-empty {missing}"
                )
            if self.node_id:
                raise ValueError("ElementRef(kind='edge') must not set 'node_id'")

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize to a plain JSON object for persistence in a node's attrs."""
        if self.kind == ELEMENT_KIND_NODE:
            return {"kind": self.kind, "node_id": self.node_id, "bound": self.bound}
        return {
            "kind": self.kind,
            "src": self.src,
            "edge_type": self.edge_type,
            "dst": self.dst,
            "bound": self.bound,
        }

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> ElementRef:
        """Inverse of :meth:`to_json_dict`. Raises ``ValueError`` (naming the
        field) on any malformed payload — never returns a guessed handle."""
        if not isinstance(payload, Mapping):
            raise ValueError(
                f"ElementRef payload must be a mapping, got "
                f"{type(payload).__name__}"
            )
        bound = payload.get("bound", False)
        if not isinstance(bound, bool):
            raise ValueError(
                f"ElementRef payload 'bound' must be a boolean, got {bound!r}"
            )
        kind = payload.get("kind")
        if kind == ELEMENT_KIND_NODE:
            return cls(
                kind=kind, node_id=_ref_coord(payload, "node_id"), bound=bound
            )
        if kind == ELEMENT_KIND_EDGE:
            return cls(
                kind=kind,
                src=_ref_coord(payload, "src"),
                edge_type=_ref_coord(payload, "edge_type"),
                dst=_ref_coord(payload, "dst"),
                bound=bound,
            )
        raise ValueError(
            f"ElementRef payload 'kind' must be one of {sorted(_ELEMENT_KINDS)}, "
            f"got {kind!r}"
        )


def element_ref_from_attr(payload: Any) -> ElementRef | None:
    """Parse an ``element_ref`` attr payload into an :class:`ElementRef`.

    ``None``/absent -> a corpus-level claim (no graph referent). A JSON object
    (or its JSON text) -> the handle. An :class:`ElementRef` passes through. A
    malformed payload raises ``ValueError`` — never silently ``None`` (the
    honest-shape contract; the derivations wrap this as their loud GATING
    failure).
    """
    if payload is None:
        return None
    if isinstance(payload, ElementRef):
        return payload
    if isinstance(payload, str):
        payload = json.loads(payload)
    return ElementRef.from_json_dict(payload)


def _required_string(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"claim_semantics.{field} must be a non-empty string")
    return value


def _optional_string(
    payload: Mapping[str, Any], field: str, default: str = ""
) -> str:
    value = payload.get(field, default)
    if not isinstance(value, str):
        raise ValueError(f"claim_semantics.{field} must be a string")
    return value


@dataclass(frozen=True)
class ClaimSemantics:
    """The additive v2 semantic payload carried beside the v1 compatibility view."""

    logical_subject: str
    activity: str
    value_type: str = ""
    target_role: str = ""
    polarity: str = POLARITY_UNSPECIFIED
    modality: str = MODALITY_UNSPECIFIED
    conditions: tuple[str, ...] = ()
    applicability: Applicability = Applicability()
    event_order: int | None = None

    def __post_init__(self) -> None:
        for name in ("logical_subject", "activity"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"claim_semantics.{name} must be a non-empty string")
        for name in ("value_type", "target_role"):
            if not isinstance(getattr(self, name), str):
                raise ValueError(f"claim_semantics.{name} must be a string")
        if self.polarity not in _POLARITIES:
            raise ValueError(
                f"claim_semantics.polarity must be one of {sorted(_POLARITIES)}"
            )
        if self.modality not in _MODALITIES:
            raise ValueError(
                f"claim_semantics.modality must be one of {sorted(_MODALITIES)}"
            )
        if not isinstance(self.conditions, tuple) or not all(
            isinstance(condition, str) and condition for condition in self.conditions
        ):
            raise ValueError(
                "claim_semantics.conditions must be a tuple of non-empty strings"
            )
        if self.event_order is not None and (
            isinstance(self.event_order, bool) or not isinstance(self.event_order, int)
        ):
            raise ValueError("claim_semantics.event_order must be an integer or null")
        if not isinstance(self.applicability, Applicability):
            raise ValueError(
                "claim_semantics.applicability must be an Applicability object"
            )

    def resolved_applicability(self) -> Applicability:
        return Applicability(
            scope_bounds=self.applicability.scope_bounds,
            heading_path=self.applicability.heading_path,
            heading_state=self.applicability.heading_state,
            valid_time=self.applicability.valid_time,
            logical_subject=self.logical_subject,
            activity=self.activity,
            modality=(
                "" if self.modality == MODALITY_UNSPECIFIED else self.modality
            ),
        )

    def requires_typed_rules(self) -> bool:
        return bool(
            self.value_type
            or self.target_role
            or self.polarity != POLARITY_UNSPECIFIED
            or self.conditions
            or self.event_order is not None
        )

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": CLAIM_SEMANTICS_SCHEMA_VERSION,
            "logical_subject": self.logical_subject,
            "activity": self.activity,
            "value_type": self.value_type,
            "target_role": self.target_role,
            "polarity": self.polarity,
            "modality": self.modality,
            "conditions": list(self.conditions),
            "applicability": self.applicability.to_json_dict(),
            "event_order": self.event_order,
        }

    def to_attr(self) -> str:
        return json.dumps(self.to_json_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "ClaimSemantics":
        if not isinstance(payload, Mapping):
            raise ValueError("claim_semantics must be an object")
        allowed = {
            "schema_version",
            "logical_subject",
            "activity",
            "value_type",
            "target_role",
            "polarity",
            "modality",
            "conditions",
            "applicability",
            "event_order",
        }
        unknown = sorted(key for key in payload if key not in allowed)
        if unknown:
            raise ValueError(f"claim_semantics contains unknown fields {unknown}")
        version = payload.get("schema_version")
        if version != CLAIM_SEMANTICS_SCHEMA_VERSION:
            raise ValueError(
                "claim_semantics.schema_version must be "
                f"{CLAIM_SEMANTICS_SCHEMA_VERSION!r}, got {version!r}"
            )
        raw_conditions = payload.get("conditions", [])
        if not isinstance(raw_conditions, list):
            raise ValueError("claim_semantics.conditions must be a list")
        raw_event_order = payload.get("event_order")
        return cls(
            logical_subject=_required_string(payload, "logical_subject"),
            activity=_required_string(payload, "activity"),
            value_type=_optional_string(payload, "value_type"),
            target_role=_optional_string(payload, "target_role"),
            polarity=_optional_string(payload, "polarity", POLARITY_UNSPECIFIED),
            modality=_optional_string(payload, "modality", MODALITY_UNSPECIFIED),
            conditions=tuple(raw_conditions),
            applicability=Applicability.from_json_dict(
                payload.get("applicability", {})
            ),
            event_order=raw_event_order,
        )


def claim_semantics_from_attr(payload: Any) -> ClaimSemantics | None:
    if payload is None:
        return None
    if isinstance(payload, ClaimSemantics):
        return payload
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"claim_semantics is not valid JSON: {exc}") from exc
    return ClaimSemantics.from_json_dict(payload)


@dataclass(frozen=True)
class ClaimView:
    """The minimal view of one effective claim that checks and resolvers consume.

    Decoupled from any extractor: ``node_id`` is the claim's deterministic
    logical id (see :mod:`.identity`), ``source_id`` the document/record the
    claim came from (used only for internal-vs-cross-document locality),
    ``scope`` a heading-path string with ``scope_state`` declaring whether the
    scope is known ("" + ``unknown`` means the claim's applicability could not
    be recovered).

    ``element_ref`` is the OPTIONAL typed handle to the graph element the claim
    describes (adjudication rows 1-2); ``None`` is a corpus-level claim with no
    graph referent. Additive (default ``None``): the module version stays
    ``views.v1`` and claim identity ignores it.
    """

    node_id: str
    source_id: str
    predicate: str
    normalized_key: str
    value: str
    scope: str = ""
    scope_state: str = SCOPE_UNKNOWN
    element_ref: ElementRef | None = None
    semantics: ClaimSemantics | None = None

    def __post_init__(self) -> None:
        if self.scope_state not in {SCOPE_KNOWN, SCOPE_UNKNOWN}:
            raise ValueError(
                "ClaimView.scope_state must be 'known' or 'unknown', got "
                f"{self.scope_state!r}"
            )
        if self.semantics is not None and not isinstance(
            self.semantics, ClaimSemantics
        ):
            raise ValueError("ClaimView.semantics must be ClaimSemantics or null")
        if self.semantics is None:
            return
        typed = self.semantics.applicability
        legacy_is_default = self.scope == "" and self.scope_state == SCOPE_UNKNOWN
        if legacy_is_default:
            return
        typed_matches_legacy = (
            typed.heading_path == self.scope
            and typed.heading_state
            == (HEADING_KNOWN if self.scope_state == SCOPE_KNOWN else HEADING_UNKNOWN)
        )
        if not typed_matches_legacy:
            raise ValueError(
                f"claim {self.node_id!r}: legacy scope/scope_state disagree with "
                "claim_semantics.applicability heading_path/heading_state"
            )

    def resolved_applicability(self) -> Applicability:
        if self.semantics is None:
            return Applicability.from_v1(self.scope, self.scope_state)
        return self.semantics.resolved_applicability()


def scope_relation(a: ClaimView, b: ClaimView) -> str:
    """Compatibility name delegating to the single typed applicability operator."""

    return applicability_relation(a.resolved_applicability(), b.resolved_applicability())
