"""Effective-claim override-merge: ``C_eff = derived ⊕ operator source-corrections``.

The **C-updater** of the (C, Σ, ≻) claim-consistency model — the single object
an operator *source correction* updates (a rule correction updates Σ; a
governance precedent updates ≻; see :mod:`.precedence`). Each correction names
a derived claim by its deterministic id (:mod:`.identity`) and either drops it,
overrides a field, or asserts an operator-authored claim. The merged effective
set is what deterministic conflict checks consume — an operator's "that claim
was misread" changes what conflicts, without ever touching the source.

Hoisted from ``cern_twiki.effective_claims`` (the reference implementation;
PACT ``consistency-claim-model-core``). Pure merge over fixture dataclasses:
DB-free, no LLM, deterministic sorted output; corrections apply in decision
order with last-writer-wins per ``claim_id``.

Two failure modes, kept distinct:

- a correction whose target ``claim_id`` is absent from the derived set is
  **EXPIRED** — the underlying claim vanished (re-extraction dropped it); the
  correction is recorded and never applied, and that is *not* an error
  (v1-currency-spec §3: "a vanished claim expires its decisions");
- a **malformed** correction (unknown action, an OVERRIDE that changes nothing,
  an ASSERT missing coordinates, a half-specified scope) fails **LOUD** with
  :class:`EffectiveClaimError` — never silently dropped.

The merge never mutates a derived claim in place: overrides produce new
``ClaimView`` values via :func:`dataclasses.replace` (source-preserving).
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable

from okg.substrate.consistency.claim_model.views import ClaimSemantics, ClaimView

EFFECTIVE_CLAIMS_SCHEMA_VERSION = "okg.claim_model.effective.v1"

# Correction actions (the three shapes a source correction can take).
ACTION_RETRACT = "retract"    # the claim was misread entirely -> drop it from C_eff
ACTION_OVERRIDE = "override"  # mis-read value / mis-scoped / mis-linked -> replace fields
ACTION_ASSERT = "assert"      # operator authors a claim absent from derivation
_ACTIONS = frozenset({ACTION_RETRACT, ACTION_OVERRIDE, ACTION_ASSERT})

# Per-claim provenance in the merged set.
PROV_DERIVED = "derived"             # straight from the extractor, untouched
PROV_CORRECTED = "corrected"         # a derived claim with an operator override applied
PROV_ASSERTED = "operator_asserted"  # operator-authored (no derived original)

# Ordering of actions that share a sequence number (deterministic tie-break only).
_ACTION_ORDER = {ACTION_RETRACT: 0, ACTION_OVERRIDE: 1, ACTION_ASSERT: 2}


class EffectiveClaimError(RuntimeError):
    """A correction is malformed (not merely expired); fail loud rather than guess."""


@dataclass(frozen=True)
class Correction:
    """One operator source-correction, targeting a claim by deterministic ``claim_id``.

    ``sequence`` is the decision order (lower applied first); corrections on the
    same ``claim_id`` resolve last-writer-wins. ``decision_id`` is an optional
    provenance back-pointer (the decision node that produced this correction).

    For OVERRIDE, the non-``None`` fields among ``value`` / ``normalized_key`` /
    (``scope`` + ``scope_state``) replace the derived claim's; ``None`` means
    "leave unchanged"; ``scope`` and ``scope_state`` must be set together. For
    ASSERT, the full coordinates ``source_id`` / ``predicate`` /
    ``normalized_key`` / ``value`` are required (``scope`` / ``scope_state``
    optional, together).
    """

    claim_id: str
    action: str
    sequence: int = 0
    decision_id: str = ""
    value: str | None = None
    normalized_key: str | None = None
    scope: str | None = None
    scope_state: str | None = None
    source_id: str | None = None
    predicate: str | None = None
    semantics: ClaimSemantics | None = None


@dataclass(frozen=True)
class MergeResult:
    """The effective claim set plus per-claim provenance and applied bookkeeping."""

    effective: tuple[ClaimView, ...]
    provenance: dict[str, str]       # claim_id -> PROV_*  (only for claims in `effective`)
    retracted: tuple[str, ...]       # claim_ids dropped by a RETRACT
    corrected: tuple[str, ...]       # derived claim_ids with an override applied
    asserted: tuple[str, ...]        # operator-authored claim_ids
    expired: tuple[str, ...]         # corrections naming a vanished/absent claim_id
    schema_version: str = EFFECTIVE_CLAIMS_SCHEMA_VERSION


def _sorted_corrections(corrections: Iterable[Correction]) -> list[Correction]:
    """Global decision order: (sequence, claim_id, action) — total and deterministic."""
    return sorted(
        corrections,
        key=lambda c: (c.sequence, c.claim_id, _ACTION_ORDER.get(c.action, 99)),
    )


def _check_scope_pair(c: Correction) -> None:
    if (c.scope is None) != (c.scope_state is None):
        raise EffectiveClaimError(
            f"correction {c.claim_id!r}: scope and scope_state must be set together"
        )


def _apply_override(cur: ClaimView, c: Correction) -> ClaimView:
    _check_scope_pair(c)
    changes: dict[str, Any] = {}
    if c.value is not None:
        changes["value"] = c.value
    if c.normalized_key is not None:
        changes["normalized_key"] = c.normalized_key
    if c.scope is not None:
        changes["scope"] = c.scope
        changes["scope_state"] = c.scope_state  # type: ignore[assignment]
    if c.semantics is not None:
        changes["semantics"] = c.semantics
    if not changes:
        raise EffectiveClaimError(
            f"correction {c.claim_id!r}: OVERRIDE changes no field"
        )
    return replace(cur, **changes)


def _asserted_claim(c: Correction) -> ClaimView:
    missing = [
        f for f in ("source_id", "predicate", "normalized_key", "value")
        if getattr(c, f) is None
    ]
    if missing:
        raise EffectiveClaimError(
            f"correction {c.claim_id!r}: ASSERT missing coordinates {missing}"
        )
    _check_scope_pair(c)
    return ClaimView(
        node_id=c.claim_id,
        source_id=c.source_id,             # type: ignore[arg-type]
        predicate=c.predicate,             # type: ignore[arg-type]
        normalized_key=c.normalized_key,   # type: ignore[arg-type]
        value=c.value,                     # type: ignore[arg-type]
        scope=c.scope or "",
        scope_state=c.scope_state or "unknown",
        semantics=c.semantics,
    )


def merge_effective_claims(
    derived: Iterable[ClaimView],
    corrections: Iterable[Correction],
) -> MergeResult:
    """``C_eff = derived ⊕ corrections`` — the override-merge. Pure, deterministic.

    Corrections apply in decision order (last-writer-wins per ``claim_id``). A
    correction whose target is absent is EXPIRED (recorded, never applied); a
    malformed correction raises :class:`EffectiveClaimError`. Derived claims are
    never mutated in place.
    """
    state: dict[str, ClaimView | None] = {c.node_id: c for c in derived}
    origin: dict[str, str] = {nid: PROV_DERIVED for nid in state}
    touched: set[str] = set()
    retracted: set[str] = set()
    asserted: set[str] = set()
    expired: list[str] = []

    for c in _sorted_corrections(corrections):
        if c.action not in _ACTIONS:
            raise EffectiveClaimError(
                f"correction {c.claim_id!r}: unknown action {c.action!r}"
            )
        cid = c.claim_id
        present = state.get(cid) is not None
        ever = cid in origin

        if c.action == ACTION_RETRACT:
            if present:
                state[cid] = None
                retracted.add(cid)
            elif not ever:
                expired.append(cid)  # retract of a claim that never existed
            # else: redundant retract of an already-gone claim -> no-op
            continue

        if c.action == ACTION_OVERRIDE:
            if not present:
                expired.append(cid)  # cannot override an absent/retracted claim
                continue
            state[cid] = _apply_override(state[cid], c)  # type: ignore[arg-type]
            touched.add(cid)
            continue

        # ASSERT
        if present:
            raise EffectiveClaimError(
                f"correction {cid!r}: ASSERT of an already-present claim "
                f"(use OVERRIDE to change a derived claim)"
            )
        state[cid] = _asserted_claim(c)
        origin[cid] = PROV_ASSERTED
        asserted.add(cid)
        retracted.discard(cid)  # an ASSERT after a RETRACT resurrects the id

    effective = tuple(sorted(
        (v for v in state.values() if v is not None), key=lambda x: x.node_id
    ))
    provenance: dict[str, str] = {}
    for cv in effective:
        cid = cv.node_id
        if origin.get(cid) == PROV_ASSERTED:
            provenance[cid] = PROV_ASSERTED
        elif cid in touched:
            provenance[cid] = PROV_CORRECTED
        else:
            provenance[cid] = PROV_DERIVED

    return MergeResult(
        effective=effective,
        provenance=provenance,
        retracted=tuple(sorted(cid for cid in retracted if state.get(cid) is None)),
        corrected=tuple(sorted(cid for cid in touched if state.get(cid) is not None)),
        asserted=tuple(sorted(cid for cid in asserted if state.get(cid) is not None)),
        expired=tuple(sorted(set(expired))),
    )
