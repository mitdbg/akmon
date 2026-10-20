"""Witnesses for the optional typed element handle on claims (ElementRef).

PACT ``consistency-claim-graph-binding`` task.binding.element-handle: a claim
may bind the graph element it describes (a node, or an edge) via an OPTIONAL
``element_ref`` handle, per the ratified adjudication (rows 1-2) — subject-key
and corpus-level claims stay first-class, node-existence is the handle's
bound/unbound state (no ``exists`` predicate), and an unresolvable reference is
recorded UNBOUND, never guessed by string matching. These witnesses pin: the
additive field round-trips through the merge / ``dataclasses.replace`` overrides
unchanged; claim identity ignores the handle; malformed handles fail LOUD (at
construction and through each derivation's parse path); and the JSON round-trip
is exact. Pure and DB-free — the derivation parse helpers operate on in-memory
node/row dicts.
"""
from __future__ import annotations

import dataclasses
import json

import pytest

from okg.substrate.consistency.claim_model import (
    ACTION_OVERRIDE,
    ELEMENT_KIND_EDGE,
    ELEMENT_KIND_NODE,
    ClaimView,
    Correction,
    ElementRef,
    claim_logical_id,
    merge_effective_claims,
)
from okg.substrate.consistency.claim_model.views import element_ref_from_attr
from okg.substrate.derived_artifacts.claim_effective_derivation import (
    _claim_view,
    library_truth_rows,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import _view_of
from okg.substrate.errors import SubstrateError

# The four handle shapes exercised throughout (bound/unbound × node/edge).
NODE_BOUND = ElementRef(ELEMENT_KIND_NODE, node_id="n:1", bound=True)
NODE_UNBOUND = ElementRef(ELEMENT_KIND_NODE, node_id="n:missing", bound=False)
EDGE_BOUND = ElementRef(
    ELEMENT_KIND_EDGE, src="n:a", edge_type="cites", dst="n:b", bound=True
)
EDGE_UNBOUND = ElementRef(
    ELEMENT_KIND_EDGE, src="n:a", edge_type="cites", dst="n:gone", bound=False
)
ALL_SHAPES = [NODE_BOUND, NODE_UNBOUND, EDGE_BOUND, EDGE_UNBOUND]


def _claim(node_id: str, value: str, element_ref: ElementRef | None) -> ClaimView:
    return ClaimView(
        node_id=node_id,
        source_id="PageA",
        predicate="preferred_target",
        normalized_key="reconstruction",
        value=value,
        scope="Reco",
        scope_state="known",
        element_ref=element_ref,
    )


def _node(node_id: str = "claim:x", element_ref=None) -> dict:
    """A ``consistency_claim`` graph node dict (the effective derivation's input)."""
    attrs = {
        "predicate": "preferred_target",
        "normalized_key": "reconstruction",
        "value": "v0",
        "scope": "Reco",
        "scope_state": "known",
        "source_id": "PageA",
    }
    if element_ref is not None:
        attrs["element_ref"] = element_ref
    return {"node_id": node_id, "attrs": attrs}


def _eff_row(claim_id: str = "claim:x", element_ref=None) -> dict:
    """An ``okg.claim_effective`` kept row (the projection derivation's input)."""
    row = {
        "claim_id": claim_id,
        "predicate": "preferred_target",
        "normalized_key": "reconstruction",
        "value": "v0",
        "scope": "Reco",
        "scope_state": "known",
        "source_id": "PageA",
    }
    if element_ref is not None:
        row["element_ref"] = element_ref
    return row


# --------------------------------------------------------------------------- #
# Round-trip through the C-updater: merge + dataclasses.replace overrides      #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("element_ref", [*ALL_SHAPES, None])
def test_element_ref_survives_merge_and_override(element_ref):
    """Bound / unbound / corpus-level (None) claims round-trip through
    ``merge_effective_claims`` and a value OVERRIDE (which the merge applies via
    ``dataclasses.replace``) with the handle carried through untouched."""
    derived = _claim("claim:a", "v0", element_ref)

    # No corrections: the handle is preserved verbatim.
    res = merge_effective_claims([derived], [])
    assert res.effective[0].element_ref == element_ref

    # A value override must NOT disturb the additive handle field.
    corr = Correction(claim_id="claim:a", action=ACTION_OVERRIDE, sequence=1, value="v1")
    out = merge_effective_claims([derived], [corr]).effective[0]
    assert out.value == "v1"
    assert out.element_ref == element_ref


@pytest.mark.parametrize("element_ref", [*ALL_SHAPES, None])
def test_dataclasses_replace_preserves_element_ref(element_ref):
    """The exact mechanism the effective merge uses for overrides."""
    view = _claim("claim:a", "v0", element_ref)
    assert dataclasses.replace(view, value="vX").element_ref == element_ref


# --------------------------------------------------------------------------- #
# Identity excludes the handle (adjudication row 3 stays intact)              #
# --------------------------------------------------------------------------- #
def test_claim_logical_id_identical_with_and_without_element_ref():
    """Two coordinate-identical claims — one bound, one corpus-level — share
    the SAME logical id: identity is (source, predicate, key, occurrence,
    anchor) and never sees the handle (``claim_logical_id`` has no such arg)."""
    bound = _claim("claim:a", "v0", NODE_BOUND)
    corpus = _claim("claim:a", "v0", None)
    id_bound = claim_logical_id(
        bound.source_id, bound.predicate, bound.normalized_key, 0
    )
    id_corpus = claim_logical_id(
        corpus.source_id, corpus.predicate, corpus.normalized_key, 0
    )
    assert id_bound == id_corpus
    # Re-binding (bound -> unbound) likewise cannot change the id.
    rebound = dataclasses.replace(bound, element_ref=NODE_UNBOUND)
    assert claim_logical_id(
        rebound.source_id, rebound.predicate, rebound.normalized_key, 0
    ) == id_bound


# --------------------------------------------------------------------------- #
# Loud validation at construction — ValueError naming the field               #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "kwargs, field",
    [
        ({"kind": "triangle"}, "kind"),
        ({"kind": ELEMENT_KIND_NODE}, "node_id"),
        ({"kind": ELEMENT_KIND_EDGE, "edge_type": "e", "dst": "d"}, "src"),
        ({"kind": ELEMENT_KIND_EDGE, "src": "s", "dst": "d"}, "edge_type"),
        ({"kind": ELEMENT_KIND_EDGE, "src": "s", "edge_type": "e"}, "dst"),
        # Cross-field: a node handle carrying edge coordinates, and vice versa —
        # an ambiguous shape is a silent-shape bug, so it is rejected loudly.
        ({"kind": ELEMENT_KIND_NODE, "node_id": "n", "src": "x"}, "src"),
        (
            {"kind": ELEMENT_KIND_EDGE, "src": "s", "edge_type": "e", "dst": "d",
             "node_id": "n"},
            "node_id",
        ),
    ],
)
def test_malformed_element_ref_construction_raises_naming_field(kwargs, field):
    with pytest.raises(ValueError) as ei:
        ElementRef(**kwargs)
    assert field in str(ei.value)


# --------------------------------------------------------------------------- #
# JSON round-trip is exact for every valid shape                              #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("element_ref", ALL_SHAPES)
def test_element_ref_json_roundtrip_exact(element_ref):
    # dict round-trip
    assert ElementRef.from_json_dict(element_ref.to_json_dict()) == element_ref
    # JSON-text round-trip (the attrs-persistence idiom derivations would use)
    assert element_ref_from_attr(json.dumps(element_ref.to_json_dict())) == element_ref


def test_element_ref_from_attr_variants():
    assert element_ref_from_attr(None) is None  # corpus-level claim
    assert element_ref_from_attr(NODE_BOUND) is NODE_BOUND  # passthrough
    assert element_ref_from_attr(NODE_BOUND.to_json_dict()) == NODE_BOUND  # dict
    assert element_ref_from_attr(
        json.dumps(EDGE_BOUND.to_json_dict())  # JSON text
    ) == EDGE_BOUND


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "triangle"},
        {"kind": ELEMENT_KIND_NODE},  # node without node_id
        {"kind": ELEMENT_KIND_EDGE, "src": "a"},  # edge missing edge_type/dst
        {"kind": ELEMENT_KIND_NODE, "node_id": "n", "bound": "yes"},  # bound not bool
        {"kind": ELEMENT_KIND_NODE, "node_id": 5},  # node_id not a string
        "}{ not json",  # unparseable JSON text
        5,  # not a mapping
    ],
)
def test_element_ref_from_attr_malformed_raises(payload):
    with pytest.raises(ValueError):
        element_ref_from_attr(payload)


# --------------------------------------------------------------------------- #
# The derivation parse paths: parse from attrs, carry through, fail LOUD       #
# --------------------------------------------------------------------------- #
def test_effective_derivation_parses_handle_from_claim_attrs():
    """The effective derivation reads the handle off the claim node's attrs;
    an attr -> ClaimView -> ``to_json_dict`` round-trip is exact (parse then
    serialize back)."""
    payload = NODE_BOUND.to_json_dict()
    view = _claim_view(_node(element_ref=payload))
    assert view.element_ref == NODE_BOUND
    assert view.element_ref.to_json_dict() == payload
    # No handle attr -> corpus-level claim.
    assert _claim_view(_node()).element_ref is None
    # A bound claim flows through the derivation recompute entry without error.
    rows = library_truth_rows([_node(element_ref=payload)], [])
    assert ("effective_claim", "claim:x") in rows


def test_projection_derivation_passthrough_handle_from_effective_row():
    """The projection carries the handle through untouched as a passthrough
    (tier 1 does not persist it yet, so a real effective row today yields
    None) — but a present handle parses correctly."""
    view = _view_of(_eff_row(element_ref=EDGE_UNBOUND.to_json_dict()))
    assert view.element_ref == EDGE_UNBOUND
    assert _view_of(_eff_row()).element_ref is None


def test_effective_derivation_malformed_handle_raises_loud():
    """A malformed ``element_ref`` in a claim node's attrs blocks the tier-1
    recompute LOUD (SubstrateError, the GATING failure mode) — never dropped
    silently to None."""
    bad = _node(element_ref={"kind": "triangle"})
    with pytest.raises(SubstrateError, match="malformed element_ref"):
        _claim_view(bad)
    # ...and it propagates out of the derivation recompute entry.
    with pytest.raises(SubstrateError, match="malformed element_ref"):
        library_truth_rows([bad], [])


def test_projection_derivation_malformed_handle_raises_loud():
    bad = _eff_row(element_ref={"kind": ELEMENT_KIND_EDGE, "src": "a"})
    with pytest.raises(SubstrateError, match="malformed element_ref"):
        _view_of(bad)
