"""Coverage for `add-subtype-refines-to-catalog`.

Covers the pure-Python pieces — cycle detection in
`modules_apply._detect_refinement_cycles` and refinement-aware
subtype matching in `producer_authority._subtype_matches`. The
DB-driven closure population, narrowing trigger, and end-to-end
producer-authority gate behavior are covered by integration
tests under `tests/substrate/catalog/` (marked
`pytest.mark.postgres`) and exercised against a real
`clean_runtime` fixture.
"""
from __future__ import annotations

import pytest

from okg.substrate.catalog.modules_apply import (
    CircularRefinementError,
    _detect_refinement_cycles,
)
from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.producer_authority import _subtype_matches


# ---------- _subtype_matches ----------


def test_subtype_matches_exact_legacy() -> None:
    """No ancestors map → falls back to exact-match (strict
    superset of pre-refinement behavior)."""
    assert _subtype_matches("foo", "foo", None) is True
    assert _subtype_matches("foo", "bar", None) is False


def test_subtype_matches_self_via_closure() -> None:
    """A concrete subtype matches its own rule via the reflexive
    depth-0 ancestor row."""
    ancestors = {"foo": {"foo"}, "bar": {"bar"}}
    assert _subtype_matches("foo", "foo", ancestors) is True


def test_subtype_matches_abstract_parent_to_refinement() -> None:
    """A rule declared at an abstract parent matches a record
    whose subtype refines that parent."""
    ancestors = {
        "paper_citation_evidence": {
            "paper_citation_evidence", "evidence",
        },
        "evidence": {"evidence"},
    }
    # Rule declares `evidence` (abstract); record carries the
    # refinement. Match expected.
    assert _subtype_matches(
        "evidence", "paper_citation_evidence", ancestors,
    ) is True


def test_subtype_matches_refinement_does_not_match_unrelated_rule() -> None:
    ancestors = {
        "paper_citation_evidence": {
            "paper_citation_evidence", "evidence",
        },
    }
    assert _subtype_matches(
        "hypothesis", "paper_citation_evidence", ancestors,
    ) is False


def test_subtype_matches_concrete_rule_does_not_match_parent() -> None:
    """The reverse direction: a rule declared at a concrete
    subtype SHALL NOT match a record carrying the abstract
    parent. Refinement is one-way; an `evidence` record is not
    a `paper_citation_evidence`.
    """
    ancestors = {
        "paper_citation_evidence": {
            "paper_citation_evidence", "evidence",
        },
        "evidence": {"evidence"},
    }
    assert _subtype_matches(
        "paper_citation_evidence", "evidence", ancestors,
    ) is False


def test_subtype_matches_unknown_record_subtype() -> None:
    """A record whose subtype isn't in the ancestors map (e.g.,
    a brand-new subtype not in the active catalog) doesn't match
    any rule besides an exact-named one."""
    ancestors = {"evidence": {"evidence"}}
    assert _subtype_matches(
        "evidence", "newly_minted_subtype", ancestors,
    ) is False
    # Self exact-match still works (it's the same string).
    assert _subtype_matches(
        "newly_minted_subtype", "newly_minted_subtype", ancestors,
    ) is True


# ---------- _detect_refinement_cycles ----------


def test_cycle_detection_acyclic_chain() -> None:
    """A → B → C (no cycle) SHALL NOT raise."""
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {"a", "b", "c"}
    composed.subtype_parent = {"a": "b", "b": "c"}
    _detect_refinement_cycles(composed)  # no raise


def test_cycle_detection_root_subtypes() -> None:
    """Subtypes without parents are just roots; no traversal."""
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {"foo", "bar"}
    composed.subtype_parent = {}
    _detect_refinement_cycles(composed)  # no raise


def test_cycle_detection_self_parent_rejected() -> None:
    """A.parent = A SHALL raise CircularRefinementError naming A."""
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {"foo"}
    composed.subtype_parent = {"foo": "foo"}
    with pytest.raises(CircularRefinementError) as exc_info:
        _detect_refinement_cycles(composed)
    assert "foo" in str(exc_info.value)


def test_cycle_detection_two_node_cycle() -> None:
    """A↔B SHALL raise enumerating both."""
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {"a", "b"}
    composed.subtype_parent = {"a": "b", "b": "a"}
    with pytest.raises(CircularRefinementError) as exc_info:
        _detect_refinement_cycles(composed)
    msg = str(exc_info.value)
    assert "a" in msg and "b" in msg


def test_cycle_detection_three_node_cycle() -> None:
    """A→B→C→A SHALL raise enumerating all three."""
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {"a", "b", "c"}
    composed.subtype_parent = {"a": "b", "b": "c", "c": "a"}
    with pytest.raises(CircularRefinementError) as exc_info:
        _detect_refinement_cycles(composed)
    msg = str(exc_info.value)
    for node in ("a", "b", "c"):
        assert node in msg


def test_cycle_detection_dangling_parent_not_a_cycle() -> None:
    """A.parent = nonexistent → not a cycle (resolved at DB
    apply-time as an FK violation, not here)."""
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {"a"}
    composed.subtype_parent = {"a": "missing_parent_subtype"}
    # The walk terminates when parents.get(node) returns None.
    # Cycle detection passes; the FK on okg.subtypes catches the
    # dangling parent at INSERT time.
    _detect_refinement_cycles(composed)


# ---------- ComposedCatalog.subtype_parent integration ----------


def test_composed_catalog_subtype_parent_default_empty() -> None:
    """Fresh ComposedCatalog has no subtype_parent entries."""
    composed = ComposedCatalog(deployment_name="test")
    assert composed.subtype_parent == {}


def test_composed_catalog_subtype_parent_explicit() -> None:
    """Construction can set subtype_parent explicitly."""
    composed = ComposedCatalog(
        deployment_name="test",
        subtypes={"foo", "bar"},
        subtype_parent={"foo": "bar"},
    )
    assert composed.subtype_parent == {"foo": "bar"}
