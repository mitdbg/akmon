"""Refinement witnesses for formal/consistency/TypedApplicability.tla."""
from __future__ import annotations

from itertools import product

import pytest

from okg.substrate.consistency.claim_model.applicability import (
    OMEGA,
    REL_DISJOINT,
    REL_OVERLAP,
    REL_UNKNOWN,
    Applicability,
    ScopeBounds,
    TimeBounds,
    applicability_relation,
)
from okg.substrate.consistency.claim_model.conflicts import (
    ConflictRules,
    OUTCOME_DEFINITE,
    derive_conflict_findings,
)
from okg.substrate.consistency.claim_model.views import (
    MODALITY_CURRENT,
    MODALITY_PENDING,
    POLARITY_NEGATED,
    ClaimSemantics,
    ClaimView,
    ElementRef,
    claim_semantics_from_attr,
)


def _semantics(
    *,
    subject: str = "entity:one",
    activity: str = "installation",
    modality: str = MODALITY_CURRENT,
    start: str = "2025-01-01",
    end: str = "2026-01-01",
    **kwargs: object,
) -> ClaimSemantics:
    return ClaimSemantics(
        logical_subject=subject,
        activity=activity,
        modality=modality,
        applicability=Applicability(valid_time=TimeBounds.known(start, end)),
        **kwargs,  # type: ignore[arg-type]
    )


def _claim(node_id: str, value: str, semantics: ClaimSemantics) -> ClaimView:
    return ClaimView(
        node_id=node_id,
        source_id="doc:one",
        predicate="exclusive_target",
        normalized_key="legacy:shared",
        value=value,
        semantics=semantics,
    )


_RULES = ConflictRules(
    conflict_predicates=frozenset({"exclusive_target"}),
    incompatible=lambda _predicate, left, right: left != right,
    constraint_for=lambda _predicate: "exclusive",
)


def test_typed_payload_roundtrip_and_binding_independence() -> None:
    semantics = _semantics(subject="logical:service", activity="deployment")
    assert claim_semantics_from_attr(semantics.to_attr()) == semantics
    claim = ClaimView(
        node_id="claim:bound",
        source_id="doc:one",
        predicate="exclusive_target",
        normalized_key="legacy:shared",
        value="blue",
        element_ref=ElementRef(kind="node", node_id="graph:other", bound=True),
        semantics=semantics,
    )
    assert claim.element_ref is not None
    assert claim.element_ref.node_id == "graph:other"
    assert claim.semantics.logical_subject == "logical:service"
    assert claim.semantics.activity == "deployment"


@pytest.mark.parametrize(
    "payload, match",
    [
        ({"schema_version": "wrong"}, "schema_version"),
        ({"schema_version": "okg.claim_model.semantics.v2", "logical_subject": "", "activity": "a"}, "logical_subject"),
        (
            {
                "schema_version": "okg.claim_model.semantics.v2",
                "logical_subject": "entity:one",
                "activity": "installation",
                "polairty": "negated",
            },
            "unknown fields.*polairty",
        ),
    ],
)
def test_malformed_typed_payload_fails_loud(payload: object, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        claim_semantics_from_attr(payload)
    with pytest.raises(ValueError, match="half-open"):
        TimeBounds.known("2025-01-01", "2025-01-01")
    with pytest.raises(ValueError, match="event_order"):
        _semantics(event_order=True)


def test_malformed_nested_typed_payloads_fail_without_coercion() -> None:
    with pytest.raises(ValueError, match=r"scope_bounds.lo\['era'\].*list"):
        ScopeBounds.from_json_dict({
            "lo": {"era": {"run3": True}},
            "hi": {"era": "*"},
        })
    with pytest.raises(ValueError, match=r"lo\['era'\].*list/set"):
        ScopeBounds.known({"era": "run3"})
    with pytest.raises(ValueError, match="dimension names"):
        ScopeBounds.unbounded([1])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="unknown fields.*future_axis"):
        Applicability.from_json_dict({"future_axis": "run4"})
    with pytest.raises(ValueError, match="unknown fields.*timezone"):
        TimeBounds.from_json_dict({"state": "unbounded", "timezone": "UTC"})
    with pytest.raises(ValueError, match="ClaimSemantics"):
        ClaimView("c", "d", "p", "k", "v", semantics={})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Applicability"):
        ClaimSemantics(
            logical_subject="entity:one",
            activity="installation",
            applicability={},  # type: ignore[arg-type]
        )


def test_time_bounds_canonicalize_iso_variants_before_comparison() -> None:
    compact = TimeBounds.known("20250101", "20250201")
    extended = TimeBounds.known("2025-01-15", "2025-01-20")
    assert compact.start == "2025-01-01"
    assert compact.end == "2025-02-01"
    assert applicability_relation(
        Applicability(valid_time=compact), Applicability(valid_time=extended)
    ) == REL_OVERLAP


def test_dual_heading_authority_disagreement_fails_loud() -> None:
    semantics = ClaimSemantics(
        logical_subject="entity:one",
        activity="install",
        applicability=Applicability.from_v1("Typed", "known"),
    )
    with pytest.raises(ValueError, match="legacy scope/scope_state disagree"):
        ClaimView("c", "d", "p", "k", "v", "Legacy", "known", semantics=semantics)


def test_typed_conflict_scenarios_cover_time_activity_and_modality() -> None:
    current = _claim("c:current", "blue", _semantics())
    concurrent = _claim("c:concurrent", "green", _semantics())
    assert derive_conflict_findings([current, concurrent], _RULES)[0].outcome == (
        OUTCOME_DEFINITE
    )

    pending = _claim(
        "c:pending", "green", _semantics(modality=MODALITY_PENDING)
    )
    historical = _claim(
        "c:historical",
        "green",
        _semantics(start="2024-01-01", end="2025-01-01"),
    )
    other_activity = _claim(
        "c:activity", "green", _semantics(activity="retirement")
    )
    assert derive_conflict_findings([current, pending], _RULES) == []
    assert derive_conflict_findings([current, historical], _RULES) == []
    assert derive_conflict_findings([current, other_activity], _RULES) == []


def test_callback_owned_semantics_require_typed_callback() -> None:
    left = _claim("c:left", "blue", _semantics(polarity=POLARITY_NEGATED))
    right = _claim("c:right", "green", _semantics())
    with pytest.raises(ValueError, match="typed_incompatible"):
        derive_conflict_findings([left, right], _RULES)
    calls: list[tuple[str, str]] = []
    typed_rules = ConflictRules(
        conflict_predicates=_RULES.conflict_predicates,
        incompatible=_RULES.incompatible,
        constraint_for=_RULES.constraint_for,
        typed_incompatible=lambda a, b: not calls.append((a.node_id, b.node_id)),
    )
    assert len(derive_conflict_findings([left, right], typed_rules)) == 1
    assert calls == [("c:left", "c:right")]


def test_unknown_never_becomes_definite() -> None:
    unknown = Applicability(scope_bounds=ScopeBounds.unknown())
    known = Applicability(scope_bounds=ScopeBounds.known({"era": {"run3"}}))
    assert applicability_relation(unknown, known) == REL_UNKNOWN


def test_upper_disjoint_never_produces_a_finding() -> None:
    run2 = Applicability(scope_bounds=ScopeBounds.known({"era": {"run2"}}))
    run3 = Applicability(scope_bounds=ScopeBounds.known({"era": {"run3"}}))
    assert applicability_relation(run2, run3) == REL_DISJOINT


def test_partial_lower_disjoint_upper_overlap_is_possible() -> None:
    a = Applicability(scope_bounds=ScopeBounds.partial(
        lo={"era": {"run2"}}, hi={"era": {"run2", "run3"}}
    ))
    b = Applicability(scope_bounds=ScopeBounds.partial(
        lo={"era": {"run3"}}, hi={"era": {"run2", "run3"}}
    ))
    assert applicability_relation(a, b) == REL_UNKNOWN


def test_explicit_unbounded_is_definite_capable() -> None:
    any_era = Applicability(scope_bounds=ScopeBounds.unbounded(["era"]))
    run3 = Applicability(scope_bounds=ScopeBounds.known({"era": {"run3"}}))
    assert applicability_relation(any_era, run3) == REL_OVERLAP
    assert any_era.scope_bounds is not None
    assert any_era.scope_bounds.lo_bound("era") is OMEGA


def test_axes_compose_conjunctively() -> None:
    a = Applicability(
        scope_bounds=ScopeBounds.known({"era": {"run3"}}), activity="replay"
    )
    b = Applicability(
        scope_bounds=ScopeBounds.known({"era": {"run3"}}), activity="processing"
    )
    assert applicability_relation(a, b) == REL_DISJOINT


def test_v1_heading_adapter_equivalence_including_known_empty() -> None:
    root = Applicability.from_v1("A", "known")
    child_a = Applicability.from_v1("A > B", "known")
    child_b = Applicability.from_v1("A > C", "known")
    empty = Applicability.from_v1("", "known")
    unknown = Applicability.from_v1("", "unknown")

    assert applicability_relation(root, child_a) == REL_OVERLAP
    assert applicability_relation(child_a, child_b) == REL_DISJOINT
    assert applicability_relation(empty, empty) == REL_OVERLAP
    assert applicability_relation(empty, root) == REL_DISJOINT
    assert applicability_relation(unknown, root) == REL_UNKNOWN


def test_half_open_time_intervals_separate_successive_claims() -> None:
    old = Applicability(valid_time=TimeBounds.known("2024-01-01", "2025-01-01"))
    new = Applicability(valid_time=TimeBounds.known("2025-01-01", "2026-01-01"))
    assert applicability_relation(old, new) == REL_DISJOINT


def test_exhaustive_finite_model_truth_table_and_symmetry() -> None:
    """Refines every finite shape/heading state in TypedApplicability.tla."""
    values = frozenset({"x", "y"})
    shape_bounds = {
        "unknown": (frozenset(), OMEGA),
        "unbounded": (OMEGA, OMEGA),
        "known_x": (frozenset({"x"}), frozenset({"x"})),
        "known_y": (frozenset({"y"}), frozenset({"y"})),
        "partial_x": (frozenset({"x"}), values),
        "partial_y": (frozenset({"y"}), values),
    }
    headings = {
        "absent": Applicability(),
        "unknown": Applicability.from_v1("", "unknown"),
        "empty": Applicability.from_v1("", "known"),
        "root": Applicability.from_v1("A", "known"),
        "child_a": Applicability.from_v1("A > B", "known"),
        "child_b": Applicability.from_v1("A > C", "known"),
    }

    def intersects(a: object, b: object) -> bool:
        if a is OMEGA and b is OMEGA:
            return True
        if a is OMEGA:
            return bool(b)
        if b is OMEGA:
            return bool(a)
        return bool(a & b)  # type: ignore[operator]

    def heading_overlaps(a: Applicability, b: Applicability) -> tuple[bool, bool]:
        if a.heading_state == b.heading_state == "absent":
            return True, True
        if "unknown" in (a.heading_state, b.heading_state):
            return False, True
        if "absent" in (a.heading_state, b.heading_state):
            return False, True
        pa, pb = a.heading_path, b.heading_path
        overlap = pa == pb or bool(
            pa and pb and (pa.startswith(pb + " > ") or pb.startswith(pa + " > "))
        )
        return overlap, overlap

    def bounds(s1: str, s2: str) -> ScopeBounds:
        lo1, hi1 = shape_bounds[s1]
        lo2, hi2 = shape_bounds[s2]
        return ScopeBounds(
            lo={"axis1": lo1, "axis2": lo2},
            hi={"axis1": hi1, "axis2": hi2},
        )

    shape_names = tuple(shape_bounds)
    for sa1, sb1, sa2, sb2, ha, hb in product(
        shape_names, shape_names, shape_names, shape_names, headings, headings
    ):
        heading_a, heading_b = headings[ha], headings[hb]
        a = Applicability(
            scope_bounds=bounds(sa1, sa2),
            heading_path=heading_a.heading_path,
            heading_state=heading_a.heading_state,
        )
        b = Applicability(
            scope_bounds=bounds(sb1, sb2),
            heading_path=heading_b.heading_path,
            heading_state=heading_b.heading_state,
        )
        alo1, ahi1 = shape_bounds[sa1]
        blo1, bhi1 = shape_bounds[sb1]
        alo2, ahi2 = shape_bounds[sa2]
        blo2, bhi2 = shape_bounds[sb2]
        heading_lo, heading_hi = heading_overlaps(heading_a, heading_b)
        lo_overlap = (
            intersects(alo1, blo1)
            and intersects(alo2, blo2)
            and heading_lo
        )
        hi_overlap = (
            intersects(ahi1, bhi1)
            and intersects(ahi2, bhi2)
            and heading_hi
        )
        expected = (
            REL_DISJOINT
            if not hi_overlap
            else REL_OVERLAP if lo_overlap else REL_UNKNOWN
        )
        relation = applicability_relation(a, b)
        assert relation == expected, (sa1, sb1, sa2, sb2, ha, hb)
        assert relation == applicability_relation(b, a)

    assert applicability_relation(
        Applicability(
            scope_bounds=bounds("unbounded", "unbounded"),
            heading_state="absent",
        ),
        Applicability(
            scope_bounds=bounds("known_x", "known_x"), heading_state="absent"
        ),
    ) == REL_OVERLAP
