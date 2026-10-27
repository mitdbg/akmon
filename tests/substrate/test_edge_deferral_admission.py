"""Admission of a cross-source edge whose endpoint is not there yet.

Source B writes ``note references widget``; the widget is another source's
record. These cases pin which edges wait (design.md 3.1) and which keep
today's refusal. The database cases are in
``tests/substrate/ingest/test_cross_source_endpoint_wedge_db.py``.
"""
from __future__ import annotations

import pytest

from okg.substrate.graph_store_internal.edge_deferrals import DeferralPolicy
from okg.substrate.producer_authority import (
    DEFERRED_EDGES_OVER_LIMIT_CODE,
    ProducerPolicyViolation,
    ProducerProfile,
    _evaluate_records,
    classify_edge_deferral,
    deferral_rules_for_profile,
)

_ON = DeferralPolicy(
    enabled=True, max_per_run=1000, max_fraction=1.0, cap_mode="absolute",
)

_EDGE = {
    "src_subtype": "note",
    "edge_type": "references",
    "dst_subtype": "widget",
    "clause_id": "note_widget",
}


def _profile(
    *,
    edges: list[dict] | None = None,
    nodes: list[dict] | None = None,
    unbounded: list[str] | None = None,
) -> ProducerProfile:
    signature: dict = {
        "nodes": nodes if nodes is not None else [{"subtype": "note"}],
        "edges": edges if edges is not None else [dict(_EDGE)],
    }
    return ProducerProfile(
        deployment_name="fixture",
        producer_id="fixture.b",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=("node", "edge"),
        output_signature=signature,
        policy_clauses=(),
        output_scope_summary=(
            {"write_envelope": {"unbounded": unbounded}} if unbounded else {}
        ),
        source_ref={},
        catalog_version_id=1,
        profile_fingerprint="sha256:b",
        status="active",
    )


def _note(node_id: str = "note:1") -> dict:
    return {"node_id": node_id, "subtype": "note", "attrs": {}, "op": "I"}


def _edge(
    dst: str = "widget:1",
    *,
    dst_subtype: str | None = None,
    src: str = "note:1",
    edge_type: str = "references",
) -> dict:
    return {
        "src": src, "src_subtype": "note", "edge_type": edge_type,
        "dst": dst, "dst_subtype": dst_subtype, "attrs": {}, "op": "I",
    }


def _evaluate(
    profile, edges, *, policy=_ON, ancestors=None, owner_published=None,
):
    return _evaluate_records(
        profile=profile,
        node_records=[_note()],
        edge_records=edges,
        endpoint_scope="test fixture",
        ancestors=ancestors,
        defer_policy=policy,
        owner_published=owner_published,
    )


def test_missing_endpoint_of_a_foreign_type_is_deferred() -> None:
    evidence = _evaluate(_profile(), [_edge()])

    deferred = evidence["producer_policy"]["deferred_edges"]
    assert deferred["count"] == 1
    assert deferred["by_state"] == {"waiting": 1}
    assert deferred["sample"][0]["dst"] == "widget:1"
    assert evidence["producer_policy"]["status"] == "matched"


def test_a_resolved_endpoint_is_matched_not_deferred() -> None:
    evidence = _evaluate(_profile(), [_edge(dst_subtype="widget")])

    assert "deferred_edges" not in evidence["producer_policy"]


def test_mismatched_endpoint_subtype_is_held() -> None:
    evidence = _evaluate(_profile(), [_edge(dst_subtype="gadget")])

    assert evidence["producer_policy"]["deferred_edges"]["by_state"] == {
        "subtype_mismatch": 1,
    }


def test_several_clauses_must_all_qualify() -> None:
    both = [
        dict(_EDGE),
        {**_EDGE, "dst_subtype": "gadget", "clause_id": "note_gadget"},
    ]
    assert _evaluate(_profile(edges=both), [_edge()])[
        "producer_policy"]["deferred_edges"]["count"] == 1

    # The second clause names a subtype the producer emits itself: an own gap.
    own = [dict(_EDGE), {**_EDGE, "dst_subtype": "note", "clause_id": "n"}]
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(_profile(edges=own), [_edge()])
    assert refused.value.code == "producer_edge_endpoint_missing"


def test_a_parent_type_clause_is_read_with_is_a() -> None:
    ancestors = {
        "widget": {"widget", "artifact"},
        "note": {"note"},
        "artifact": {"artifact"},
    }
    parent = [{**_EDGE, "dst_subtype": "artifact"}]
    # The producer emits no artifact or descendant of one: deferred.
    assert _evaluate(_profile(edges=parent), [_edge()], ancestors=ancestors)[
        "producer_policy"]["deferred_edges"]["count"] == 1

    # The producer emits a widget, a descendant of artifact: its own gap.
    own = _profile(
        edges=parent,
        nodes=[{"subtype": "note"}, {"subtype": "widget"}],
    )
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(own, [_edge()], ancestors=ancestors)
    assert refused.value.code == "producer_edge_endpoint_missing"


def test_own_subtype_gap_is_refused() -> None:
    own = _profile(edges=[{**_EDGE, "dst_subtype": "note"}])
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(own, [_edge()])
    assert refused.value.code == "producer_edge_endpoint_missing"


def test_endpoint_absent_refuse_keeps_the_refusal() -> None:
    strict = _profile(edges=[{**_EDGE, "endpoint_absent": "refuse"}])
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(strict, [_edge()])
    assert refused.value.code == "producer_edge_endpoint_missing"


def test_undeclared_edge_type_is_refused() -> None:
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(_profile(), [_edge(edge_type="mentions")])
    assert refused.value.code == "producer_edge_triple_not_allowed"


def test_unbounded_edges_and_no_edge_signature_are_unchanged() -> None:
    assert classify_edge_deferral(
        _edge(), deferral_rules_for_profile(
            _profile(unbounded=["edges"]), ancestors=None,
        ),
    ) is None
    assert not deferral_rules_for_profile(
        _profile(edges=[]), ancestors=None,
    ).deferrable


def test_ranked_edge_type_is_not_deferred() -> None:
    rules = deferral_rules_for_profile(
        _profile(), ancestors=None, ranked_types=frozenset({"references"}),
    )
    assert classify_edge_deferral(_edge(), rules) is None


def test_disabled_policy_keeps_todays_refusal() -> None:
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(_profile(), [_edge()], policy=DeferralPolicy(enabled=False))
    assert refused.value.code == "producer_edge_endpoint_missing"


def test_per_run_cap_refuses_above_the_limit_and_accepts_at_it() -> None:
    policy = DeferralPolicy(
        enabled=True, max_per_run=2, max_fraction=1.0, cap_mode="absolute",
    )
    edges = [_edge(f"widget:{i}") for i in range(3)]

    at_limit = _evaluate(_profile(), edges[:2], policy=policy)
    assert at_limit["producer_policy"]["deferred_edges"]["count"] == 2

    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(_profile(), edges, policy=policy)
    assert refused.value.code == DEFERRED_EDGES_OVER_LIMIT_CODE
    assert refused.value.verdict["offending_record_count"] == 3


def test_fraction_cap_is_the_smaller_of_the_two_limits() -> None:
    policy = DeferralPolicy(
        enabled=True, max_per_run=1000, max_fraction=0.2, cap_mode="fraction",
    )
    assert policy.limit_for(10) == 2
    assert policy.limit_for(100_000) == 1000
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(_profile(), [_edge("widget:1")], policy=policy)
    assert refused.value.code == DEFERRED_EDGES_OVER_LIMIT_CODE


def test_owner_published_counts_only_deferrals_whose_owner_has_published() -> None:
    policy = DeferralPolicy(
        enabled=True, max_per_run=2, max_fraction=0.2,
        cap_mode="owner_published",
    )
    edges = [_edge(f"widget:{i}") for i in range(5)]

    # The owner of widget has never published: run order, nothing counts.
    quiet = _evaluate(
        _profile(), edges, policy=policy, owner_published=lambda _s: False,
    )
    assert quiet["producer_policy"]["deferred_edges"]["count"] == 5

    # The owner has published and still lacks them: above the cap, refused.
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(
            _profile(), edges, policy=policy,
            owner_published=lambda _s: True,
        )
    assert refused.value.code == DEFERRED_EDGES_OVER_LIMIT_CODE
    assert refused.value.verdict["offending_record_count"] == 5

    # At the cap it is accepted.
    at = _evaluate(
        _profile(), edges[:2], policy=policy, owner_published=lambda _s: True,
    )
    assert at["producer_policy"]["deferred_edges"]["count"] == 2


def test_absolute_mode_ignores_the_fraction() -> None:
    policy = DeferralPolicy(
        enabled=True, max_per_run=1000, max_fraction=0.2, cap_mode="absolute",
    )
    assert policy.limit_for(10) == 1000
    assert _evaluate(_profile(), [_edge()], policy=policy)[
        "producer_policy"]["deferred_edges"]["count"] == 1


def test_cap_mode_setting_is_validated(tmp_path) -> None:
    from okg.substrate.publisher.deployment_stack import load_deferral_policy

    def policy_for(block: dict):
        (tmp_path / "deployment.yaml").write_text(
            "name: x\nconsistency: " + repr(block) + "\n",
        )
        return load_deferral_policy(tmp_path)

    assert policy_for({}).cap_mode == "owner_published"
    assert policy_for({"deferred_cap_mode": "fraction"}).cap_mode == "fraction"
    with pytest.raises(ValueError, match="deferred_cap_mode"):
        policy_for({"deferred_cap_mode": "bogus"})
