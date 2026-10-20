"""PACT ``concord-failure-taxonomy`` — req.tax.taxonomy, req.tax.evaluation.

Each witness drives one property of the taxonomy against a deliberately holed
input and shows the property FIRE. A taxonomy whose rules are never shown to
discriminate, and an evaluation never shown able to fail, would let any
partition of the errors pass as a diagnosis.
"""
from __future__ import annotations

import pytest

from okg.bench.synth.failure_taxonomy import (
    CATEGORIES,
    EVALUATION_CRITERIA,
    UNCLASSIFIED,
    FailureCategory,
    classify_row,
    classify_rows,
    evaluate_taxonomy,
)
from okg.bench.synth.resolution import FUNNEL_STATES


def _row(state, corrupted, clean, other=0, **extra):
    return {
        "corruption_id": extra.pop("cid", f"corr:{state}:{corrupted}-{clean}"),
        "state": state,
        "realized_vote": {"corrupted": corrupted, "clean": clean, "other": other},
        "partition": extra.pop("partition", "cross_doc"),
        "constraint_kind": extra.pop("constraint_kind", "functional"),
        "corruption_class": extra.pop("corruption_class", "value_perturbation"),
        "layer": extra.pop("layer", "kg_fork"),
        **extra,
    }


# --------------------------------------------------------------------------- #
# The taxonomy REFINES the funnel — it never moves an error between states.    #
# --------------------------------------------------------------------------- #
def test_every_category_declares_a_real_parent_state() -> None:
    for c in CATEGORIES:
        assert c.parent_state in FUNNEL_STATES, c
        assert c.remedy.strip(), f"{c.name} implies no action"
        assert c.reads, f"{c.name} declares no evidence"


def test_every_terminal_state_has_at_least_one_category() -> None:
    """A state with no category would silently vanish from the refinement."""
    covered = {c.parent_state for c in CATEGORIES}
    assert covered == set(FUNNEL_STATES), sorted(set(FUNNEL_STATES) - covered)


def test_a_category_never_matches_a_row_in_another_state() -> None:
    for c in CATEGORIES:
        for state in FUNNEL_STATES:
            if state == c.parent_state:
                continue
            assert not c.matches(_row(state, 1, 2)), (c.name, state)


# --------------------------------------------------------------------------- #
# Each rule fires, and stops firing when ONE field moves.                      #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "category,fires,does_not",
    [
        ("no_contradiction_available", _row("not_detected", 1, 0),
         _row("not_detected", 1, 1)),
        ("contradiction_unflagged", _row("not_detected", 1, 1),
         _row("not_detected", 1, 0)),
        ("tie_abstained", _row("detected_only", 1, 1),
         _row("detected_only", 1, 2)),
        ("no_repair_path", _row("detected_only", 1, 2),
         _row("detected_only", 1, 1)),
        ("majority_wrong", _row("mis_repaired", 2, 1),
         _row("mis_repaired", 1, 2)),
        ("tie_broken_wrong", _row("mis_repaired", 1, 1),
         _row("mis_repaired", 2, 1)),
        ("minority_chosen", _row("mis_repaired", 1, 2),
         _row("mis_repaired", 2, 1)),
        ("unopposed_wrong", _row("mis_repaired", 1, 0),
         _row("mis_repaired", 1, 1)),
        ("repaired_by_majority", _row("repaired", 1, 2),
         _row("repaired", 2, 1)),
        ("repaired_against_majority", _row("repaired", 2, 1),
         _row("repaired", 1, 2)),
    ],
)
def test_each_rule_fires_and_a_one_field_change_stops_it(
    category, fires, does_not
) -> None:
    assert classify_row(fires)[0] == category, classify_row(fires)
    assert classify_row(does_not)[0] != category, classify_row(does_not)


def test_no_row_matches_two_categories() -> None:
    """Overlapping rules would make declaration order the real classifier."""
    for state in FUNNEL_STATES:
        for bad, good in ((1, 0), (1, 1), (1, 2), (2, 1), (3, 1), (0, 2)):
            _cat, hits = classify_row(_row(state, bad, good))
            assert len(hits) <= 1, (state, bad, good, hits)


def test_an_unmatched_row_is_unclassified_with_its_evidence() -> None:
    """NOT nearest-fitted. The unclassified count is the only signal that the
    taxonomy is incomplete, so it must survive."""
    orphan = _row("invented_state", 1, 1)
    category, hits = classify_row(orphan)
    assert category is UNCLASSIFIED and hits == []
    out = classify_rows([orphan])[0]
    assert out["failure_category"] is UNCLASSIFIED
    assert out["classification_evidence"]["realized_vote"] == orphan["realized_vote"]
    assert out["classification_evidence"]["state"] == "invented_state"


# --------------------------------------------------------------------------- #
# The evaluation must be able to FAIL.                                         #
# --------------------------------------------------------------------------- #
def test_the_five_criteria_are_pinned() -> None:
    assert EVALUATION_CRITERIA == (
        "total", "non_vacuous", "discriminating", "stable", "actionable"
    )


def test_evaluation_reports_a_healthy_corpus_cleanly() -> None:
    rows = classify_rows([
        _row("repaired", 1, 2, cid="a"), _row("detected_only", 1, 1, cid="b"),
        _row("mis_repaired", 2, 1, cid="c"), _row("not_detected", 1, 0, cid="d"),
    ])
    ev = evaluate_taxonomy(rows)
    assert ev["total"]["unclassified"] == 0 and ev["total"]["rate"] == 1.0
    assert ev["discriminating"]["passed"] and ev["actionable"]["passed"]
    assert set(ev["non_vacuous"]["observed"]) == {
        "repaired_by_majority", "tie_abstained", "majority_wrong",
        "no_contradiction_available",
    }
    assert ev["non_vacuous"]["unobserved"], "unobserved categories must be listed"


def test_evaluation_reports_an_alias_pair_rather_than_passing_it() -> None:
    """ACCEPTANCE (able to fail): two categories selecting the SAME errors are
    not a distinction, and the grader must say so."""
    twin = FailureCategory(
        "repaired_by_majority_twin", "repaired", ("state", "realized_vote"),
        "a deliberately duplicated remedy", lambda row: row["realized_vote"]["clean"]
        > row["realized_vote"]["corrupted"])
    cats = CATEGORIES + (twin,)
    rows = [{**_row("repaired", 1, 2, cid="a"), "failure_category": "repaired_by_majority"},
            {**_row("repaired", 1, 2, cid="a"), "failure_category": "repaired_by_majority_twin"}]
    ev = evaluate_taxonomy(rows, categories=cats)
    assert ev["discriminating"]["passed"] is False
    assert ["repaired_by_majority", "repaired_by_majority_twin"] in ev["discriminating"]["alias_pairs"]


def test_evaluation_reports_an_unstable_signature() -> None:
    """One signature mapping to two categories means the taxonomy is reading
    something the signature does not capture."""
    a = classify_rows([_row("repaired", 1, 2, cid="x")])[0]
    b = dict(a)
    b["failure_category"] = "tie_abstained"
    b["corruption_id"] = "y"
    ev = evaluate_taxonomy([a, b])
    assert ev["stable"]["passed"] is False and ev["stable"]["split_signatures"]
    assert ev["stable"]["rate"] == 0.0


def test_evaluation_reports_a_collapse_candidate() -> None:
    """Two observed categories naming the SAME remedy are one category."""
    twin = FailureCategory(
        "echo", "repaired", ("state",),
        next(c.remedy for c in CATEGORIES if c.name == "repaired_by_majority"),
        lambda row: False)
    rows = [{**_row("repaired", 1, 2, cid="a"), "failure_category": "repaired_by_majority"},
            {**_row("repaired", 1, 2, cid="b"), "failure_category": "echo"}]
    ev = evaluate_taxonomy(rows, categories=CATEGORIES + (twin,))
    assert ev["actionable"]["passed"] is False
    assert ["echo", "repaired_by_majority"] in ev["actionable"]["collapse_candidates"]
