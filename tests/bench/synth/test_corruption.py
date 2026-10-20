"""task.synth.corruption witnesses: per-class realization, bidirectional
partition assignment, majority-wrong, twins, fail-closed eligibility."""

from __future__ import annotations

import pytest

from okg.bench.synth.config import load_config
from okg.bench.synth.corruption import (
    LAYER_KG_FORK,
    LAYER_RENDER,
    PARTITION_CROSS_DOC,
    PARTITION_INTERNAL_ONLY,
    PARTITION_UNDETECTABLE,
    PARTITION_WORLD_KNOWLEDGE,
    CorruptionError,
    Planner,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus

EXPECTED_LAYER = {
    "value_perturbation": LAYER_KG_FORK,
    "entity_swap": LAYER_KG_FORK,
    "cardinality_violation": LAYER_KG_FORK,
    "polarity_flip": LAYER_KG_FORK,
    "derived_value": LAYER_KG_FORK,
    "digit_transposition": LAYER_RENDER,
    "unit_conversion_error": LAYER_RENDER,
    "label_value_mismatch": LAYER_RENDER,
    "broken_reference": LAYER_RENDER,
    "dangling_entity": LAYER_RENDER,
    # Σ-coverage classes (synth-constraint-coverage): each is a KG-fork belief
    # change that trips one relational constraint kind.
    "incompatible_status_pair": LAYER_KG_FORK,
    "linked_reference": LAYER_KG_FORK,
    "ambiguous_subject": LAYER_KG_FORK,
}


def _config(classes: dict, k=(1, 3), twin_rate=0.0, genres=None):
    body = {
        "seed": "corr-witness-1",
        "world_path": "world_smoke",
        "redundancy": {
            "renderings_per_fact_min": k[0],
            "renderings_per_fact_max": k[1],
        },
        "corruption": {"classes": classes},
        "benign_variation": {"unit_rescale_rate": twin_rate},
    }
    if genres is not None:
        body["realism"] = {"genres": list(genres)}
    return load_config(body)


def _plan(world, config):
    planner = Planner(world, config, SynthPrf(config.seed))
    return planner, planner.plan()


# -- per-class witnesses ---------------------------------------------------


@pytest.mark.parametrize("cls", sorted(EXPECTED_LAYER))
def test_class_realizes_at_declared_layer(world, cls):
    planner, plan = _plan(world, _config({cls: {"count": 1}}))
    rows = [r for r in plan.rows if r.corruption_class == cls]
    assert len(rows) == 1
    row = rows[0]
    assert row.layer == EXPECTED_LAYER[cls]
    # Asserting membership in the COMPLETE partition vocabulary passes for any
    # assignment whatsoever. The falsifiable statement is what the label MEANS:
    # a cross_doc site keeps a clean counterpart (the corruption occupies fewer
    # than all of the fact's renderings), while the counterpart-free partitions
    # require it to occupy all of them. `world_knowledge` is the out-of-range
    # escape and is orthogonal to redundancy.
    assert row.partition in {
        PARTITION_CROSS_DOC,
        PARTITION_INTERNAL_ONLY,
        PARTITION_WORLD_KNOWLEDGE,
        PARTITION_UNDETECTABLE,
    }
    k = planner._k[row.fact_id]
    if row.partition == PARTITION_CROSS_DOC:
        assert row.visibility_corrupted < k, (
            f"{cls}: cross_doc claims a surviving clean counterpart, but the "
            f"corruption occupies {row.visibility_corrupted} of {k} renderings"
        )
    elif row.partition in (PARTITION_INTERNAL_ONLY, PARTITION_UNDETECTABLE):
        assert row.visibility_corrupted >= k, (
            f"{cls}: {row.partition} claims no clean counterpart, but the "
            f"corruption occupies only {row.visibility_corrupted} of {k} "
            "renderings — the site is cross-document detectable"
        )
    if cls not in ("broken_reference",):
        assert row.clean_value != row.corrupted_value


def test_registry_is_complete():
    assert set(Planner.REGISTRY) == set(EXPECTED_LAYER)


# -- partition assignment: bidirectional -----------------------------------


def test_implausible_is_world_knowledge_never_cross_doc(world):
    config = _config(
        {"value_perturbation": {"count": 2, "allow_implausible": True}}, k=(3, 3)
    )
    _, plan = _plan(world, config)
    for row in plan.rows:
        assert row.partition == PARTITION_WORLD_KNOWLEDGE
        lo, hi = (float(x) for x in row.params["range"].split(".."))
        assert not (lo <= float(row.corrupted_value) <= hi)


def test_cross_doc_rows_are_in_range_with_other_doc_counterpart(world):
    """The converse direction: every cross_doc row is in-range AND, after
    rendering, has a clean counterpart in a DIFFERENT document."""
    config = _config(
        {
            "value_perturbation": {"count": 2, "visibility_corrupted": 1},
            "entity_swap": {"count": 1},
            "polarity_flip": {"count": 1},
        },
        k=(3, 3),
    )
    planner, plan = _plan(world, config)
    prf = SynthPrf(config.seed)
    rendered = render_corpus(world, config, planner, plan, prf)
    cross_rows = [
        r for r in plan.rows if rendered.finalized_partitions[r.corruption_id] == PARTITION_CROSS_DOC
    ]
    assert cross_rows, "witness needs at least one cross_doc corruption"
    site_docs = {}
    for site in rendered.sites:
        site_docs.setdefault(site.corruption_id, set()).add(site.doc)
    for row in cross_rows:
        if "range" in row.params:
            lo, hi = (float(x) for x in row.params["range"].split(".."))
            assert lo <= float(row.corrupted_value) <= hi
        corrupted_docs = site_docs[row.corruption_id]
        clean_docs = {
            t.doc
            for t in rendered.trace
            if t.fact_id == row.fact_id and not t.corrupted
        }
        assert clean_docs, "cross_doc requires a clean counterpart"
        assert clean_docs.isdisjoint(corrupted_docs) or (clean_docs - corrupted_docs), (
            "counterpart must live in a different document"
        )


def test_fully_visible_in_range_value_is_undetectable_not_internal(world):
    config = _config(
        {"value_perturbation": {"count": 1, "visibility_corrupted": 3}}, k=(3, 3)
    )
    _, plan = _plan(world, config)
    assert plan.rows[0].partition == PARTITION_UNDETECTABLE


def test_backup_check_classes_stay_internal_when_fully_visible(world):
    config = _config(
        {"cardinality_violation": {"count": 1, "visibility_corrupted": 3}}, k=(3, 3)
    )
    _, plan = _plan(world, config)
    assert plan.rows[0].partition == PARTITION_INTERNAL_ONLY


def test_unrelated_entity_swap_is_inexpressible(world):
    """v1 has no cross-category formulary: labeling a plausible sibling swap
    world_knowledge would misstate detectability (review finding M5)."""
    config = _config({"entity_swap": {"count": 1, "swap_distance": "unrelated"}})
    planner = Planner(world, config, SynthPrf(config.seed))
    with pytest.raises(CorruptionError, match="unrelated"):
        planner.plan()


# -- majority-wrong --------------------------------------------------------


def test_majority_wrong_scenario(world):
    """visibility 2 of k=3: the corrupted value is the majority; gold still
    calls it cross_doc (one clean counterpart survives). Uses care_provider,
    the fact kind eligible in all three genres."""
    config = _config(
        {"cardinality_violation": {"count": 1, "visibility_corrupted": 2}}, k=(3, 3)
    )
    planner, plan = _plan(world, config)
    row = plan.rows[0]
    assert row.partition == PARTITION_CROSS_DOC
    prf = SynthPrf(config.seed)
    rendered = render_corpus(world, config, planner, plan, prf)
    assert rendered.finalized_partitions[row.corruption_id] == PARTITION_CROSS_DOC
    corrupted = [t for t in rendered.trace if t.fact_id == row.fact_id and t.corrupted]
    clean = [t for t in rendered.trace if t.fact_id == row.fact_id and not t.corrupted]
    assert len(corrupted) == 2 and len(clean) == 1, (
        "majority of renderings must show the corrupted value"
    )


def test_pack_inexpressible_majority_wrong_fails_closed(world):
    """A majority-wrong request on a fact kind capped below the needed
    redundancy must FAIL CLOSED, never silently demote every site (review
    finding M3).

    The cap is now imposed DELIBERATELY by restricting the genre set, rather
    than relying on the pack. Observations used to be eligible in exactly two
    genres, which made this case inexpressible by accident; the
    referral_letter genre lifted them to three, so the original fixture stopped
    exercising the guard at all. Selecting two genres reproduces the condition
    on purpose and keeps the witness meaningful however the pack evolves.
    """
    from okg.bench.synth.render import RenderError

    config = _config(
        {"value_perturbation": {"count": 1, "visibility_corrupted": 2}},
        k=(3, 3),
        genres=("discharge_summary", "clinic_note"),
    )
    planner, plan = _plan(world, config)
    assert plan.rows[0].partition == PARTITION_CROSS_DOC  # planned against k=3
    prf = SynthPrf(config.seed)
    with pytest.raises(RenderError, match="majority-wrong"):
        render_corpus(world, config, planner, plan, prf)


# -- twins -----------------------------------------------------------------


def test_twins_derived_from_rendered_variation(world):
    """Twins are the renderer's realized variation sites (the wired
    benign_variation.unit_rescale_rate knob - review finding H2), never
    corrupted facts."""
    config = _config({"value_perturbation": {"count": 1}}, k=(2, 3), twin_rate=1.0)
    planner = Planner(world, config, SynthPrf(config.seed))
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, SynthPrf(config.seed))
    assert rendered.twins, "unit_rescale_rate 1.0 must realize twins"
    corrupted_facts = {r.fact_id for r in plan.rows}
    varied_facts = {t.fact_id for t in rendered.twins}
    for twin in rendered.twins:
        assert twin.family == "unit_equivalent"
        assert twin.fact_id not in corrupted_facts, "twins are must-NOT-flag sites"
    trace_varied = {t.fact_id for t in rendered.trace if t.variation}
    assert varied_facts == trace_varied, "twin gold must equal realized variation"


# -- fail-closed eligibility ----------------------------------------------


def test_unknown_class_raises(world):
    config = _config({})
    planner = Planner(world, config, SynthPrf(config.seed))
    # bypass config validation deliberately: the planner must ALSO refuse.
    from okg.bench.synth.config import ClassSpec

    object.__setattr__(
        config.corruption, "classes", {"time_travel_paradox": ClassSpec(count=1)}
    )
    with pytest.raises(CorruptionError, match="unknown corruption class"):
        planner.plan()


def test_one_corruption_per_fact(world):
    config = _config(
        {
            "value_perturbation": {"count": 3},
            "digit_transposition": {"count": 3},
            "label_value_mismatch": {"count": 2},
        }
    )
    _, plan = _plan(world, config)
    fact_ids = [r.fact_id for r in plan.rows]
    assert len(fact_ids) == len(set(fact_ids))
