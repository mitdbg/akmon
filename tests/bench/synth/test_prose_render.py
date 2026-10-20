"""Prose rendering, and the traceability that keeps it honest.

Making the corpus harder is only safe if the gold still points at the values.
The gold maps a corruption to a span; prose that stated a fact somewhere the
gold could not locate would turn a detectable corruption into an untraceable
one, shrink the recall denominator, and RAISE every score. A traceability
regression here looks like an improvement, which is why it is pinned rather
than assumed.
"""

from __future__ import annotations

import json

import pytest

from okg.bench.synth.config import load_config
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import RenderError, load_pack, render_corpus
from okg.bench.synth.runner import _packaged_world_dir, _subset_world


def _config(mode: str):
    return load_config({
        "seed": "prose-witness-1",
        "world_path": "world_smoke",
        "corruption": {"classes": {"value_perturbation": {"count": 2}}},
        "realism": {
            "render_mode": mode,
            "extractor": "llm" if mode == "prose" else "deterministic_labels",
        },
        "scale": {"patients": 2},
    })


def _render(mode: str):
    config = _config(mode)
    world_dir = _packaged_world_dir(config)
    prov = json.loads((world_dir / "provenance.json").read_text(encoding="utf-8"))
    world = world_from_synthea_dir(
        world_dir, reference_date=prov["reference_date"], provenance=prov,
        max_facts_per_patient=config.scale.max_facts_per_patient)
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    return config, render_corpus(world, config, planner, plan, prf)


def test_every_trace_row_points_at_its_value_in_prose() -> None:
    """THE traceability guarantee: the recorded span must contain the value,
    verbatim, in both the clean and corrupted texts."""
    _config_, rendered = _render("prose")
    assert rendered.trace, "no trace rows — nothing was rendered"

    for row in rendered.trace:
        clean = rendered.clean_docs[row.doc]
        corrupted = rendered.corrupted_docs[row.doc]
        assert clean[row.clean_start:row.clean_end], f"{row.fact_id}: empty span"
        assert corrupted[row.corrupted_start:row.corrupted_end] == row.surface_value, (
            f"{row.fact_id} in {row.doc}: recorded span "
            f"{corrupted[row.corrupted_start:row.corrupted_end]!r} does not hold "
            f"the surface value {row.surface_value!r}"
        )


def test_every_corruption_site_span_holds_its_post_text() -> None:
    """The gold's own spans, checked the same way. These are what the scorer
    keys on, so a drifted offset here mis-scores every repair."""
    _config_, rendered = _render("prose")
    assert rendered.sites, "no corruption sites — the check would be vacuous"

    for site in rendered.sites:
        text = rendered.corrupted_docs[site.doc]
        lo, hi = site.corrupted_span
        assert text[lo:hi] == site.post_text, (
            f"{site.corruption_id}: span {text[lo:hi]!r} != post_text "
            f"{site.post_text!r}"
        )
        clean = rendered.clean_docs[site.doc]
        clo, chi = site.clean_span
        assert clean[clo:chi] == site.pre_text


def test_prose_does_not_restate_the_label_verbatim() -> None:
    """Non-vacuity: if prose still emitted "Body Weight: 77.2 kg" it would be a
    structured line in a costume and would measure no added difficulty."""
    _config_, rendered = _render("prose")
    doc = next(iter(rendered.corrupted_docs.values()))
    body = "\n".join(
        line for line in doc.splitlines() if not line.startswith("#")
    )
    assert "Body Weight:" not in body
    assert "Weight at this visit was" in doc or "stands" in doc


def test_structured_mode_is_unchanged() -> None:
    """The LLM-free lane must render exactly as before."""
    _config_, rendered = _render("structured")
    doc = next(iter(rendered.corrupted_docs.values()))
    assert ": " in doc
    for row in rendered.trace:
        corrupted = rendered.corrupted_docs[row.doc]
        assert (
            corrupted[row.corrupted_start:row.corrupted_end] == row.surface_value
        )


def test_a_kind_without_a_prose_template_fails_loudly() -> None:
    """A missing template must not fall back to a structured line: the corpus
    would then mix rendering styles and the difficulty measurement would
    average two of them without saying so."""
    from okg.bench.synth.render import _prose_template

    pack = dict(load_pack("clinical_v1"))
    pack["prose"] = {"care_provider": "Care is coordinated by {value}."}

    class _F:
        kind = "observation:body_weight"
        key = "body_weight"

    with pytest.raises(RenderError, match="no prose template"):
        _prose_template(pack, _F())


def test_a_template_without_a_value_placeholder_fails_loudly() -> None:
    """Such a template would render a sentence with no value in it, and the
    fact would be silently untraceable."""
    from okg.bench.synth.render import _render_fact_line

    class _F:
        kind = "care_provider"
        key = "care_provider"

    pack = {"prose": {"care_provider": "Care is coordinated locally."}}
    with pytest.raises(RenderError, match="no .*value.* placeholder"):
        _render_fact_line(pack, _F(), "Care Provider", "Dr Who", "prose")
