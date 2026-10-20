"""task.synth.render witnesses: trace completeness, one-per-doc invariant,
markdown fidelity, benign variation, byte determinism, typst PDF lane."""

from __future__ import annotations

import hashlib

import pytest

from okg.bench.synth.corruption import Planner
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import RenderError, load_pack, render_corpus

# the fidelity witness re-extracts via the substrate claim grammar — tests
# are substrate-side, the core is not.
from okg.bench.injection.claim_corpus_source import (
    parse_document_claims,
)


@pytest.fixture(scope="module")
def rendered(world, smoke_config):
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    return render_corpus(world, smoke_config, planner, plan, prf)


def test_trace_completeness(rendered):
    counts: dict[str, int] = {}
    for row in rendered.trace:
        counts[row.fact_id] = counts.get(row.fact_id, 0) + 1
    for fact_id, k in rendered.effective_k.items():
        assert counts.get(fact_id, 0) == k, f"{fact_id}: {counts.get(fact_id)} != {k}"


def test_no_fact_renders_twice_in_one_document(rendered):
    seen = set()
    for row in rendered.trace:
        key = (row.fact_id, row.doc)
        assert key not in seen, f"{row.fact_id} rendered twice in {row.doc}"
        seen.add(key)


def test_trace_spans_are_byte_accurate(rendered):
    for row in rendered.trace:
        corrupted_doc = rendered.corrupted_docs[row.doc]
        assert (
            corrupted_doc[row.corrupted_start : row.corrupted_end] == row.surface_value
        )


def test_markdown_fidelity_unvaried_sites(world, rendered):
    """Every unvaried, uncorrupted rendering re-extracts to exactly the
    ground-truth value (+ canonical unit); variation sites compare under
    the variation's declared equivalence (unit rescale)."""
    facts = {f.fact_id: f for f in world.facts}
    checked = 0
    for row in rendered.trace:
        fact = facts[row.fact_id]
        clean_doc = rendered.clean_docs[row.doc]
        surface = clean_doc[row.clean_start : row.clean_end]
        if row.variation == "":
            expected = f"{fact.value} {fact.unit}".strip()
            assert surface == expected, (
                f"fidelity: {row.fact_id} in {row.doc} rendered {surface!r}, "
                f"ground truth {expected!r}"
            )
            checked += 1
        elif row.variation == "unit_equivalent":
            value = float(fact.value)
            if fact.unit == "kg":
                assert surface == f"{value * 1000:.0f} g"
            elif fact.unit == "cm":
                assert surface == f"{value * 10:.0f} mm"
            checked += 1
    assert checked > 100, "fidelity witness must cover the corpus"


def test_extraction_grammar_parses_every_document(rendered):
    for doc, text in rendered.clean_docs.items():
        claims = list(parse_document_claims(doc, text))
        assert claims, f"{doc} yielded no claims"
        entities = {c.attrs["normalized_key"] for c in claims if c.attrs.get("predicate") != "step"}
        assert len(entities) == 1, f"{doc} must key all claims to the H1 patient entity"


def test_benign_variation_rate_and_labeling(rendered, smoke_config):
    varied = [t for t in rendered.trace if t.variation]
    assert varied, "smoke preset variation rate must produce labeled sites"
    for row in varied:
        assert row.variation == "unit_equivalent"
        assert not row.corrupted, "variation must never coincide with corruption"


def test_double_render_byte_identical(world, smoke_config):
    def _render():
        prf = SynthPrf(smoke_config.seed)
        planner = Planner(world, smoke_config, prf)
        plan = planner.plan()
        result = render_corpus(world, smoke_config, planner, plan, prf)
        return hashlib.sha256(
            "".join(
                f"{d}\x00{result.clean_docs[d]}\x00{result.corrupted_docs[d]}"
                for d in sorted(result.clean_docs)
            ).encode("utf-8")
        ).hexdigest()

    assert _render() == _render()


def _label_orders(result) -> dict[str, tuple[str, ...]]:
    """Per-document claim-label order, the thing slot shuffling permutes."""
    return {
        doc: tuple(
            line.split(":")[0]
            for line in result.clean_docs[doc].splitlines()
            if ": " in line and not line.startswith("#")
        )
        for doc in sorted(result.clean_docs)
    }


def test_slot_shuffle_varies_line_order(world, smoke_config, rendered):
    """Slot shuffling permutes claim lines WITHIN a document relative to the
    unshuffled baseline, while the set of labels a document carries is
    unchanged (labels stable, positions not).

    Counting distinct orders ACROSS documents proves nothing: different
    patients carry different facts, so the corpus shows ~29 distinct orders
    over 32 documents even with ``slot_shuffle=False``. The witness has to
    hold the document fixed and vary the knob.
    """
    baseline_config = smoke_config.model_copy(
        update={"realism": smoke_config.realism.model_copy(update={"slot_shuffle": False})}
    )
    prf = SynthPrf(baseline_config.seed)
    planner = Planner(world, baseline_config, prf)
    baseline = render_corpus(
        world, baseline_config, planner, planner.plan(), prf
    )

    shuffled_orders = _label_orders(rendered)
    baseline_orders = _label_orders(baseline)
    assert set(shuffled_orders) == set(baseline_orders)

    permuted = [d for d in baseline_orders if shuffled_orders[d] != baseline_orders[d]]
    assert len(permuted) > len(baseline_orders) // 2, (
        "slot shuffle must reorder claim lines within most documents; "
        f"only {len(permuted)}/{len(baseline_orders)} differ from the "
        "unshuffled baseline"
    )
    for doc, order in baseline_orders.items():
        assert sorted(shuffled_orders[doc]) == sorted(order), (
            f"{doc}: slot shuffle changed WHICH labels render, not just their order"
        )


def test_unknown_genre_rejected(world, smoke_config):
    from okg.bench.synth.config import load_config

    bad = load_config(
        {
            "seed": "x",
            "world_path": "world_smoke",
            "realism": {"genres": ["discharge_summary", "coronation_scroll"]},
        }
    )
    prf = SynthPrf(bad.seed)
    planner = Planner(world, bad, prf)
    with pytest.raises(RenderError, match="coronation_scroll"):
        render_corpus(world, bad, planner, planner.plan(), prf)


def test_pack_loads_and_covers_world_kinds(world):
    pack = load_pack()
    from okg.bench.synth.render import _label_for

    for fact in world.facts:
        label = _label_for(pack, fact)
        assert label and label[0].isupper()


# -- PDF lane --------------------------------------------------------------


def test_pdf_lane(rendered, smoke_config, tmp_path):
    """Deterministic typst PDF + text-layer round-trip. Skips VISIBLY (with
    the named reason) when typst is absent — never silently passes."""
    from okg.bench.synth.pdfgen import PdfUnavailable, materialize_pdfs

    sample = dict(sorted(rendered.corrupted_docs.items())[:2])
    try:
        first = materialize_pdfs(sample, tmp_path / "a", seed=smoke_config.seed)
        second = materialize_pdfs(sample, tmp_path / "b", seed=smoke_config.seed)
    except PdfUnavailable as exc:
        pytest.skip(f"PDF lane skipped: {exc}")

    for doc in sample:
        assert (
            hashlib.sha256(first[doc].read_bytes()).hexdigest()
            == hashlib.sha256(second[doc].read_bytes()).hexdigest()
        ), f"typst output for {doc} is not byte-deterministic"

    from pypdf import PdfReader

    for doc, pdf_path in first.items():
        text = "\n".join(page.extract_text() for page in PdfReader(pdf_path).pages)
        markdown = sample[doc]
        claim_lines = [
            line for line in markdown.splitlines() if ": " in line and not line.startswith("#")
        ]
        assert claim_lines
        recovered = sum(
            1
            for line in claim_lines
            if line.split(": ", 1)[1].strip() in text.replace("\n", " ")
        )
        assert recovered / len(claim_lines) > 0.9, (
            f"{doc}: PDF text layer recovered only {recovered}/{len(claim_lines)} claims"
        )
