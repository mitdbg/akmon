"""The renderer mode and its one incompatible pairing.

Prose cannot simply replace structured rendering, because the two lanes read
documents with different extractors. The LLM-free lane uses the deterministic
label parser (claim_corpus_source.parse_document_claims), which walks a
document line by line matching "Label: value". A narrative corpus has no such
line, so that lane would extract ZERO claims -- and the damage is not a crash
but a SILENCE: no claims means no buckets, no findings, and every recall metric
reports a clean-looking zero denominator while the run completes.

So the pairing is rejected at config load, and these witnesses pin that the
guard fires rather than merely exists.
"""

from __future__ import annotations

import pytest

from okg.bench.synth.config import ConfigError, load_config, load_preset


def _cfg(**realism):
    return load_config({
        "seed": "mode-witness-1",
        "world_path": "world_smoke",
        "corruption": {"classes": {"value_perturbation": {"count": 1}}},
        "realism": realism,
    })


def test_structured_is_the_default() -> None:
    """The LLM-free lane must keep working untouched by this change."""
    cfg = _cfg()
    assert cfg.realism.render_mode == "structured"
    assert cfg.realism.extractor == "deterministic_labels"


def test_smoke_preset_stays_structured() -> None:
    """CI's lane is LLM-free and reproducible; it must not silently acquire a
    corpus its extractor cannot read."""
    assert load_preset("smoke").realism.render_mode == "structured"


def test_prose_with_the_label_parser_is_rejected_at_load() -> None:
    """The guard must FIRE. This pairing produces a corpus that extracts
    nothing, and a zero denominator reads as 'no corruptions to find' rather
    than as a broken run."""
    with pytest.raises(ConfigError, match="incompatible with extractor"):
        _cfg(render_mode="prose", extractor="deterministic_labels")


def test_prose_with_an_llm_extractor_is_accepted() -> None:
    """The pairing that makes sense: reading a value out of a sentence is what
    the LLM lane exists to measure."""
    cfg = _cfg(render_mode="prose", extractor="llm")
    assert cfg.realism.render_mode == "prose"


def test_structured_with_an_llm_extractor_is_accepted() -> None:
    """Kept legal deliberately: it is the CONTROL arm of the difficulty
    measurement -- same extractor, easier documents -- and forbidding it would
    make the comparison unbuildable."""
    cfg = _cfg(render_mode="structured", extractor="llm")
    assert cfg.realism.render_mode == "structured"
