"""Phase 5 tests for A4 — Tier 1/2/3 promotion machinery."""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.scholarly.tier_promotion import (
    PromotionConfig,
    filter_promotions_for_source,
    is_a1_available,
    is_a2_available,
    load_promotions_yaml,
    trigger_tier_1_extraction,
)


# ----------------------------------------------------------------------
# PromotionConfig dataclass + YAML round-trip
# ----------------------------------------------------------------------


def test_promotion_config_accepts_valid_tier() -> None:
    p = PromotionConfig(paper_id="paper:doi:x", tier=1, auto_extract=True)
    assert p.tier == 1
    assert p.auto_extract is True


def test_promotion_config_rejects_invalid_tier() -> None:
    with pytest.raises(SubstrateError) as exc:
        PromotionConfig(paper_id="paper:doi:x", tier=4)
    assert "must be 1, 2, or 3" in str(exc.value)


def test_load_promotions_yaml_top_level_list(tmp_path: Path) -> None:
    yaml_path = tmp_path / "promotions.yaml"
    yaml_path.write_text(
        "- paper_id: paper:doi:10.1/abc\n"
        "  tier: 1\n"
        "  auto_extract: true\n"
        "- paper_id: paper:arxiv:1234.5678\n"
        "  tier: 2\n",
        encoding="utf-8",
    )
    promotions = load_promotions_yaml(yaml_path)
    assert len(promotions) == 2
    assert promotions[0].paper_id == "paper:doi:10.1/abc"
    assert promotions[0].tier == 1
    assert promotions[0].auto_extract is True
    assert promotions[1].tier == 2
    assert promotions[1].auto_extract is False


def test_load_promotions_yaml_under_promotions_key(tmp_path: Path) -> None:
    yaml_path = tmp_path / "promotions.yaml"
    yaml_path.write_text(
        "promotions:\n"
        "  - paper_id: paper:doi:10.1/abc\n"
        "    tier: 1\n",
        encoding="utf-8",
    )
    promotions = load_promotions_yaml(yaml_path)
    assert len(promotions) == 1
    assert promotions[0].paper_id == "paper:doi:10.1/abc"


def test_load_promotions_yaml_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(SubstrateError) as exc:
        load_promotions_yaml(tmp_path / "missing.yaml")
    assert "does not exist" in str(exc.value)


def test_load_promotions_yaml_missing_paper_id_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text("- tier: 1\n", encoding="utf-8")
    with pytest.raises(SubstrateError) as exc:
        load_promotions_yaml(yaml_path)
    assert "missing `paper_id`" in str(exc.value)


def test_load_promotions_yaml_missing_tier_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text("- paper_id: paper:doi:x\n", encoding="utf-8")
    with pytest.raises(SubstrateError) as exc:
        load_promotions_yaml(yaml_path)
    assert "missing `tier`" in str(exc.value)


# ----------------------------------------------------------------------
# Soft hooks: A1 / A2 graceful no-op when modules not yet implemented
# ----------------------------------------------------------------------


def test_is_a1_a2_reflect_current_repo_state() -> None:
    # A1 (paper_anatomy) and A2 (claim_evidence) enricher modules
    # don't exist yet in this repo state — by design (drafts merged,
    # impl forthcoming). Until they land, both must return False.
    assert is_a1_available() is False
    assert is_a2_available() is False


def test_trigger_tier_1_extraction_skips_gracefully_when_a1_a2_absent() -> None:
    result = trigger_tier_1_extraction(
        paper_id="paper:doi:test",
        deployment="memex",
    )
    assert result == {"a1": "skipped", "a2": "skipped"}


# ----------------------------------------------------------------------
# Filter helper
# ----------------------------------------------------------------------


def test_filter_promotions_for_source() -> None:
    promotions = [
        PromotionConfig(paper_id="paper:doi:a", tier=1),
        PromotionConfig(paper_id="paper:doi:b", tier=2),
        PromotionConfig(paper_id="paper:doi:c", tier=1),
    ]
    filtered = filter_promotions_for_source(
        promotions,
        source_paper_ids=["paper:doi:a", "paper:doi:c"],
    )
    assert set(filtered.keys()) == {"paper:doi:a", "paper:doi:c"}
    assert filtered["paper:doi:a"].tier == 1
    assert filtered["paper:doi:c"].tier == 1
