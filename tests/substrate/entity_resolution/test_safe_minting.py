from __future__ import annotations

import pytest

from okg.substrate.entity_resolution import (
    EntityResolutionRejected,
    mint_entity_id,
    normalize_entity_name,
)


def test_unicode_escape_is_stable_and_preserves_meaningful_codepoints() -> None:
    assert mint_entity_id("region", "γ Signal Region") == mint_entity_id(
        "region", "γ  Signal—Region"
    )
    assert "u03b3" in mint_entity_id("region", "γ Signal Region")
    assert mint_entity_id("region", "γ Signal Region") != mint_entity_id(
        "region", "Signal Region"
    )


def test_nfkc_casefold_normalization_is_explicit() -> None:
    assert normalize_entity_name("  Extended–ＡＢＣＤ  ") == "extended abcd"


def test_combining_marks_remain_part_of_the_deterministic_identity() -> None:
    bare = mint_entity_id("region", "क")
    marked = mint_entity_id("region", "का")

    assert bare != marked
    assert "u093e" in marked


def test_empty_identity_is_rejected_instead_of_falling_back() -> None:
    with pytest.raises(EntityResolutionRejected):
        mint_entity_id("region", "!!!")
