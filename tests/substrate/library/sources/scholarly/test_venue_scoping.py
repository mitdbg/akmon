"""Phase 1 tests for A4 — venue scoping + substrate-library cache_root purity.

The `add-substrate-bibliographic-sources` Phase 1 adds a deployment-
configurable `venue_slugs: list[str]` filter to the three scholarly
adapters that have a venues catalog (`DblpSource`, `OpenReviewSource`,
`InspireHepSource`), and removes memex-specific cache_root defaults
from all 8 firehose adapters.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.scholarly.dblp import DblpSource
from okg.substrate.library.sources.scholarly.inspire_hep import InspireHepSource
from okg.substrate.library.sources.scholarly.openreview import OpenReviewSource


# ----------------------------------------------------------------------
# DblpSource — venue_slugs filter + cache_root guard
# ----------------------------------------------------------------------


def test_dblp_venue_slugs_filters_catalog(tmp_path: Path) -> None:
    src = DblpSource(
        venue_slugs=["osdi", "sigmod"],
        cache_root=tmp_path,
    )
    slugs = {v["slug"] for v in src.venues}
    assert slugs == {"osdi", "sigmod"}


def test_dblp_venue_slugs_unknown_raises(tmp_path: Path) -> None:
    with pytest.raises(SubstrateError) as exc:
        DblpSource(venue_slugs=["definitely-not-a-venue"], cache_root=tmp_path)
    assert "not in the venues catalog" in str(exc.value)
    assert "definitely-not-a-venue" in str(exc.value)


def test_dblp_venue_slugs_none_preserves_full_catalog(tmp_path: Path) -> None:
    src = DblpSource(cache_root=tmp_path)
    # Default venues catalog has 20+ entries; venue_slugs=None must not
    # filter.
    assert len(src.venues) >= 20


def test_dblp_cache_root_required_when_no_fixture() -> None:
    """Substrate-library purity: cache_root defaults to None (no memex
    fallback). When the source runs against the live API path
    (fixture_path is None), it must fail loudly."""
    src = DblpSource()
    with pytest.raises(SubstrateError) as exc:
        list(src.run("test-run-id", mode="scope_complete").facts)
    assert "cache_root is required" in str(exc.value)


# ----------------------------------------------------------------------
# OpenReviewSource — venue_slugs filter + cache_root guard
# ----------------------------------------------------------------------


def test_openreview_venue_slugs_filters_catalog(tmp_path: Path) -> None:
    # Pick a slug guaranteed to be in OpenReview's DEFAULT_VENUES.
    from okg.substrate.library.sources.scholarly.openreview import (
        DEFAULT_VENUES,
    )
    first = DEFAULT_VENUES[0]["slug"]
    src = OpenReviewSource(
        venue_slugs=[first],
        cache_root=tmp_path,
    )
    slugs = {v["slug"] for v in src.venues}
    assert slugs == {first}


def test_openreview_venue_slugs_unknown_raises(tmp_path: Path) -> None:
    with pytest.raises(SubstrateError) as exc:
        OpenReviewSource(
            venue_slugs=["definitely-not-a-venue"],
            cache_root=tmp_path,
        )
    assert "not in the venues catalog" in str(exc.value)


def test_openreview_cache_root_required_when_no_fixture() -> None:
    src = OpenReviewSource()
    with pytest.raises(SubstrateError) as exc:
        list(src.run("test-run-id", mode="scope_complete").facts)
    assert "cache_root is required" in str(exc.value)


# ----------------------------------------------------------------------
# InspireHepSource — venue_slugs filter + cache_root guard
# ----------------------------------------------------------------------


def test_inspire_hep_venue_slugs_filters_catalog(tmp_path: Path) -> None:
    from okg.substrate.library.sources.scholarly.inspire_hep import (
        DEFAULT_VENUES,
    )
    first = DEFAULT_VENUES[0]["slug"]
    src = InspireHepSource(
        venue_slugs=[first],
        cache_root=tmp_path,
    )
    slugs = {v["slug"] for v in src.venues}
    assert slugs == {first}


def test_inspire_hep_venue_slugs_unknown_raises(tmp_path: Path) -> None:
    with pytest.raises(SubstrateError) as exc:
        InspireHepSource(
            venue_slugs=["definitely-not-a-venue"],
            cache_root=tmp_path,
        )
    assert "not in the venues catalog" in str(exc.value)


def test_inspire_hep_cache_root_required_when_no_fixture() -> None:
    src = InspireHepSource()
    with pytest.raises(SubstrateError) as exc:
        list(src.run("test-run-id", mode="scope_complete").facts)
    assert "cache_root is required" in str(exc.value)
