"""Coverage tests for `okg.substrate.catalog.invariants_scope`.

The module produces a `scope` CTE that filters node IDs to a
Concept's neighborhood. We exercise the four behaviors:

1. `concept_name_to_node_id` slug rules (pure function).
2. `compute_scope_injection` happy path (concept resolves; CTE
   built; params populated).
3. `compute_scope_injection` raises `ScopeUnresolved` when the
   concept doesn't resolve.
4. Multi-hop / non-mentions hops raise `NotImplementedError`
   (future-work guard).
"""
from __future__ import annotations

import psycopg
import pytest

from tests.substrate._db_helpers import ensure_published_generation
from okg.substrate.catalog.invariants_scope import (
    ScopeInjection,
    ScopeUnresolved,
    compute_scope_injection,
    concept_name_to_node_id,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact


pytestmark = pytest.mark.postgres


# ---------- concept_name_to_node_id (pure) ----------


def test_concept_name_slug_lowercases_and_collapses_separators() -> None:
    assert concept_name_to_node_id("Bad Weather") == "concept:bad_weather"
    assert concept_name_to_node_id("crew-rest") == "concept:crew_rest"
    assert concept_name_to_node_id("CHAPLAIN_SERVICES") == (
        "concept:chaplain_services"
    )


def test_concept_name_slug_drops_punctuation_and_collapses_dunders() -> None:
    # commas / parens drop; multiple underscores collapse to one.
    assert concept_name_to_node_id("Bad, Weather!") == (
        "concept:bad_weather"
    )
    assert concept_name_to_node_id("a   b") == "concept:a_b"
    assert concept_name_to_node_id("__x__") == "concept:x"


# ---------- compute_scope_injection ----------


def _seed_concept(
    conn: psycopg.Connection, concept_id: str, *, gen: int = 1,
) -> None:
    """Write a Concept node into the Chronos graph at the given
    generation.

    The node is written first, then the generation is checkpointed,
    so the reader's pinned `checkout_generation(gen)` resolves to a
    snapshot containing the concept.
    """
    ChronosGraphStore.from_connection(conn).apply_facts(
        [NodeFact(concept_id, "concept", {}, {"id": concept_id}, {"v": 1})],
        source="default",
        strict_endpoints=True,
    )
    ensure_published_generation(conn, gen)


def test_compute_scope_injection_happy_path(clean_runtime: str) -> None:
    """A concept that resolves yields a `ScopeInjection` with the
    expected CTE shape and params."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_concept(conn, "concept:bad_weather", gen=1)

    spec = {
        "concept_name": "Bad Weather",
        "min_confidence": 0.5,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        injection = compute_scope_injection(
            conn, spec, generation_id=1,
        )
    assert isinstance(injection, ScopeInjection)
    assert injection.concept_id == "concept:bad_weather"
    assert "scope AS (" in injection.cte_sql
    assert "edge_type = 'mentions'" in injection.cte_sql
    assert injection.params["scope_concept_id"] == "concept:bad_weather"
    assert injection.params["scope_min_confidence"] == 0.5


def test_compute_scope_injection_raises_when_concept_missing(
    clean_runtime: str,
) -> None:
    """When the concept doesn't resolve at the given generation,
    the runner raises ScopeUnresolved (NOT silently empty)."""
    # Checkpoint an (otherwise empty) generation so the reader's
    # pinned `checkout_generation(1)` resolves; the concept is
    # genuinely absent from the snapshot.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_published_generation(conn, 1)

    spec = {
        "concept_name": "Nonexistent Concept",
        "min_confidence": 0.5,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ScopeUnresolved, match="concept_name"):
            compute_scope_injection(conn, spec, generation_id=1)


def test_compute_scope_injection_rejects_multihop(
    clean_runtime: str,
) -> None:
    """Future-work guard: hops > 1 or non-`mentions` hop_edges
    raises loudly rather than silently degrading."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_concept(conn, "concept:bad_weather", gen=1)

    spec_multihop = {
        "concept_name": "Bad Weather",
        "min_confidence": 0.5,
        "hops": 2,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(NotImplementedError, match="hops=2"):
            compute_scope_injection(conn, spec_multihop, generation_id=1)

    spec_other_edge = {
        "concept_name": "Bad Weather",
        "min_confidence": 0.5,
        "hop_edges": ["references"],
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(NotImplementedError, match="hop_edges"):
            compute_scope_injection(
                conn, spec_other_edge, generation_id=1,
            )


def test_compute_scope_injection_concept_at_different_gen(
    clean_runtime: str,
) -> None:
    """A concept seeded at gen 5 is NOT resolved at gen 3."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Checkpoint the earlier generation (3) BEFORE the concept
        # is written, so its snapshot excludes the concept; then
        # write the concept and checkpoint generation 5.
        ensure_published_generation(conn, 3)
        _seed_concept(conn, "concept:bad_weather", gen=5)

    spec = {
        "concept_name": "Bad Weather",
        "min_confidence": 0.5,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ScopeUnresolved):
            compute_scope_injection(conn, spec, generation_id=3)
        # Same concept resolves at gen 5+.
        injection = compute_scope_injection(
            conn, spec, generation_id=5,
        )
        assert injection.concept_id == "concept:bad_weather"


def test_compute_scope_injection_accepts_jsonb_list_hop_edges(
    clean_runtime: str,
) -> None:
    """The JSONB-deserialized spec form has hop_edges as a list
    (not tuple). Both forms must work."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_concept(conn, "concept:bad_weather", gen=1)

    spec = {
        "concept_name": "Bad Weather",
        "min_confidence": 0.5,
        "hop_edges": ["mentions"],  # list, not tuple
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        injection = compute_scope_injection(
            conn, spec, generation_id=1,
        )
        assert injection.concept_id == "concept:bad_weather"
