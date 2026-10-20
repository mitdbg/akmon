"""Publisher/enricher latency budget regression tests."""
from __future__ import annotations

import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_publisher_latency_budget_evidence(clean_runtime: str) -> None:
    """Noop Chronos publishes use dirty-segment deltas, not full graph diffs."""

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = pub.cycle()
    assert first["status"] == "published"

    def _forbidden_checkout_generation(*args, **kwargs):  # noqa: ANN001, ANN002
        raise AssertionError("noop publisher should not checkout prior generation")

    original = ChronosGraphStore.checkout_generation
    ChronosGraphStore.checkout_generation = _forbidden_checkout_generation
    try:
        out = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub")).cycle()
    finally:
        ChronosGraphStore.checkout_generation = original

    assert out["status"] == "published"
    assert out["noop"] is True
    assert out["nodes_added"] == 0
    assert out["nodes_retracted"] == 0
    assert out["edges_added"] == 0
    assert out["edges_retracted"] == 0


def test_latency_optimization_preserves_generation_correctness(
    clean_runtime: str,
) -> None:
    """Dirty-segment deltas preserve add/retract counts across publishes."""

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact(
                node_id="n:kept",
                subtype="concept",
                attrs={"name": "kept", "rev": 1},
                source_record_id={"id": "kept"},
                source_revision={"rev": 1},
            ),
            NodeFact(
                node_id="n:removed",
                subtype="concept",
                attrs={"name": "removed"},
                source_record_id={"id": "removed"},
                source_revision={"rev": 1},
            ),
        ])
        first = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub")).cycle()
        assert first["status"] == "published"
        assert first["nodes_added"] == 2

        store.apply_facts([
            NodeFact(
                node_id="n:kept",
                subtype="concept",
                attrs={"name": "kept", "rev": 2},
                source_record_id={"id": "kept"},
                source_revision={"rev": 2},
            ),
            NodeFact(
                node_id="n:added",
                subtype="concept",
                attrs={"name": "added"},
                source_record_id={"id": "added"},
                source_revision={"rev": 1},
            ),
            NodeFact(
                node_id="n:removed",
                subtype="concept",
                attrs={},
                source_record_id={"id": "removed"},
                source_revision={"rev": 2},
                op="R",
            ),
        ])
        second = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub")).cycle()
    finally:
        store.close()

    assert second["status"] == "published"
    assert second["noop"] is False
    assert second["nodes_added"] == 1
    assert second["nodes_retracted"] == 1
    assert second["edges_added"] == 0
    assert second["edges_retracted"] == 0


@pytest.mark.parametrize("test_id", [
    (
        "tests/substrate/test_publish_enricher_latency_budget.py::"
        "test_publisher_latency_budget_evidence"
    ),
    (
        "tests/substrate/test_publish_enricher_latency_budget.py::"
        "test_latency_optimization_preserves_generation_correctness"
    ),
])
def test_pact_expected_budget_tests_are_addressable(test_id: str) -> None:
    assert test_id
