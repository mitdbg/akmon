from __future__ import annotations

import inspect
from pathlib import Path

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.generations import (
    ChronosGraphGenerationLifecycleMixin,
)
from okg.substrate.graph_store_internal.read_branch import (
    ChronosGraphReadBranchMixin,
)
from okg.substrate.graph_store_internal.schema import ChronosGraphSchemaMixin
from okg.substrate.graph_store_internal.writes import ChronosGraphWriteMixin


ROOT = Path(__file__).resolve().parents[2]
GRAPH_STORE = ROOT / "src" / "okg" / "substrate" / "graph_store.py"


def test_chronos_graph_store_composes_internal_boundaries() -> None:
    mro = ChronosGraphStore.__mro__

    assert ChronosGraphSchemaMixin in mro
    assert ChronosGraphGenerationLifecycleMixin in mro
    assert ChronosGraphReadBranchMixin in mro
    assert ChronosGraphWriteMixin in mro


def test_root_graph_store_is_public_facade_not_monolith() -> None:
    source = GRAPH_STORE.read_text()

    assert len(source.splitlines()) < 250
    assert "CREATE TABLE IF NOT EXISTS okg.graph_nodes" not in source
    assert "def _stamp_publisher_derived_edge_lineage" not in source
    assert "def _missing_edge_endpoints" not in source
    assert "def _materialize_checkpoint_tables" not in source


def test_internal_boundaries_own_expected_methods() -> None:
    assert hasattr(ChronosGraphSchemaMixin, "ensure_schema")
    assert hasattr(ChronosGraphSchemaMixin, "ensure_interval_storage_upgrade")
    assert hasattr(ChronosGraphGenerationLifecycleMixin, "checkpoint_generation")
    assert hasattr(ChronosGraphGenerationLifecycleMixin, "latest_generation")
    assert hasattr(ChronosGraphReadBranchMixin, "checkout_generation")
    assert hasattr(ChronosGraphReadBranchMixin, "merge")
    assert hasattr(ChronosGraphWriteMixin, "apply_facts")
    assert hasattr(ChronosGraphWriteMixin, "_missing_edge_endpoints")


def test_public_static_guard_entrypoints_remain_defined_on_facade() -> None:
    for name in (
        "apply_facts",
        "checkout_generation",
        "checkpoint_generation",
        "get_generation",
        "list_generations",
        "latest_generation",
    ):
        source = inspect.getsource(getattr(ChronosGraphStore, name))
        assert "super()." in source
        assert "nodes_live" not in source
        assert "edges_live" not in source


def _edge_row() -> dict:
    return {
        "edge_id": 7,
        "src": "a",
        "dst": "b",
        "edge_type": "references",
        "attrs": {"k": 1},
        "source": "s",
        "source_record_id": {"r": 1},
        "source_revision": {"v": 1},
        "provenance": "p1",
        "confidence": 1.0,
    }


def test_edge_confidence_or_provenance_only_change_is_not_a_noop() -> None:
    """Regression (PR #448 deep review): confidence/provenance are dedicated
    edge columns (not in attrs); a change to either is a real change and must
    not be dropped by the semantic-no-op filter."""
    noop = ChronosGraphWriteMixin._edge_upsert_semantic_noop
    base = _edge_row()
    assert noop(_edge_row(), _edge_row()) is True  # identical -> no-op
    assert noop({**base, "confidence": 0.5}, _edge_row()) is False
    assert noop({**base, "provenance": "p2"}, _edge_row()) is False


def test_edge_semantic_noop_keeps_distinct_null_column_values() -> None:
    """Null is not empty provenance and null is not zero confidence. A lossy
    normalization here would discard a real change before the interval backend
    can protect or version it."""
    noop = ChronosGraphWriteMixin._edge_upsert_semantic_noop
    base = _edge_row()

    assert noop({**base, "provenance": None}, {**base, "provenance": ""}) is False
    assert noop({**base, "confidence": None}, {**base, "confidence": 0.0}) is False


def test_edge_attr_changes_marks_column_only_confidence_provenance() -> None:
    """A column-only confidence/provenance change must land in the dirty scope
    (a non-empty changed set), so it is reported, not silently dropped."""
    prior = _edge_row()
    upsert = {**_edge_row(), "confidence": 0.5, "provenance": "p2"}
    out = ChronosGraphWriteMixin._edge_attr_changes_by_type(
        edge_upserts=[upsert],
        edge_deletes=[],
        prior_by_id={7: prior},
    )
    changed = out["references"][7]
    assert "__confidence__" in changed
    assert "__provenance__" in changed
    assert changed  # non-empty -> the edge is dirty
