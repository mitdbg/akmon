"""Generations minted outside the publisher carry their overlays (PostgreSQL).

Issue #4049, PACT lane-s-storage-cutover. A fast-forward, a history
compaction root and a rollover root are published generations the publisher
never materializes. With the graph rows plus overlays the only read path,
such a generation had no overlay marker, so every unpinned read refused it
until a publish. Each now runs what a publish runs for overlays, in the
transaction that mints it: the predecessor's accepted overlays carried
forward, the marker, and the overlay ranges where it has an ordinal.

Each case: publish, accept an overlay, then the operation. A filter with no
pin, and one pinned at the new generation, are served and show the overlay;
each case failed before the fix (no marker on the new generation). A
publish afterwards works and keeps the overlay after each of the three. (The
publish after a fast-forward was refused by the interval backfill
precondition, #2317, until PACT lane-s-storage-drop-copies removed it.)
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    OVERLAY_GENERATION_MARKER_TABLE,
    OVERLAY_RANGES_TABLE,
    materialize_effective_attributes_for_generation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from tests.substrate.test_overlay_generation_marker_db import (
    _Graph,
    _filter_ids,
    _marker,
    _ordinal,
    _query,
)

pytestmark = pytest.mark.postgres

OVERLAID = {"op": "eq", "field": "attrs.path", "value": "over-a.py"}


def _fast_forward(dsn: str, graph: _Graph, source: str) -> str:
    from okg.substrate.catalog.aliases import active_catalog_version_id

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch("staging:root", from_branch="default",
                            metadata={"base_branch": "default"})
        store.apply_facts(
            [NodeFact(graph.id("g"), "doc", {"path": "g.py", "rank": 9},
                      {"id": graph.id("g")}, {"v": 1})],
            branch="staging:root", source="fork.src", strict_endpoints=True,
        )
        return store.fast_forward(
            "staging:root", target="default", deployment_name="default",
            catalog_version_id=active_catalog_version_id(conn),
        )


def _compact(dsn: str, graph: _Graph, source: str) -> str:
    # As `okg branch compact` runs it: an autocommit connection.
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        result = store.compact_branch_from_generation(
            source, deployment_name="default", branch="default",
            metadata={"compacted_by": "test"},
        )
    return str(result["new_generation_id"])


def _rollover(dsn: str, graph: _Graph, source: str) -> str:
    # As `okg branch rollover` runs it: one transaction the caller commits.
    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
        )
        try:
            result = store.rollover_branch_from_generation(
                source, deployment_name="default", branch="default",
                metadata={"rolled_over_by": "test"},
            )
            conn.commit()
            store.context.finalize_external_transaction(committed=True)
        except Exception:
            conn.rollback()
            store.context.finalize_external_transaction(committed=False)
            raise
    return str(result["new_generation_id"])


OPERATIONS = {"fast_forward": _fast_forward, "compaction": _compact,
              "rollover": _rollover}


@pytest.mark.parametrize("operation", sorted(OPERATIONS))
def test_a_root_generation_carries_its_overlays_and_is_served(
    clean_runtime: str, operation: str,
) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    source = graph.publish()
    graph.overlay(source, "a", "path", "over-a.py")
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=source)
    assert _filter_ids(dsn, source, OVERLAID) == [graph.id("a")]

    minted = OPERATIONS[operation](dsn, graph, source)
    assert minted != source
    marker: Any = _marker(dsn, minted)
    assert marker is not None, (operation, minted)
    assert marker[0] >= 1, marker  # the carried-forward overlay row
    # The range sync ran: a compaction and a rollover root take the next
    # ordinal and fold their overlays into the ranges; a fast-forward mints
    # no ordinal (#2317), so there is nothing to sync and the marker says so.
    ordinals = _query(
        dsn, "SELECT ordinal FROM okg.generation_ordinals WHERE checkpoint_id = %s",
        (minted,),
    )
    if operation == "fast_forward":
        assert ordinals == [], ordinals
        assert marker[2] is None and marker[3] is False, marker
    else:
        assert marker[2] == _ordinal(dsn, minted), marker
        assert marker[3] is True, marker
    # Served unpinned (the new head) and pinned, with the overlay.
    assert _filter_ids(dsn, None, OVERLAID) == [graph.id("a")], operation
    assert _filter_ids(dsn, minted, OVERLAID) == [graph.id("a")], operation


@pytest.mark.parametrize("operation", [
    pytest.param("compaction"),
    pytest.param("fast_forward"),
    pytest.param("rollover"),
])
def test_a_publish_after_a_root_generation_keeps_its_overlay(
    clean_runtime: str, operation: str,
) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    source = graph.publish()
    graph.overlay(source, "a", "path", "over-a.py")
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=source)
    OPERATIONS[operation](dsn, graph, source)

    after = graph.publish()
    assert _marker(dsn, after) is not None
    assert _filter_ids(dsn, after, OVERLAID) == [graph.id("a")], operation


_METHODS = {
    "compaction": ("compact_branch_from_generation", {"compacted_by": "test"}),
    "rollover": ("rollover_branch_from_generation", {"rolled_over_by": "test"}),
}


def _run_root_operation(
    dsn: str, source: str, operation: str, *, caller_transaction: bool,
) -> None:
    """Compaction or rollover of ``source`` on the default branch, either on
    an autocommit connection (as `okg branch compact` runs) or inside a
    transaction the caller owns and rolls back on failure (as `okg branch
    rollover` runs)."""
    method, metadata = _METHODS[operation]
    if not caller_transaction:
        with psycopg.connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            getattr(store, method)(
                source, deployment_name="default", branch="default", metadata=metadata,
            )
        return
    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
        )
        try:
            getattr(store, method)(
                source, deployment_name="default", branch="default", metadata=metadata,
            )
            conn.commit()
            store.context.finalize_external_transaction(committed=True)
        except Exception:
            conn.rollback()
            store.context.finalize_external_transaction(committed=False)
            raise


def _lifecycle_state(dsn: str) -> dict[str, Any]:
    """What a failed root operation must leave as it was: every checkpoint,
    the default branch's head, every overlay marker, every ordinal and the
    overlay ranges."""
    return {
        "checkpoints": _query(
            dsn,
            "SELECT checkpoint_id, branch_id FROM _chronos_branch_interval_checkpoints "
            "ORDER BY checkpoint_id",
        ),
        "branch": _query(
            dsn,
            "SELECT current_segment_id, metadata FROM _chronos_branch_interval_branches "
            "WHERE branch_id = 'default'",
        ),
        "markers": _query(
            dsn,
            f"SELECT generation_id, overlay_rows, ordinal, ranges_synced "
            f"FROM {OVERLAY_GENERATION_MARKER_TABLE} ORDER BY generation_id",
        ),
        "ordinals": _query(
            dsn, "SELECT checkpoint_id, ordinal FROM okg.generation_ordinals ORDER BY ordinal",
        ),
        "ranges": _query(
            dsn,
            f"SELECT target_id, attr_key, from_ord, to_ord, value "
            f"FROM {OVERLAY_RANGES_TABLE} ORDER BY 1, 2, 3",
        ),
    }


class _InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize("caller_transaction", [False, True],
                         ids=["autocommit", "caller_transaction"])
@pytest.mark.parametrize("failing", [
    "materialize_effective_attributes_for_generation",
    "sync_overlay_ranges_for_generation",
])
@pytest.mark.parametrize("operation", sorted(_METHODS))
def test_a_failed_overlay_write_leaves_no_root_generation_and_the_old_head(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
    operation: str, failing: str, caller_transaction: bool,
) -> None:
    """The mint and its overlay writes are one transaction (issue #4049): when
    the overlay copy or the range sync raises inside a compaction or a
    rollover, there is no new generation, no new marker, and the old head is
    as it was and still served with its overlay. On an autocommit connection
    (the compaction CLI's case) this failed before the transaction: the
    generation was committed before its overlay writes ran."""
    import okg.substrate.attributes as attributes

    dsn = clean_runtime
    graph = _Graph(dsn)
    source = graph.publish()
    graph.overlay(source, "a", "path", "over-a.py")
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=source)
    before = _lifecycle_state(dsn)
    calls: list[str] = []

    def fail(*args: Any, **kwargs: Any) -> None:
        calls.append(failing)
        raise _InjectedFailure(failing)

    monkeypatch.setattr(attributes, failing, fail)
    with pytest.raises(_InjectedFailure):
        _run_root_operation(dsn, source, operation, caller_transaction=caller_transaction)
    monkeypatch.undo()

    assert calls == [failing]
    assert _lifecycle_state(dsn) == before
    assert _filter_ids(dsn, None, OVERLAID) == [graph.id("a")]
    assert _filter_ids(dsn, source, OVERLAID) == [graph.id("a")]
