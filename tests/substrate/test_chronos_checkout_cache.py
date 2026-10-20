"""Coherence tests for the interval backend's checkout memo.

The interval backend memoizes ``checkout`` reads (the branch row's mutable
current-segment pointer and the immutable segment rows) and drops the matching
memo via ``_CheckoutCacheAdapter`` whenever a statement mutates the backing
metadata table. These tests guard that a checkout issued AFTER a write observes
the new state (no stale pointer) and that the segment memo never bleeds one
checkpoint's view into another.
"""

from __future__ import annotations

import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact


pytestmark = pytest.mark.postgres


def _read_n1(session) -> list[dict]:
    return session.query(
        "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'"
    )


def test_checkout_after_write_observes_new_state(clean_runtime: str) -> None:
    """A checkout after a pointer-advancing write must NOT serve a stale memo."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([NodeFact("n:1", "claim", {"text": "v1"}, {}, {})])
        store.checkpoint_generation("cp-a", deployment_name="checkout-cache-test")

        # Populate the checkout memo (branch pointer + current segment).
        assert _read_n1(store.checkout("default")) == [{"attrs": {"text": "v1"}}]

        # Write again + checkpoint: the branch's current_segment_id advances.
        # If the branch memo were not invalidated, this next checkout would
        # resolve the stale segment and miss v2.
        store.apply_facts([NodeFact("n:1", "claim", {"text": "v2"}, {}, {})])
        store.checkpoint_generation("cp-b", deployment_name="checkout-cache-test")

        assert _read_n1(store.checkout("default")) == [{"attrs": {"text": "v2"}}]

        # The historical checkpoint still resolves to its own immutable segment;
        # the segment memo must not have served cp-b's view for cp-a.
        assert _read_n1(store.checkout_generation("cp-a")) == [
            {"attrs": {"text": "v1"}}
        ]
    finally:
        store.close()


def test_checkout_sees_branch_created_after_first_checkout(
    clean_runtime: str,
) -> None:
    """get_branch must not cache a not-found result: a branch created after a
    miss is visible on the next checkout."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([NodeFact("n:1", "claim", {"text": "base"}, {}, {})])
        # Touch the default branch so the memo is warm.
        assert _read_n1(store.checkout("default")) == [{"attrs": {"text": "base"}}]

        store.create_branch("feature", from_branch="default")
        store.apply_facts(
            [NodeFact("n:1", "claim", {"text": "on-feature"}, {}, {})],
            branch="feature",
        )

        assert _read_n1(store.checkout("feature")) == [
            {"attrs": {"text": "on-feature"}}
        ]
        # Default is unchanged by the branch-local write.
        assert _read_n1(store.checkout("default")) == [{"attrs": {"text": "base"}}]
    finally:
        store.close()
