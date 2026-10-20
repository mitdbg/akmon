"""Promote-scoping filter (worktree-fork-merge-back): only durable-marked fork rows are
eligible to merge onto default; scratch rows and deletions are excluded deny-by-default.

Pure tests pin the predicate/partition against the real RowDiff shape; the real-chronos
test proves the filter reads the marker off an actual fork diff, not a hand-built dict.
"""
from __future__ import annotations

import uuid

import pytest

from chronos_core.branching import RowDiff

from okg.substrate.merge_guard import MergeRefused
from okg.substrate.promote_filter import (
    assert_only_promote_marked,
    is_promote_marked,
    scope_to_promote_marked,
)


def _node(node_id: str, attrs: dict | None, change: str = "added") -> RowDiff:
    after = None if change == "deleted" else {"node_id": node_id, "attrs": attrs}
    before = None if change == "added" else {"node_id": node_id, "attrs": attrs}
    return RowDiff("okg.graph_nodes", {"node_id": node_id}, change, before, after)


# --------------------------------------------------------------------------- #
# Pure predicate / partition
# --------------------------------------------------------------------------- #

def test_durable_marker_is_promote_marked() -> None:
    assert is_promote_marked(_node("a", {"promote_intent": "durable", "text": "t"}))
    # case / whitespace tolerant
    assert is_promote_marked(_node("a", {"promote_intent": " Durable "}))


def test_scratch_and_missing_marker_excluded() -> None:
    assert not is_promote_marked(_node("a", {"text": "t"}))            # no marker key
    assert not is_promote_marked(_node("a", {"promote_intent": "scratch"}))
    assert not is_promote_marked(_node("a", {"promote_intent": ""}))
    assert not is_promote_marked(_node("a", None))                      # attrs None
    assert not is_promote_marked(_node("a", {}))                        # empty attrs


def test_deletion_is_never_promote_marked() -> None:
    # after is None for a deletion -> excluded by construction (deletes are gated by the
    # separate deletion-authority guard, never silently promoted here).
    assert not is_promote_marked(_node("a", {"promote_intent": "durable"}, change="deleted"))


def test_partition_buckets_durable_vs_scratch() -> None:
    durable = _node("dur", {"promote_intent": "durable"})
    scratch = _node("scr", {"text": "t"})
    deleted = _node("del", None, change="deleted")
    scope = scope_to_promote_marked([durable, scratch, deleted])
    assert scope.promote == (durable,)
    assert {d.key["node_id"] for d in scope.excluded} == {"scr", "del"}


def test_assert_only_promote_marked_refuses_unmarked() -> None:
    durable = _node("dur", {"promote_intent": "durable"})
    scratch = _node("scr", {"text": "t"})
    with pytest.raises(MergeRefused) as ei:
        assert_only_promote_marked([durable, scratch], source="experiment:f", target="default")
    assert ei.value.data["reason"] == "not_promote_marked"
    assert ei.value.data["excluded_keys"] == [{"node_id": "scr"}]


def test_assert_only_promote_marked_passes_all_marked() -> None:
    rows = [_node("a", {"promote_intent": "durable"}), _node("b", {"promote_intent": "durable"})]
    assert assert_only_promote_marked(rows, source="experiment:f", target="default") == tuple(rows)


def test_after_not_a_dict_is_excluded_not_error() -> None:
    # A malformed diff (after is a non-dict) must be excluded, never raise KeyError.
    bad = RowDiff("okg.graph_nodes", {"node_id": "x"}, "added", None, "oops")  # type: ignore[arg-type]
    assert not is_promote_marked(bad)
    assert scope_to_promote_marked([bad]).excluded == (bad,)


# --------------------------------------------------------------------------- #
# Real chronos: the filter over an actual fork diff
# --------------------------------------------------------------------------- #

@pytest.mark.postgres
def test_promote_filter_over_real_fork_diff(clean_runtime: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    sfx = uuid.uuid4().hex[:8]
    base = f"n:{sfx}:base"
    durable = f"n:{sfx}:durable"
    scratch = f"n:{sfx}:scratch"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(base, "person", {}, {"id": base}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        store.create_branch(fork, from_branch="default", metadata={"base_branch": "default"})
        # Durable-marked + scratch fork rows (marker lands in the attrs JSONB column).
        store.apply_facts(
            [
                NodeFact(durable, "person", {"promote_intent": "durable"}, {"id": durable}, {"v": 1}),
                NodeFact(scratch, "person", {}, {"id": scratch}, {"v": 1}),
            ],
            branch=fork, source="fork.src", strict_endpoints=True,
        )

        changes = store.diff("default", fork).changes
        node_changes = [c for c in changes if c.key.get("node_id") in {durable, scratch}]
        assert {c.key["node_id"] for c in node_changes} == {durable, scratch}, node_changes

        scope = scope_to_promote_marked(node_changes)
        assert {c.key["node_id"] for c in scope.promote} == {durable}
        assert {c.key["node_id"] for c in scope.excluded} == {scratch}
