"""Merge admission partition (worktree-fork-merge-back): split a merge preview into
default-accepted vs quarantine (conflicts + illegal rows), with no silent loss.

Pure tests pin the PACT's accept/conflict/illegal bucketing + the totality invariant; the
real-chronos test proves the partition + legality verdict run over an actual fork preview.
"""
from __future__ import annotations

import uuid

import pytest

from chronos_core.branching import MergePreview, RowDiff

from okg.substrate.merge_guard import MergeRefused
from okg.substrate.merge_partition import (
    NODE_TABLE,
    partition_merge,
    subtype_legality_verdict,
)

LEGAL = subtype_legality_verdict(frozenset({"person"}))


def _node(node_id: str, subtype: str | None, change: str = "added") -> RowDiff:
    after = None
    if change != "deleted":
        after = {"node_id": node_id, "subtype": subtype} if subtype is not None else {"node_id": node_id}
    before = None if change == "added" else {"node_id": node_id, "subtype": subtype}
    return RowDiff(NODE_TABLE, {"node_id": node_id}, change, before, after)


def _edge(edge_id: int) -> RowDiff:
    return RowDiff("okg.graph_edges", {"edge_id": edge_id}, "added", None,
                   {"edge_id": edge_id, "edge_type": "mentions"})


def _preview(changes, conflicts=()) -> MergePreview:
    return MergePreview("experiment:f", "default", list(changes), list(conflicts))


def test_pact_accept_conflict_illegal_bucketing() -> None:
    # The PACT's literal acceptance: one clean+legal row, one conflict, one
    # non-conflicting illegal row -> accepted == {clean}, quarantined == {conflict, illegal}
    # as SETS (not merely non-empty).
    clean = _node("ok", "person")
    illegal = _node("bad", "ghost_subtype")
    conflict = _node("conf", "person")
    part = partition_merge(_preview([clean, illegal], conflicts=[conflict]), is_legal=LEGAL)
    assert {d.key["node_id"] for d in part.accepted} == {"ok"}
    assert {d.key["node_id"] for d in part.quarantined} == {"conf", "bad"}


def test_total_coverage_no_silent_loss() -> None:
    # accepted ∪ quarantined == changes ∪ conflicts; nothing is dropped.
    changes = [_node("a", "person"), _node("b", "ghost"), _node("c", "person")]
    conflicts = [_node("d", "person"), _node("e", "ghost")]
    part = partition_merge(_preview(changes, conflicts), is_legal=LEGAL)
    assert len(part.accepted) + len(part.quarantined) == len(changes) + len(conflicts)
    acc = {d.key["node_id"] for d in part.accepted}
    quar = {d.key["node_id"] for d in part.quarantined}
    assert acc.isdisjoint(quar)
    assert acc | quar == {"a", "b", "c", "d", "e"}


def test_deletions_and_edges_are_legal() -> None:
    deleted = _node("gone", "person", change="deleted")
    edge = _edge(1)
    part = partition_merge(_preview([deleted, edge]), is_legal=LEGAL)
    assert {d.key.get("node_id") or d.key.get("edge_id") for d in part.accepted} == {"gone", 1}
    assert part.quarantined == ()


def test_node_with_no_subtype_is_unclassifiable_not_silently_accepted() -> None:
    with pytest.raises(MergeRefused) as ei:
        partition_merge(_preview([_node("x", None)]), is_legal=LEGAL)
    assert ei.value.data["reason"] == "unclassifiable_merge_row"


def test_illegal_subtype_quarantined_known_accepted() -> None:
    part = partition_merge(_preview([_node("k", "person"), _node("r", "retired")]), is_legal=LEGAL)
    assert {d.key["node_id"] for d in part.accepted} == {"k"}
    assert {d.key["node_id"] for d in part.quarantined} == {"r"}


# --------------------------------------------------------------------------- #
# Real chronos: partition + legality verdict over an actual fork preview
# --------------------------------------------------------------------------- #

@pytest.mark.postgres
def test_partition_over_real_fork_preview(clean_runtime: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    sfx = uuid.uuid4().hex[:8]
    base = f"n:{sfx}:base"
    legal = f"n:{sfx}:legal"
    ghost = f"n:{sfx}:ghost"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([NodeFact(base, "person", {}, {"id": base}, {"v": 1})],
                          source="default", strict_endpoints=True)
        store.create_branch(fork, from_branch="default", metadata={"base_branch": "default"})
        # A legal (person) and an illegal (unknown subtype) fork add.
        store.apply_facts(
            [
                NodeFact(legal, "person", {}, {"id": legal}, {"v": 1}),
                NodeFact(ghost, "ghost_subtype", {}, {"id": ghost}, {"v": 1}),
            ],
            branch=fork, source="fork.src", strict_endpoints=True,
        )
        preview = store.context._backend.merge_preview(fork, "default")
        assert preview is not None
        part = partition_merge(preview, is_legal=subtype_legality_verdict(frozenset({"person"})))
        accepted = {d.key.get("node_id") for d in part.accepted}
        quarantined = {d.key.get("node_id") for d in part.quarantined}
        assert legal in accepted and legal not in quarantined
        assert ghost in quarantined and ghost not in accepted
