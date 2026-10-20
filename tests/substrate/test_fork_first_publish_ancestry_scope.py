"""Issue #2865: a fork's first publish checks every row written since its base.

A fork's first publish gates only its delta against the recorded base
generation. Before this fix the delta was the rows written in the branch's
CURRENT segment. A branch made from another branch inherits rows its parent
wrote after the base, and those rows sit in the parent's segment, so the gate
never saw them. The merge-back gate branch is exactly that shape: it is made
from the staging branch, which holds the fork's composed rows. A violating fork
therefore passed the gate and was caught only when it landed on ``default``,
which forced the rollback of #2862.

These tests prove the gate now reads every row written since the base along the
whole branch ancestry: a violating fork blocks at the gate and ``default`` is
never written; a clean fork still lands; and a plain fork-of-a-fork's first
publish checks the rows its parent wrote.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

import okg.substrate.merge_back as merge_back_module  # noqa: E402
from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.merge_guard import MergeDecision  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_factory_test_deployment,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

DECIDER = MergeDecision(decided_by="alice@example.org", decision="accept", fast_track=False)

# Lane E's shape (PR #2863): `references` is allowed only from a reviewer
# record to a person, so a `references` edge between two reviewer records is a
# narrowing violation.
REVIEWER = "akmon_reviewer_record"
PERSON = "person"
NARROWINGS = [(REVIEWER, "references", PERSON)]


def _publisher(dsn: str, branch: str) -> Publisher:
    return Publisher(
        dsn,
        "default",
        branch_name=branch,
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    )


def _seed_default(conn: Any, dsn: str, sfx: str) -> tuple[str, list[str]]:
    """Two reviewer records and a person on default, published."""
    seed_catalog(conn, subtypes=[REVIEWER, PERSON], narrowings=NARROWINGS)
    store = ChronosGraphStore.from_connection(conn)
    nodes = [f"n:{sfx}:r1", f"n:{sfx}:r2", f"n:{sfx}:p"]
    store.apply_facts(
        [
            NodeFact(nodes[0], REVIEWER, {}, {"id": nodes[0]}, {"v": 1}),
            NodeFact(nodes[1], REVIEWER, {}, {"id": nodes[1]}, {"v": 1}),
            NodeFact(nodes[2], PERSON, {}, {"id": nodes[2]}, {"v": 1}),
        ],
        source="default",
        strict_endpoints=True,
    )
    out = _publisher(dsn, "default").cycle()
    assert out["status"] == "published", out
    return str(out["generation_id"]), nodes


def _references(src: str, dst: str, *, durable: bool) -> EdgeFact:
    return EdgeFact(
        src,
        dst,
        "references",
        attrs={"promote_intent": "durable"} if durable else {},
        source_record_id={"id": f"{src}->{dst}"},
        source_revision={"v": 1},
    )


def _default_edges(store: ChronosGraphStore) -> list[dict]:
    return store.checkout("default").query(
        "SELECT src, dst, edge_type FROM okg.graph_edges"
    )


def _spy_merges(monkeypatch: Any) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    real_merge = ChronosGraphStore.merge

    def spy(self, source, target, *args, **kwargs):
        calls.append((str(source), str(target)))
        return real_merge(self, source, target, *args, **kwargs)

    monkeypatch.setattr(ChronosGraphStore, "merge", spy)
    return calls


def test_merge_back_violating_fork_blocks_at_gate_and_never_writes_default(
    clean_runtime: str, monkeypatch: Any, tmp_path: Path,
) -> None:
    bind_factory_test_deployment(monkeypatch, tmp_path, "default")
    merges = _spy_merges(monkeypatch)
    sfx = uuid.uuid4().hex[:8]
    fork = f"experiment:{sfx}:bad"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen, (r1, r2, _p) = _seed_default(conn, clean_runtime, sfx)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [_references(r1, r2, durable=True)],
            branch=fork, source="fork.src", strict_endpoints=True,
        )
        edges_before = _default_edges(store)

        result = merge_back_module.merge_back(
            store, fork, dsn=clean_runtime, base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "blocked", result
        # Blocked by the GATE, not by the landing publish on default.
        assert result.blocked_reason, result
        assert not str(result.blocked_reason).startswith("land_gate_blocked"), result
        # default was never written: no merge targeted it, its published
        # generation did not move, and the illegal edge is absent.
        assert [call for call in merges if call[1] == "default"] == [], merges
        latest = store.latest_generation(
            deployment_name="default", branch="default", status="published",
        )
        assert str(latest.checkpoint_id) == base_gen
        assert _default_edges(store) == edges_before


def test_merge_back_clean_fork_still_lands(
    clean_runtime: str, monkeypatch: Any, tmp_path: Path,
) -> None:
    bind_factory_test_deployment(monkeypatch, tmp_path, "default")
    sfx = uuid.uuid4().hex[:8]
    fork = f"experiment:{sfx}:good"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen, (r1, _r2, p) = _seed_default(conn, clean_runtime, sfx)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [_references(r1, p, durable=True)],
            branch=fork, source="fork.src", strict_endpoints=True,
        )

        result = merge_back_module.merge_back(
            store, fork, dsn=clean_runtime, base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "merged", result
        assert {(r1, p, "references")} <= {
            (row["src"], row["dst"], row["edge_type"]) for row in _default_edges(store)
        }


def test_fork_of_fork_first_publish_checks_rows_the_parent_wrote(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    parent = f"experiment:{sfx}:parent"
    child = f"experiment:{sfx}:child"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen, (r1, r2, _p) = _seed_default(conn, clean_runtime, sfx)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            parent, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        # The parent writes the violation and never publishes.
        store.apply_facts(
            [_references(r1, r2, durable=False)],
            branch=parent, source="default", strict_endpoints=True,
        )
        # The child is made from the parent but anchored to default's base,
        # like the merge-back gate branch. It writes nothing itself.
        store.create_branch(
            child, from_branch=parent,
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )

    out = _publisher(clean_runtime, child).cycle()

    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["blocks_publish"] is True, gate
    assert gate["reason"] == "transform_consistency_violation", gate
    # The parent's edge was the scoped delta; the child wrote nothing itself.
    assert gate["preflight"]["dirty_edge_count"] == 1, gate


def test_fork_of_fork_first_publish_clean_parent_rows_publish(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    parent = f"experiment:{sfx}:parent"
    child = f"experiment:{sfx}:child"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen, (r1, _r2, p) = _seed_default(conn, clean_runtime, sfx)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            parent, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [_references(r1, p, durable=False)],
            branch=parent, source="default", strict_endpoints=True,
        )
        store.create_branch(
            child, from_branch=parent,
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )

    out = _publisher(clean_runtime, child).cycle()

    assert out["status"] == "published", out
    # The parent's edge is the child's delta against the base: it was in scope.
    assert out["edges_added"] == 1, out


def test_fork_first_publish_checks_default_rows_written_after_the_base(
    clean_runtime: str,
) -> None:
    # A fork made while default holds writes its latest published generation
    # does not have: the fork's graph contains them, the base never validated
    # them, so they are part of the fork's delta.
    sfx = uuid.uuid4().hex[:8]
    fork = f"experiment:{sfx}:late"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen, (r1, r2, _p) = _seed_default(conn, clean_runtime, sfx)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [_references(r1, r2, durable=False)],
            source="default", strict_endpoints=True,
        )
        store.create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )

    out = _publisher(clean_runtime, fork).cycle()

    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["reason"] == "transform_consistency_violation", gate
    assert gate["preflight"]["dirty_edge_count"] == 1, gate


def _branch_from(
    store: ChronosGraphStore, name: str, parent: str, base_gen: str,
) -> None:
    store.create_branch(
        name, from_branch=parent,
        metadata={"base_branch": "default", "base_generation_id": base_gen},
    )


_EMPTY_SCOPE_HINT = ({}, {}, True, {}, {}, None)
_ZERO_COUNTS_HINT = {
    "nodes_added": 0, "nodes_retracted": 0, "nodes_updated": 0,
    "edges_added": 0, "edges_retracted": 0, "edges_updated": 0,
}


def test_fork_of_fork_ignores_empty_caller_hint(clean_runtime: str) -> None:
    # A caller's changed-row hint lists only the caller's own writes. On a
    # fork's first publish it cannot stand for the rows the parent wrote.
    sfx = uuid.uuid4().hex[:8]
    parent = f"experiment:{sfx}:parent"
    child = f"experiment:{sfx}:child"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen, (r1, r2, _p) = _seed_default(conn, clean_runtime, sfx)
        store = ChronosGraphStore.from_connection(conn)
        _branch_from(store, parent, "default", base_gen)
        store.apply_facts(
            [_references(r1, r2, durable=False)],
            branch=parent, source="default", strict_endpoints=True,
        )
        _branch_from(store, child, parent, base_gen)

    publisher = _publisher(clean_runtime, child)
    publisher.chronos_dirty_scope_hint = _EMPTY_SCOPE_HINT
    out = publisher.cycle()

    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["preflight"]["dirty_edge_count"] == 1, gate


def test_fork_of_fork_reads_the_visible_version_of_a_parent_row(
    clean_runtime: str,
) -> None:
    # The parent edits p and restores it; the child then re-types p so the
    # inherited reviewer->person edge breaks the narrowing. p has versions in
    # both segments, and only the one visible at the child's branch point
    # (the child's re-type) is its current state. Reading another version
    # compares an unchanged person and misses the violation.
    sfx = uuid.uuid4().hex[:8]
    parent = f"experiment:{sfx}:parent"
    child = f"experiment:{sfx}:child"
    r1, p = f"n:{sfx}:r1", f"n:{sfx}:p"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[REVIEWER, PERSON], narrowings=NARROWINGS)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(r1, REVIEWER, {}, {"id": r1}, {"v": 1}),
                NodeFact(p, PERSON, {}, {"id": p}, {"v": 1}),
                _references(r1, p, durable=False),
            ],
            source="default", strict_endpoints=True,
        )
        first = _publisher(clean_runtime, "default").cycle()
        assert first["status"] == "published", first
        base_gen = str(first["generation_id"])
        _branch_from(store, parent, "default", base_gen)
        store.apply_facts(
            [NodeFact(p, PERSON, {"x": 1}, {"id": p}, {"v": 2})],
            branch=parent, source="default", strict_endpoints=True,
        )
        store.apply_facts(
            [NodeFact(p, PERSON, {}, {"id": p}, {"v": 1})],
            branch=parent, source="default", strict_endpoints=True,
        )
        _branch_from(store, child, parent, base_gen)
        store.apply_facts(
            [NodeFact(p, REVIEWER, {}, {"id": p}, {"v": 3})],
            branch=child, source="default", strict_endpoints=True,
        )

    out = _publisher(clean_runtime, child).cycle()

    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["reason"] == "transform_consistency_violation", gate


def _default_with_prefork_violation(conn: Any, dsn: str, sfx: str) -> None:
    """Publish default, write a violating edge on it, then fork it.

    The fork moves default to a new segment, so the violating edge sits in
    default's previous segment when default publishes next.
    """
    base_gen, (r1, r2, _p) = _seed_default(conn, dsn, sfx)
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [_references(r1, r2, durable=False)],
        source="default", strict_endpoints=True,
    )
    _branch_from(store, f"experiment:{sfx}:x", "default", base_gen)


def test_default_publish_after_a_fork_checks_writes_made_before_it(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _default_with_prefork_violation(conn, clean_runtime, sfx)

    out = _publisher(clean_runtime, "default").cycle()

    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["reason"] == "transform_consistency_violation", gate
    assert gate["preflight"]["dirty_edge_count"] == 1, gate


def test_default_publish_after_a_fork_ignores_caller_hints(
    clean_runtime: str,
) -> None:
    # An ingest hint names only the rows that ingest wrote; the edge written
    # before the fork is not in it and must still be checked.
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _default_with_prefork_violation(conn, clean_runtime, sfx)

    publisher = _publisher(clean_runtime, "default")
    publisher.chronos_dirty_scope_hint = _EMPTY_SCOPE_HINT
    publisher.chronos_delta_counts_hint = dict(_ZERO_COUNTS_HINT)
    out = publisher.cycle()

    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["preflight"]["dirty_edge_count"] == 1, gate
