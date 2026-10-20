"""worktree-fork-writes / fork-publish-soundness: the TransformConsistencyGate
must evaluate the BRANCH BEING PUBLISHED, not always `default`.

Before this fix the gate's checkout fell back to `default` (store.checkout() with
no branch, against a synthetic gen token that is never a real checkpoint). A fork
publish therefore validated default -- where the fork's brand-new edges do not
exist -- so the scoped antijoin found zero candidate edges and passed VACUOUSLY,
admitting a genuinely-dangling fork edge. These tests pin that the gate now sees
the fork's own delta, catches a fork-local dangling edge, resolves an inherited
(copy-on-write) endpoint, and that a default-branch evaluation is blind to the
fork (the behavior the threading changes).
"""
from __future__ import annotations

import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore, _edge_id  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

# A synthetic generation token that is never a real Chronos checkpoint, so the
# gate's checkout_generation always raises and falls back to the branch head --
# exactly what the publisher passes (int(time.time_ns() // 1000)).
_SYN_GEN = 9_000_000_000_001


def _setup_fork_with_dangling_edge(
    conn, *, publish_base_dsn: str | None = None,
) -> tuple[str, int, int, int, Any]:
    """default has one inherited `person` base node; a fork adds a ticket plus a
    VALID edge (dst = inherited base node) and a DANGLING edge (dst absent).

    With ``publish_base_dsn``, default is published through the real publisher
    (running this gate) before the fork is created, and the fork records that
    generation as its base the way ``okg branch create`` does. The scoped gate
    needs both: since PR #1172 it scopes only on top of a verified full
    published result, which a fork inherits from its recorded base.

    Returns (fork_branch, catalog_version_id, valid_edge_id, dangling_edge_id,
    fork_dirty_scope), where fork_dirty_scope is the fork write's own
    ``chronos_dirty_scope()``.
    """
    sfx = uuid.uuid4().hex[:8]
    base = f"n:{sfx}:base"
    forkn = f"n:{sfx}:fork"
    missing = f"n:{sfx}:missing"
    fork = f"experiment:{sfx}"

    cv = seed_catalog(
        conn,
        subtypes=["ticket", "person"],
        narrowings=[("ticket", "mentions", "person")],
    )
    store = ChronosGraphStore.from_connection(conn)
    # Inherited base node, on default.
    store.apply_facts(
        [NodeFact(base, "person", {}, {"id": base}, {"v": 1})],
        source="default",
        strict_endpoints=True,
    )
    if publish_base_dsn is None:
        store.create_branch(fork, from_branch="default")
    else:
        published = Publisher(
            publish_base_dsn,
            "default",
            consistency_gates=[TransformConsistencyGate()],
            catalog_identity=bind_test_catalog_writer(publish_base_dsn, "default"),
        ).cycle()
        assert published["status"] == "published", published
        cv = int(published["catalog_version_id"])
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "base_branch": "default",
                "base_generation_id": published["generation_id"],
            },
        )
    # Fork-local: a node, a valid edge to the inherited base node, and a dangling
    # edge to a never-created node. strict_endpoints=False so the dangling edge is
    # actually written (the gate, not apply-time, is the thing under test).
    write_set = store.apply_facts(
        [
            NodeFact(forkn, "ticket", {}, {"id": forkn}, {"v": 1}),
            EdgeFact(forkn, base, "mentions",
                     source_record_id={"id": "valid"}, source_revision={"v": 1}),
            EdgeFact(forkn, missing, "mentions",
                     source_record_id={"id": "dangling"}, source_revision={"v": 1}),
        ],
        branch=fork,
        source="default",
        strict_endpoints=False,
    )
    return (
        fork,
        cv,
        _edge_id(forkn, "mentions", base),
        _edge_id(forkn, "mentions", missing),
        write_set.chronos_dirty_scope(),
    )


def test_full_gate_evaluates_fork_branch_not_default(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fork, cv, valid_eid, dangling_eid, _ = _setup_fork_with_dangling_edge(conn)
        gate = TransformConsistencyGate()  # block policy -> dangling appear in details

        ev_fork = gate.evaluate(
            conn, target_generation_id=_SYN_GEN, catalog_version_id=cv,
            deployment_name="default", branch_name=fork,
        )
        fork_dangling = set(ev_fork.details["dangling_edge_ids"])
        # The fork's dangling edge is CAUGHT...
        assert dangling_eid in fork_dangling, ev_fork.details
        # ...and the valid edge whose dst is an INHERITED base node resolves via
        # copy-on-write read-through (not a false dangling).
        assert valid_eid not in fork_dangling, ev_fork.details

        # A default-branch evaluation is BLIND to the fork's edges -- the old
        # branch-unaware behavior. This asserts the branch threading changes the
        # result (otherwise the fork publish would pass vacuously).
        ev_default = gate.evaluate(
            conn, target_generation_id=_SYN_GEN, catalog_version_id=cv,
            deployment_name="default", branch_name="default",
        )
        assert dangling_eid not in set(ev_default.details["dangling_edge_ids"]), \
            ev_default.details


def test_scoped_gate_evaluates_fork_branch_not_default(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fork, cv, valid_eid, dangling_eid, dirty_scope = (
            _setup_fork_with_dangling_edge(conn, publish_base_dsn=clean_runtime)
        )
        gate = TransformConsistencyGate()

        # The dirty scope is the fork write's own (its ticket node, its two
        # edges, and the attribute-change markers the scoped gate requires),
        # and the fork inherits a published full gate result from its recorded
        # base, so the incremental gate runs the SCOPED path rather than
        # falling back to full evaluation.
        assert {k: len(v) for k, v in dirty_scope[0].items()} == {"ticket": 1}
        assert sorted(dirty_scope[1]["mentions"]) == sorted([valid_eid, dangling_eid])
        delta_counts = {
            "nodes_added": 1, "nodes_retracted": 0, "nodes_updated": 0,
            "edges_added": 2, "edges_retracted": 0, "edges_updated": 0,
        }

        ev_fork = gate.evaluate_incremental(
            conn, target_generation_id=_SYN_GEN, catalog_version_id=cv,
            deployment_name="default", previous_catalog_version_id=cv,
            delta_counts=delta_counts, dirty_scope=dirty_scope, branch_name=fork,
        )
        # The SCOPED path engaged (no fallback to full eval)...
        assert ev_fork.details.get("scope_mode") == "dirty_edges+incident_dirty_nodes", \
            ev_fork.details
        fork_dangling = set(ev_fork.details["dangling_edge_ids"])
        assert dangling_eid in fork_dangling, ev_fork.details
        assert valid_eid not in fork_dangling, ev_fork.details

        # On default the same dirty scope finds ZERO candidate edges (the fork's
        # edge ids do not exist there) -> a vacuous pass over the fork's delta.
        ev_default = gate.evaluate_incremental(
            conn, target_generation_id=_SYN_GEN, catalog_version_id=cv,
            deployment_name="default", previous_catalog_version_id=cv,
            delta_counts=delta_counts, dirty_scope=dirty_scope, branch_name="default",
        )
        assert dangling_eid not in set(ev_default.details["dangling_edge_ids"]), \
            ev_default.details
