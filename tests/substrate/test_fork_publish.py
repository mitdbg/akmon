"""worktree-fork-writes / fork-publish-soundness: a fork's FIRST publish.

A fresh fork has no own published generation, so the consistency gate used to
full-validate the entire inherited base (re-tripping its latent violations and
blocking). delta-scoped-fork-publish resolves the fork's BASE generation as the
gate baseline + supplies a scoped delta, so the gate runs incrementally over the
fork's OWN delta and the fork publishes. A fork whose base cannot be resolved MUST
block loudly rather than silently full-validate. (The success path is also
exercised live on okg-workspace; these are the hermetic CI regressions.)
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _default_publisher(dsn: str) -> Publisher:
    return Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default"))


def _fork_publisher(dsn: str, fork: str) -> Publisher:
    return Publisher(
        dsn, "default", branch_name=fork,
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    )


def _has(session, node_id: str) -> bool:
    return len(list(session.query(
        "SELECT node_id FROM okg.graph_nodes WHERE node_id=%(n)s", {"n": node_id}
    ))) > 0


def _seed_and_publish_default(conn, dsn: str, *, base_nodes: list[str]) -> str:
    """Seed a catalog + several base nodes on default and publish; return the
    published base generation id (a Chronos checkpoint)."""
    seed_catalog(
        conn,
        subtypes=["ticket", "person"],
        narrowings=[("ticket", "mentions", "person")],
    )
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [NodeFact(n, "person", {}, {"id": n}, {"v": 1}) for n in base_nodes],
        source="default",
        strict_endpoints=True,
    )
    out = _default_publisher(dsn).cycle()
    assert out["status"] == "published", out
    return out["generation_id"]


def test_fork_first_publish_succeeds_scoped(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    base_nodes = [f"n:{sfx}:base{i}" for i in range(6)]
    forkn = f"n:{sfx}:fork"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen = _seed_and_publish_default(conn, clean_runtime, base_nodes=base_nodes)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        # Fork-local: a ticket + a valid edge to an INHERITED base person node.
        store.apply_facts(
            [
                NodeFact(forkn, "ticket", {}, {"id": forkn}, {"v": 1}),
                EdgeFact(forkn, base_nodes[0], "mentions",
                         source_record_id={"id": "e"}, source_revision={"v": 1}),
            ],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()

    # (a) the fork publishes (it would block on a full re-validate without the fix).
    assert out["status"] == "published", out
    # (b) the gate ran SCOPED over the fork delta, not the full inherited base.
    # The basis also names the cardinality family as SKIPPED: this fixture's
    # catalog carries no at-most-one-target annotation, so that family never
    # activated, and the basis states which families actually ran rather than
    # naming a check that did not.
    gate = out["consistency_gates"][0]
    assert gate["coverage_basis"] == (
        "chronos_dirty_edges+incident_dirty_nodes"
        "+skipped_families:cardinality"
    ), gate
    assert (gate.get("details") or {}).get("scope_fallback_reason") is None, gate
    basis = (gate.get("details") or {})["published_induction_basis"]
    assert basis["checkpoint_id"] == base_gen, basis
    assert basis["basis_branch"] == "default", basis
    assert basis["inherited_from_fork_base"] is True, basis
    # change receipt is the fork delta (a couple of nodes), NOT the ~7-node base.
    assert out["nodes_added"] <= 3, out
    # node_count stays the full copy-on-write size (base + fork).
    assert out["node_count"] >= len(base_nodes) + 1, out

    # (c) read isolation + copy-on-write read-through.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        assert _has(store.checkout(branch=fork), forkn)
        assert not _has(store.checkout(branch="default"), forkn)
        assert _has(store.checkout(branch=fork), base_nodes[0])


def test_fork_first_publish_catches_dangling_fork_edge(clean_runtime: str) -> None:
    # The scoped gate must still CATCH a real violation in the fork's delta -- it
    # blocks only the inherited base from re-validation, not the fork's own edges.
    sfx = uuid.uuid4().hex[:8]
    base_nodes = [f"n:{sfx}:base{i}" for i in range(3)]
    forkn = f"n:{sfx}:fork"
    missing = f"n:{sfx}:missing"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen = _seed_and_publish_default(conn, clean_runtime, base_nodes=base_nodes)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(forkn, "ticket", {}, {"id": forkn}, {"v": 1}),
                EdgeFact(forkn, missing, "mentions",
                         source_record_id={"id": "d"}, source_revision={"v": 1}),
            ],
            branch=fork,
            source="default",
            strict_endpoints=False,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()
    assert out["status"] == "blocked", out


def test_fork_first_publish_catches_retyped_inherited_endpoint(clean_runtime: str) -> None:
    # The fork writes NO edge -- it copy-on-write RE-TYPES an inherited node that an
    # INHERITED edge points to, turning that edge into a narrowing violation. The
    # scoped gate must diff the fork's dirty scope against the base (marking the
    # re-typed node __subtype__) so the inherited edge is pulled into scope and the
    # violation is caught -- otherwise the fork publishes a catalog-violating graph.
    sfx = uuid.uuid4().hex[:8]
    t = f"n:{sfx}:t"
    p = f"n:{sfx}:p"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person", "place"],
            narrowings=[("ticket", "mentions", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(t, p, "mentions",
                         source_record_id={"id": "e"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
        out0 = _default_publisher(clean_runtime).cycle()
        assert out0["status"] == "published", out0
        base_gen = out0["generation_id"]

        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        # Re-type the inherited person node to `place` on the fork (copy-on-write).
        store.apply_facts(
            [NodeFact(p, "place", {}, {"id": p}, {"v": 2})],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()
    # The inherited ticket--mentions-->place edge now violates narrowing; the scoped
    # gate must catch it.
    assert out["status"] == "blocked", out


def test_fork_read_isolation_through_mcp_surface(clean_runtime: str) -> None:
    # prove-isolation: a node written + published on a fork is visible through the
    # MCP READ surface (inspect_node, the operator the agent's `inspect` tool runs)
    # ONLY when the session is pinned to the fork branch -- not when pinned to
    # default. An inherited base node is visible from BOTH (copy-on-write
    # read-through), confirming the fork session isn't simply blind.
    from okg.substrate.mcp.session import open_session
    from okg.substrate.mcp.tools import inspect_node

    sfx = uuid.uuid4().hex[:8]
    base_nodes = [f"n:{sfx}:base{i}" for i in range(3)]
    forkn = f"n:{sfx}:fork"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen = _seed_and_publish_default(conn, clean_runtime, base_nodes=base_nodes)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [NodeFact(forkn, "person", {}, {"id": forkn}, {"v": 1})],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fork_session = open_session(
            conn, deployment_name="default", branch_name=fork,
        )
        default_session = open_session(
            conn, deployment_name="default", branch_name="default",
        )

        # The fork-written node is visible ONLY through the fork-pinned session.
        assert inspect_node(conn, fork_session, forkn) is not None
        assert inspect_node(conn, default_session, forkn) is None

        # Control: an inherited base node reads through on the fork AND is on
        # default -- so the isolation above is real, not a blind fork session.
        assert inspect_node(conn, fork_session, base_nodes[0]) is not None
        assert inspect_node(conn, default_session, base_nodes[0]) is not None


def test_fork_first_publish_blocks_unknown_node_subtype(clean_runtime: str) -> None:
    # The SCOPED node-subtype check (the incremental path a fork-first publish runs):
    # a fork node whose subtype is not in the active catalog blocks at the gate,
    # caught by _eval_node_subtype_scoped over the dirty node set. (Relevant to
    # merge-back: a fork node with a subtype default retired must not land.)
    sfx = uuid.uuid4().hex[:8]
    base_nodes = [f"n:{sfx}:base{i}" for i in range(3)]
    ghost = f"n:{sfx}:ghost"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen = _seed_and_publish_default(conn, clean_runtime, base_nodes=base_nodes)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [NodeFact(ghost, "ghost_subtype", {}, {"id": ghost}, {"v": 1})],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()
    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    # ran the SCOPED path and flagged the fork's unknown-subtype node.
    assert gate["coverage_basis"] == (
        "chronos_dirty_edges+incident_dirty_nodes"
        "+skipped_families:cardinality"
    ), gate
    assert ghost in (gate.get("details") or {}).get(
        "node_subtype_violation_node_ids", []
    ), gate


def test_fork_first_publish_blocks_retyped_inherited_node_to_unknown_subtype(
    clean_runtime: str,
) -> None:
    # The #134 analogue for the node-subtype gate, and the actual merge-back threat:
    # a fork copy-on-write RE-TYPES an INHERITED published node (no new edge) to a
    # subtype that default RETIRED. Unlike the brand-new-node case, this only blocks
    # if the pure re-type lands in the fork's dirty scope (diffed __subtype__ against
    # the fork baseline) and the scoped node-subtype antijoin runs over the FULL
    # dirty node set -- not just edge-incident nodes. Two nodes also pin the
    # multi-violation collection. (Both prior fork-gate P0s -- #129 vacuous-pass,
    # #134 re-typed-endpoint slip -- hid in exactly this shape; this is the guard.)
    sfx = uuid.uuid4().hex[:8]
    base_nodes = [f"n:{sfx}:base{i}" for i in range(3)]
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen = _seed_and_publish_default(conn, clean_runtime, base_nodes=base_nodes)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        # Re-type two inherited `person` nodes to subtypes absent from the catalog,
        # writing NO edge -- the pure node-re-type path.
        store.apply_facts(
            [
                NodeFact(base_nodes[0], "ghost_a", {}, {"id": base_nodes[0]}, {"v": 2}),
                NodeFact(base_nodes[1], "ghost_b", {}, {"id": base_nodes[1]}, {"v": 2}),
            ],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()
    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    # SCOPED path (not a full re-validate), and BOTH re-typed inherited nodes flagged.
    assert gate["coverage_basis"] == (
        "chronos_dirty_edges+incident_dirty_nodes"
        "+skipped_families:cardinality"
    ), gate
    flagged = (gate.get("details") or {}).get("node_subtype_violation_node_ids", [])
    assert base_nodes[0] in flagged, gate
    assert base_nodes[1] in flagged, gate


def test_fork_publish_blocks_on_unresolved_base(clean_runtime: str) -> None:
    # A fork created WITHOUT base_generation_id metadata cannot scope the gate; it
    # MUST block loudly with a distinct reason, never silently full-validate.
    sfx = uuid.uuid4().hex[:8]
    base_nodes = [f"n:{sfx}:base{i}" for i in range(3)]
    forkn = f"n:{sfx}:fork"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_and_publish_default(conn, clean_runtime, base_nodes=base_nodes)
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(fork, from_branch="default")  # NO base_generation_id
        store.apply_facts(
            [NodeFact(forkn, "ticket", {}, {"id": forkn}, {"v": 1})],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )

    out = _fork_publisher(clean_runtime, fork).cycle()
    assert out["status"] == "blocked", out
    assert out["blocked_reason"]["reason"] == "fork_base_generation_unresolved", out
