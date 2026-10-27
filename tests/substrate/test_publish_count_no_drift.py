"""The published checkpoint's node_count/edge_count equal the TRUE live graph
count at that generation — across multiple publishes, with no compounding drift.

Before accelerate-operator-read-path the publisher stamped ``prev + delta`` into
the checkpoint, which compounds drift across generations with no self-heal and
silently undercounts the graph (measured ~28% / ~17% low on okg-workspace:
732998 vs 1024056 nodes). The status / map read path serves these counts, so a
drifted count is a fast WRONG number. The publisher now stamps a running
counter (the previous stamp plus this cycle's delta) seeded and re-anchored by
a full Chronos count: on the first publish, after a blocked generation, and on
every OKG_COUNT_STAMP_SELF_HEAL_EVERY-th generation. The stamp equals the live
count every generation unless a baseline was already wrong; inherited drift is
carried until the next full-count cycle (okg#3112 S3 retired the attribute
receipt that used to refuse it).
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.consistency.transform_gate import TransformConsistencyGate
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.db.migrate import migrate
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _stamped_and_live(conn, gen: str) -> tuple[int, int, int, int]:
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    meta = dict(getattr(store.get_generation(gen), "metadata", None) or {})
    g = store.checkout_generation(gen)
    live_nodes = int(g.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0)
    live_edges = int(g.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0)
    return int(meta["node_count"]), int(meta["edge_count"]), live_nodes, live_edges


def _publish(dsn: str) -> str:
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def _publish_blocked(dsn: str) -> str:
    """A cycle whose nomos publish-blocker trips AFTER the snapshot is materialized,
    producing a BLOCKED generation with a real pinned graph + stamped counts."""
    import okg.substrate.publisher.publisher as pub_mod

    pub = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"))
    original = pub_mod.Publisher._nomos_generation_publish_blocker
    pub_mod.Publisher._nomos_generation_publish_blocker = (
        lambda self, *a, **k: {"reason": "test_forced_block"}
    )
    try:
        result = pub.cycle()
    finally:
        pub_mod.Publisher._nomos_generation_publish_blocker = original
    assert result["status"] == "blocked", result
    return str(result["generation_id"])


def _publish_out(dsn: str) -> dict:
    """Publish and return the WHOLE cycle receipt, so a test can read the
    segment-delta classification (nodes_added / nodes_retracted / nodes_updated)
    and not only the stamped count."""
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    return dict(published)


def _apply(dsn: str, facts: list) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )


def _live_nodes(dsn: str, gen: str) -> int:
    """Count the live nodes at a pinned generation DIRECTLY from the graph.

    This is the independent oracle. The running counter must be checked against
    this, never against its own arithmetic.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        g = store.checkout_generation(gen)
        return int(g.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0)


def _stamped_nodes(dsn: str, gen: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        meta = dict(getattr(store.get_generation(gen), "metadata", None) or {})
        return int(meta["node_count"])


def _node_interval_rows(dsn: str, node_id: str) -> list[tuple]:
    """Every physical interval row chronos holds for one node id.

    Reads the interval table through the ``_chronos_branch_tables`` registry
    because the physical name is a hashed suffix, not a fixed string. Used to
    MEASURE whether a write actually landed a row rather than assuming it did:
    ``okg.graph_nodes`` is a checkout view and shows only what is live, so it
    cannot tell a deduplicated write apart from a second tombstone.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE table_name LIKE %s",
                ("%graph_nodes%",),
            ).fetchall()
        ]
        rows: list[tuple] = []
        for table in tables:
            rows.extend(
                conn.execute(
                    "SELECT deleted, live_lo, live_hi, writer_segment_id, attrs "
                    f'FROM "{table}" WHERE node_id = %s '
                    "ORDER BY live_lo, live_hi",
                    (node_id,),
                ).fetchall()
            )
    return rows


def _running_counter_gen(dsn: str, gen: str) -> None:
    """Assert this generation's count came from the RUNNING COUNTER, not a full
    scan. Without this, a self-heal/full-scan cadence would silently make the
    resurrection assertions vacuous -- they would pass on the broken classifier
    because the count never went through prev + added - retracted."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        meta = dict(getattr(store.get_generation(gen), "metadata", None) or {})
    assert "count_stamp_cadence" in meta, (
        f"generation {gen} carries no count_stamp_cadence; cannot tell whether the "
        "running-counter path was exercised"
    )
    cadence = int(meta["count_stamp_cadence"] or 0)
    assert cadence > 0, (
        f"generation {gen} stamped a FULL-SCAN count (count_stamp_cadence=0); "
        "the running-counter path this test exists to cover was not exercised"
    )


def test_checkpoint_counts_equal_live_graph_across_generations(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    migrate(dsn)
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person"],
            narrowings=[("case", "reported_by", "person")],
        )

    c1, p1 = f"case:1:{sfx}", f"person:1:{sfx}"
    c2, p2 = f"case:2:{sfx}", f"person:2:{sfx}"
    c3 = f"case:3:{sfx}"

    # Gen 1: 2 nodes + 1 edge (first publish — full count path).
    _apply(dsn, [
        NodeFact(c1, "case", {"x": 1}, {"id": c1}, {"v": 1}),
        NodeFact(p1, "person", {"x": 1}, {"id": p1}, {"v": 1}),
        EdgeFact(c1, p1, "reported_by", attrs={},
                 source_record_id={"id": f"e1:{sfx}"}, source_revision={"v": 1}),
    ])
    gen1 = _publish(dsn)

    # Gen 2: +2 nodes +1 edge (steady-state prev+delta path — where drift lived).
    _apply(dsn, [
        NodeFact(c2, "case", {"x": 1}, {"id": c2}, {"v": 1}),
        NodeFact(p2, "person", {"x": 1}, {"id": p2}, {"v": 1}),
        EdgeFact(c2, p2, "reported_by", attrs={},
                 source_record_id={"id": f"e2:{sfx}"}, source_revision={"v": 1}),
    ])
    gen2 = _publish(dsn)

    # Gen 3: update an existing node (no count change) + add 1 node (exercise the
    # update + add delta paths together).
    _apply(dsn, [
        NodeFact(c1, "case", {"x": 2}, {"id": c1}, {"v": 2}),
        NodeFact(c3, "case", {"x": 1}, {"id": c3}, {"v": 1}),
    ])
    gen3 = _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        results = {}
        for label, gen in (("gen1", gen1), ("gen2", gen2), ("gen3", gen3)):
            sn, se, ln, le = _stamped_and_live(conn, gen)
            # The invariant: the stamped count equals the live graph, every gen.
            assert sn == ln, f"{label}: checkpoint node_count {sn} != live {ln}"
            assert se == le, f"{label}: checkpoint edge_count {se} != live {le}"
            results[label] = (sn, se)

    # Totals grew monotonically with the adds (sanity the counts are real, not 0).
    assert results["gen1"][0] < results["gen2"][0] < results["gen3"][0], results
    assert results["gen1"][1] < results["gen2"][1], results  # +1 edge at gen2
    assert results["gen3"][0] >= 5, results  # 5 nodes by gen3


def test_checkpoint_counts_equal_live_graph_after_blocked_generation(
    clean_runtime: str,
) -> None:
    """A generation published immediately after a BLOCKED generation still stamps the
    TRUE live count. The incremental materialize carries forward the last PUBLISHED
    snapshot but applies the dirty scope since the last CHECKPOINT (the blocked gen),
    so it would DROP the blocked gen's change (stamp too low by that delta) -- a P0
    wrong number served by generation_counts. The publisher forces a full-scan
    materialize for the post-blocked-gen cycle to keep the invariant."""
    dsn = clean_runtime
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case", "person"], narrowings=[])

    n1, n2, n3 = f"case:1:{sfx}", f"case:2:{sfx}", f"case:3:{sfx}"

    # Gen 1 (published).
    _apply(dsn, [NodeFact(n1, "case", {"x": 1}, {"id": n1}, {"v": 1})])
    gen1 = _publish(dsn)
    # Gen 2 (BLOCKED) -- adds a node that the next gen's carry-forward would drop.
    _apply(dsn, [NodeFact(n2, "case", {"x": 1}, {"id": n2}, {"v": 1})])
    gen2 = _publish_blocked(dsn)
    # Gen 3 (published, right after the block) -- adds one more node.
    _apply(dsn, [NodeFact(n3, "case", {"x": 1}, {"id": n3}, {"v": 1})])
    gen3 = _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        for label, gen in (("gen1", gen1), ("gen2(blocked)", gen2), ("gen3", gen3)):
            sn, se, ln, le = _stamped_and_live(conn, gen)
            assert sn == ln, f"{label}: checkpoint node_count {sn} != live {ln}"
            assert se == le, f"{label}: checkpoint edge_count {se} != live {le}"
        # Specifically: gen3's stamp counts the blocked gen2's node too (3, not 2).
        sn3, _, ln3, _ = _stamped_and_live(conn, gen3)
        assert sn3 == ln3 == 3, (sn3, ln3)


def test_prune_gate_stamps_post_gate_count_on_non_cadence_generation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-cycle prune is reflected immediately, not at the next cadence scan."""
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    sfx = uuid.uuid4().hex[:8]
    case1, case2 = f"case:1:{sfx}", f"case:2:{sfx}"
    person1, person2 = f"person:1:{sfx}", f"person:2:{sfx}"
    ghost = f"person:missing:{sfx}"
    gate = TransformConsistencyGate(dangling_policy="prune")

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person"],
            narrowings=[("case", "reported_by", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(case1, "case", {}, {"id": case1}, {"v": 1}),
                NodeFact(person1, "person", {}, {"id": person1}, {"v": 1}),
                EdgeFact(
                    case1,
                    person1,
                    "reported_by",
                    source_record_id={"id": f"baseline:{sfx}"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    first = Publisher(dsn, "default", consistency_gates=[gate], catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert first["status"] == "published", first

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(case2, "case", {}, {"id": case2}, {"v": 1}),
                NodeFact(person2, "person", {}, {"id": person2}, {"v": 1}),
                EdgeFact(
                    case2,
                    person2,
                    "reported_by",
                    source_record_id={"id": f"valid:{sfx}"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    case2,
                    ghost,
                    "reported_by",
                    source_record_id={"id": f"dangling:{sfx}"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=False,
        )

    second = Publisher(dsn, "default", consistency_gates=[gate], catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert second["status"] == "published", second
    assert "count_stamp_post_consistency_gate_full_scan" in second["timings_ms"]
    assert "count_stamp_self_heal_full_scan" not in second["timings_ms"]

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation = store.get_generation(str(second["generation_id"]))
        metadata = dict(getattr(generation, "metadata", None) or {})
        stamped_nodes, stamped_edges, live_nodes, live_edges = _stamped_and_live(
            conn,
            str(second["generation_id"]),
        )

    assert metadata["count_stamp_cadence"] == 1
    assert (stamped_nodes, stamped_edges) == (live_nodes, live_edges)
    assert stamped_edges == int(first["edge_count"]) + 1
    receipt = metadata["projection_deltas"]
    assert receipt["edges_projected_net"] == 2
    assert receipt["consistency_gate_mutations"] == {
        "dangling_edges_pruned": 1,
        "orphaned_derived_edges_retracted": 0,
        "unsupported_derived_nodes_retracted": 0,
        "unsupported_derived_incident_edges_retracted": 0,
        "nodes_removed": 0,
        "edges_removed": 1,
    }
    assert receipt["edges_checkpoint_visible_net"] == 1
    assert second["noop"] is False
    assert receipt["noop"] is False


def test_publish_after_blocked_generation_without_new_writes_does_not_reuse_old_gen(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])

    n1, n2 = f"case:1:{sfx}", f"case:2:{sfx}"

    _apply(dsn, [NodeFact(n1, "case", {"x": 1}, {"id": n1}, {"v": 1})])
    gen1 = _publish(dsn)
    _apply(dsn, [NodeFact(n2, "case", {"x": 1}, {"id": n2}, {"v": 1})])
    gen2 = _publish_blocked(dsn)

    gen3 = _publish(dsn)

    assert gen3 != gen1
    assert gen3 != gen2
    with psycopg.connect(dsn, autocommit=True) as conn:
        sn3, se3, ln3, le3 = _stamped_and_live(conn, gen3)
    assert (sn3, se3) == (ln3, le3) == (2, 0)


def test_a_deployment_already_carrying_drift_is_corrected_by_a_full_scan_generation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The correction path for deployments that drifted BEFORE this fix landed.

    History is never rewritten -- a wrong checkpoint stays wrong, by design. What
    must be true is that the deployment stops SERVING the wrong number, on its
    own, with no operator intervention, within the bounded self-heal cadence.

    okg#3112 S3 removed the attribute receipt that used to refuse the first
    incremental publish over a drifted stamp (its count came from the
    interval copy this change drops). What bounds inherited drift now is the
    full-count self-heal alone: every OKG_COUNT_STAMP_SELF_HEAL_EVERY-th
    published generation recounts the graph and re-stamps the true count
    (count_stamp_cadence == 0). Observed here with a cadence of 2:

    * the incremental publish right after the drift carries it (the running
      counter adds this cycle's delta to the bad baseline);
    * the next publish is the cadence's full-count cycle and stamps the true
      live count;
    * counting then resumes on the running counter, truthful again.
    """
    dsn = clean_runtime
    migrate(dsn)
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])

    # The first publish stamps cadence 0, so position 1 is incremental and
    # position 2 is the full-count self-heal.
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "2")
    ids = [f"case:{i}:{sfx}" for i in range(5)]
    _apply(dsn, [
        NodeFact(nid, "case", {"x": 1}, {"id": nid}, {"v": 1}) for nid in ids
    ])
    gen1 = _publish(dsn)
    assert _stamped_nodes(dsn, gen1) == _live_nodes(dsn, gen1) == 5

    # Simulate a checkpoint stamped by the pre-fix publisher, two nodes low.
    # Raw SQL is test setup; production never edits checkpoint metadata.
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        drifted = dict(store.get_generation(gen1).metadata or {})
        drifted["node_count"] = int(drifted["node_count"]) - 2
        conn.execute(
            "UPDATE public._chronos_branch_interval_checkpoints "
            "SET metadata = %s WHERE checkpoint_id = %s",
            (json.dumps(drifted), gen1),
        )
    assert _stamped_nodes(dsn, gen1) == 3 != _live_nodes(dsn, gen1)

    # (1) the next incremental publish carries the bad baseline: 3 + 1, not 6.
    _apply(dsn, [
        NodeFact(f"case:next:{sfx}", "case", {"x": 1},
                 {"id": f"case:next:{sfx}"}, {"v": 1}),
    ])
    carried = _publish(dsn)
    _running_counter_gen(dsn, carried)
    assert (_stamped_nodes(dsn, carried), _live_nodes(dsn, carried)) == (4, 6)

    # (2) AUTOMATIC recovery: the cadence's full-count cycle. No operator.
    _apply(dsn, [
        NodeFact(f"case:heal:{sfx}", "case", {"x": 1},
                 {"id": f"case:heal:{sfx}"}, {"v": 1}),
    ])
    healed = _publish(dsn)
    stamped, live = _stamped_nodes(dsn, healed), _live_nodes(dsn, healed)
    assert stamped == live == 7, (stamped, live)
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        healed_meta = dict(store.get_generation(healed).metadata or {})
    assert int(healed_meta["count_stamp_cadence"]) == 0, (
        "recovery must come from a FULL-SCAN generation, not from the running "
        f"counter continuing off a bad baseline (cadence "
        f"{healed_meta.get('count_stamp_cadence')})"
    )

    # (3) and the deployment resumes normal incremental counting, still truthful.
    _apply(dsn, [
        NodeFact(f"case:after:{sfx}", "case", {"x": 1},
                 {"id": f"case:after:{sfx}"}, {"v": 1}),
    ])
    resumed = _publish(dsn)
    _running_counter_gen(dsn, resumed)
    assert _stamped_nodes(dsn, resumed) == _live_nodes(dsn, resumed) == 8

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        retained = dict(store.get_generation(gen1).metadata or {})
    assert retained == drifted, "history must not be rewritten"


def test_resurrected_node_is_classified_added_and_recovers_the_count(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A node retracted in one generation and re-inserted in the next must come
    back in the stamped count.

    The interval-tracked stamp is a RUNNING COUNTER (previous_count + nodes_added
    - nodes_retracted). Its classifier asked whether a previous row EXISTED at the
    prior branch point, not whether that row was LIVE. A tombstone is a row, so a
    resurrection matched `p.graph_key IS NOT NULL` and was booked as an UPDATE,
    which contributes zero -- the counter never recovered the node and served a
    count one low until the next full-count cadence.
    """
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    dsn = clean_runtime
    migrate(dsn)
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])

    n1, n2, n3 = f"case:1:{sfx}", f"case:2:{sfx}", f"case:3:{sfx}"

    # Gen 1: three live nodes (first publish -- full count path).
    _apply(dsn, [
        NodeFact(n1, "case", {"x": 1}, {"id": n1}, {"v": 1}),
        NodeFact(n2, "case", {"x": 1}, {"id": n2}, {"v": 1}),
        NodeFact(n3, "case", {"x": 1}, {"id": n3}, {"v": 1}),
    ])
    gen1 = _publish_out(dsn)
    assert _stamped_nodes(dsn, gen1["generation_id"]) == 3

    # Gen 2: retract one -> two live nodes, one tombstone.
    _apply(dsn, [
        NodeFact(n3, "case", {}, {"id": n3}, {"v": 2}, op="R"),
    ])
    gen2 = _publish_out(dsn)
    _running_counter_gen(dsn, gen2["generation_id"])
    assert int(gen2["nodes_retracted"]) == 1, gen2
    assert _stamped_nodes(dsn, gen2["generation_id"]) == 2

    # Gen 3: the same node id comes back. Its previous row is the TOMBSTONE.
    _apply(dsn, [
        NodeFact(n3, "case", {"x": 9}, {"id": n3}, {"v": 3}),
    ])
    gen3 = _publish_out(dsn)
    _running_counter_gen(dsn, gen3["generation_id"])

    # The classification, not just the total: a resurrection is an ADD.
    assert int(gen3["nodes_added"]) == 1, (
        "resurrection must be classified nodes_added, got "
        f"added={gen3['nodes_added']} updated={gen3['nodes_updated']} "
        f"retracted={gen3['nodes_retracted']}"
    )
    assert int(gen3["nodes_updated"]) == 0, gen3
    assert int(gen3["nodes_retracted"]) == 0, gen3

    # The total, checked against the graph itself rather than its own arithmetic.
    stamped = _stamped_nodes(dsn, gen3["generation_id"])
    live = _live_nodes(dsn, gen3["generation_id"])
    assert live == 3, f"reproducer setup wrong: live nodes {live} != 3"
    assert stamped == live, (
        f"stamped node_count {stamped} != live graph count {live} "
        "after retract-then-reinsert"
    )


def test_re_retracting_a_tombstoned_node_is_deduplicated_before_the_classifier(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Why the symmetric face of the defect is NOT reachable end to end.

    The classifier's `retracted` bucket was `d.deleted AND p.graph_key IS NOT
    NULL`. A tombstone IS a previous row, so re-retracting an already-tombstoned
    node satisfied it a second time and would decrement the running counter twice
    for one node. That is real in the SQL, and the witness for it is
    tests/substrate/test_segment_delta_classifier.py::
    test_re_retracting_a_tombstoned_row_does_not_decrement_again, which drives the
    query directly and does fail pre-fix.

    This test is NOT that witness and does not pretend to be. What it pins down is
    the reason the face cannot be reached through the pipeline, MEASURED here
    rather than asserted: repeating a retraction of an already-tombstoned node --
    even with a bumped source revision -- writes no new interval row at all. The
    cycle reports `noop` and REUSES the prior generation id rather than minting
    one, so the classifier is never handed a dirty tombstone and there is nothing
    to misclassify. A bumped revision does NOT defeat the dedup, contrary to what
    a revision-keyed write path might suggest.

    Those are the load-bearing assertions below. If that upstream dedup ever
    changes -- if a repeated retraction starts landing a second tombstone -- this
    test fails at the noop / reused-generation assertions, which is precisely the
    signal that the symmetric face has become reachable and needs an end-to-end
    witness of its own.

    Note what this test therefore CANNOT do. Because the third cycle is a noop
    that reuses the previous generation, any count or delta read for it is really
    a read of the SECOND generation. Asserting `nodes_retracted == 0` on that
    receipt would be vacuous: the noop branch returns zeros regardless of what
    the classifier believes.
    """
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    dsn = clean_runtime
    migrate(dsn)
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])

    n1, n2, n3 = f"case:1:{sfx}", f"case:2:{sfx}", f"case:3:{sfx}"

    _apply(dsn, [
        NodeFact(n1, "case", {"x": 1}, {"id": n1}, {"v": 1}),
        NodeFact(n2, "case", {"x": 1}, {"id": n2}, {"v": 1}),
        NodeFact(n3, "case", {"x": 1}, {"id": n3}, {"v": 1}),
    ])
    gen1 = _publish_out(dsn)
    assert _stamped_nodes(dsn, gen1["generation_id"]) == 3

    _apply(dsn, [NodeFact(n3, "case", {}, {"id": n3}, {"v": 2}, op="R")])
    gen2 = _publish_out(dsn)
    _running_counter_gen(dsn, gen2["generation_id"])
    assert int(gen2["nodes_retracted"]) == 1, gen2

    # Retract the SAME already-tombstoned node again, with a FRESH revision.
    # Capture the node's physical interval rows either side of the write, so the
    # claim "no row lands" is measured and not assumed.
    rows_before = _node_interval_rows(dsn, n3)
    _apply(dsn, [NodeFact(n3, "case", {}, {"id": n3}, {"v": 3}, op="R")])
    rows_after = _node_interval_rows(dsn, n3)
    assert rows_after == rows_before, (
        "a repeated retraction wrote a new interval row; the upstream dedup that "
        "makes the symmetric double-decrement unreachable no longer holds, so "
        "that face now needs an end-to-end witness. "
        f"before={rows_before} after={rows_after}"
    )

    gen3 = _publish_out(dsn)
    # The cycle has nothing to publish, so it reuses the previous generation.
    assert gen3["noop"] is True, gen3
    assert gen3["generation_id"] == gen2["generation_id"], (
        "the noop cycle minted a NEW generation; every count read below would "
        "then be a real read of a new classification rather than a re-read of "
        f"gen2. gen2={gen2['generation_id']} gen3={gen3['generation_id']}"
    )
    # The observable count is still right -- but note this reads gen2, which the
    # assertion above establishes. It is a consistency check on the reused
    # generation, NOT evidence about the classifier on a fresh one.
    stamped = _stamped_nodes(dsn, gen2["generation_id"])
    live = _live_nodes(dsn, gen2["generation_id"])
    assert live == 2, f"reproducer setup wrong: live nodes {live} != 2"
    assert stamped == live, (
        f"stamped node_count {stamped} != live graph count {live} "
        "after the first retraction"
    )


def test_identical_republish_without_retraction_is_unchanged(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The case that already worked stays working.

    Nothing is ever tombstoned here, so no predicate should behave differently
    after the fix. This is the guard against 'fixing' one predicate by moving the
    error into another.
    """
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    dsn = clean_runtime
    migrate(dsn)
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])

    ids = [f"case:{i}:{sfx}" for i in (1, 2, 3)]
    for revision in (1, 2, 3):
        _apply(dsn, [
            NodeFact(nid, "case", {"x": revision}, {"id": nid}, {"v": revision})
            for nid in ids
        ])
        out = _publish_out(dsn)
        stamped = _stamped_nodes(dsn, out["generation_id"])
        live = _live_nodes(dsn, out["generation_id"])
        assert stamped == live == 3, (revision, stamped, live)
        if revision > 1:
            _running_counter_gen(dsn, out["generation_id"])
            assert int(out["nodes_added"]) == 0, (revision, out)
            assert int(out["nodes_retracted"]) == 0, (revision, out)
            assert int(out["nodes_updated"]) == 3, (revision, out)


def test_running_counter_tracks_live_count_across_retract_reinsert_re_retract(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full sequence, with the counter checked against a DIRECT live count at
    every generation rather than against its own arithmetic."""
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    dsn = clean_runtime
    migrate(dsn)
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])

    n1, n2, n3 = f"case:1:{sfx}", f"case:2:{sfx}", f"case:3:{sfx}"
    steps: list[tuple[str, list, int]] = [
        ("seed", [
            NodeFact(n1, "case", {"x": 1}, {"id": n1}, {"v": 1}),
            NodeFact(n2, "case", {"x": 1}, {"id": n2}, {"v": 1}),
            NodeFact(n3, "case", {"x": 1}, {"id": n3}, {"v": 1}),
        ], 3),
        ("retract", [NodeFact(n3, "case", {}, {"id": n3}, {"v": 2}, op="R")], 2),
        ("reinsert", [NodeFact(n3, "case", {"x": 2}, {"id": n3}, {"v": 3})], 3),
        ("re-retract", [NodeFact(n3, "case", {}, {"id": n3}, {"v": 4}, op="R")], 2),
        ("re-retract-again",
         [NodeFact(n3, "case", {}, {"id": n3}, {"v": 5}, op="R")], 2),
        ("reinsert-again",
         [NodeFact(n3, "case", {"x": 3}, {"id": n3}, {"v": 6})], 3),
    ]

    observed: list[tuple[str, int, int]] = []
    for index, (label, facts, expected_live) in enumerate(steps):
        _apply(dsn, facts)
        out = _publish_out(dsn)
        gen = out["generation_id"]
        stamped = _stamped_nodes(dsn, gen)
        live = _live_nodes(dsn, gen)
        observed.append((label, stamped, live))
        assert live == expected_live, (
            f"{label}: live graph count {live} != expected {expected_live}; "
            f"sequence so far {observed}"
        )
        assert stamped == live, (
            f"{label}: stamped node_count {stamped} != live graph count {live}; "
            f"sequence so far {observed}"
        )
        # Every generation after the seed must have come from the RUNNING
        # COUNTER, not a full scan. Without this the stamped == live assertions
        # above would pass vacuously on the broken classifier: a full-scan
        # generation reads the truth straight from the graph and never goes
        # through prev + added - retracted, so it cannot witness the defect.
        # The seed is the first publish and takes the full-count path by design.
        if index > 0:
            _running_counter_gen(dsn, gen)
