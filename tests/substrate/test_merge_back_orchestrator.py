"""Merge-back orchestrator (worktree-fork-merge-back: staging-and-fast-forward) end-to-end.

A clean fork merges to default and lands (via stage -> gate -> LAND-by-replay), with no
phantom-retraction on the next default publish; a gate-violating fork BLOCKS at the staging
gate, leaving default unchanged; and repeated merge-backs do not exhaust the interval budget
(the reason LAND replays instead of fast-forwarding).
"""
from __future__ import annotations

import threading
from uuid import uuid4

import pytest

import okg.substrate.merge_back as merge_back_module
from okg.substrate.consistency import ConsistencyPublishGateEvaluation
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.merge_guard import MergeDecision
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres
merge_back = merge_back_module.merge_back

# A named-human decision authorizing landing on default (merge_guard decider-boundary).
DECIDER = MergeDecision(decided_by="alice@example.edu", decision="accept", fast_track=False)


@pytest.fixture(autouse=True)
def merge_back_test_deployment(tmp_path, monkeypatch):
    """Resolve the synthetic ``default`` deployment for merge_back's factory.

    merge_back builds its GATE and LAND publishers through the deployment
    factory, which needs catalog writer authority and a deployment directory
    inside a Git repository. Without this every merge_back call here refused
    with "publisher has no resolved catalog writer authority" (issue #2866).
    """
    from tests.pytest.catalog_fixture_builders import bind_factory_test_deployment

    return bind_factory_test_deployment(monkeypatch, tmp_path, "default")


def _default_publisher(dsn: str):
    from okg.substrate.consistency import TransformConsistencyGate
    from okg.substrate.publisher import Publisher

    return Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default"))


def _default_node_ids(store: ChronosGraphStore) -> set[str]:
    return {
        r["node_id"]
        for r in store.checkout("default").query("SELECT node_id FROM okg.graph_nodes")
    }


def _node_row(store: ChronosGraphStore, branch: str, node_id: str) -> dict | None:
    rows = store.checkout(branch).query(
        """
        SELECT node_id, subtype, attrs, source, source_record_id, source_revision
          FROM okg.graph_nodes
         WHERE node_id = :node_id
        """,
        {"node_id": node_id},
    )
    return rows[0] if rows else None


def _edge_row(
    store: ChronosGraphStore,
    branch: str,
    *,
    src: str,
    dst: str,
    edge_type: str,
) -> dict | None:
    rows = store.checkout(branch).query(
        """
        SELECT src, dst, edge_type, attrs, source, source_record_id, source_revision
          FROM okg.graph_edges
         WHERE src = :src
           AND dst = :dst
           AND edge_type = :edge_type
        """,
        {"src": src, "dst": dst, "edge_type": edge_type},
    )
    return rows[0] if rows else None


def _branch_exists(store: ChronosGraphStore, branch: str) -> bool:
    return store.context.db.execute(
        "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = ?", (branch,)
    ).fetchone() is not None


def _patch_merge_back_publisher_stack(
    monkeypatch,
    *,
    enrichers=(),
    gates=(),
) -> None:
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    def setup(dsn, deployment_name):
        return (
            build_deployment_publisher_stack(
                dsn=dsn,
                deployment_name=deployment_name,
            ),
            {
                "enrichers": list(enrichers),
                "consistency_gates": list(gates),
                "derived_producer_policies": {},
            },
        )

    monkeypatch.setattr(
        merge_back_module,
        "_merge_back_publisher_setup",
        setup,
    )


def _insert_derived_edge_fact(cur, *, run_id: str, dedupe_key: str) -> int:
    cur.execute(
        """
        INSERT INTO okg.edge_facts
          (edge_id, src, dst, edge_type, attrs, provenance, confidence,
           source, source_record_id, source_revision, observed_at,
           dedupe_key, run_id, op)
        VALUES
          (nextval('okg.edge_facts_edge_id_seq'), 'n:base', 'n:merged',
           'relates_to', '{}'::jsonb, 'derived_deterministic', 1.0,
           '_enricher:mergeback_link',
           '{"id": "cleanup-test"}'::jsonb,
           '{"v": 1}'::jsonb,
           now(), %s, %s::uuid, 'I')
        RETURNING fact_id
        """,
        (dedupe_key, run_id),
    )
    return int(cur.fetchone()[0])


def _insert_derived_node_fact(cur, *, run_id: str, dedupe_key: str) -> int:
    cur.execute(
        """
        INSERT INTO okg.node_facts
          (node_id, subtype, attrs, source, source_record_id, source_revision,
           observed_at, dedupe_key, run_id, op)
        VALUES
          ('n:derived', 'person', '{}'::jsonb, '_enricher:mergeback_link',
           '{"id": "cleanup-test"}'::jsonb,
           '{"v": 1}'::jsonb,
           now(), %s, %s::uuid, 'I')
        RETURNING fact_id
        """,
        (dedupe_key, run_id),
    )
    return int(cur.fetchone()[0])


def _record_gate_edge_range(
    cur,
    *,
    gate_branch: str,
    run_id: str,
    fact_id: int,
) -> None:
    cur.execute(
        """
        WITH envelope AS (
          INSERT INTO okg.admission_envelopes
            (deployment_name, producer_id, producer_kind, intent,
             admission_mode, status, source, run_id, idempotency_key,
             authority, check_summary, verdict, workflow_ref,
             target_branch_name)
          VALUES
            ('default', '_enricher:mergeback_link', 'enricher',
             'derived_edge_emission', 'direct', 'accepted',
             '_enricher:mergeback_link', %s::uuid, %s,
             '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, %s)
          RETURNING admission_id
        )
        INSERT INTO okg.admission_record_refs
          (admission_id, ref_key, ref_kind, source, run_id,
           fact_id_min, fact_id_max, record_count, payload)
        SELECT admission_id, %s, 'edge_fact_range',
               '_enricher:mergeback_link', %s::uuid, %s, %s, 1, '{}'::jsonb
          FROM envelope
        """,
        (
            run_id,
            f"merge-back-gate-cleanup:{run_id}",
            gate_branch,
            f"edge_fact_range:_enricher:mergeback_link:{run_id}:{fact_id}",
            run_id,
            fact_id,
            fact_id,
        ),
    )


class _BranchSensitiveGate:
    name = "merge-back-branch-sensitive-test"

    def __init__(self, *, block_default: bool = False) -> None:
        self.block_default = block_default
        self.calls: list[str] = []

    def evaluate(
        self,
        conn,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        del conn, catalog_version_id, deployment_name
        self.calls.append(branch_name)
        blocked = self.block_default and branch_name == "default"
        return ConsistencyPublishGateEvaluation(
            run_id=uuid4(),
            check_id=self.name,
            target_generation_id=target_generation_id,
            gate_name=self.name,
            result="evaluated",
            blocks_publish=blocked,
            reason="test_default_block" if blocked else "ok",
            open_observation_count=1 if blocked else 0,
            coverage_complete=True,
            coverage_basis="exact_postgres",
            budget_ms=1000,
            elapsed_ms=1,
        )


class _ConcurrentDefaultWriterProbe:
    name = "merge_back_lock_probe"
    reads_subtypes = ()
    reads_edge_types = ()
    emits_edge_types = ()
    requires_narrowings = ()
    # Explicit minimal envelope: the boot guard demands a declared write
    # surface. Truthful here — the probe's concurrent thread writes a
    # person node; the behavior under test is lock contention, not
    # admission.
    write_envelope = {
        "nodes": [{"subtype": "person"}],
        "nondeterministic": False,
    }
    # The probe writes its person node straight to the store on a concurrent
    # thread, not through the enricher node emitter, so it lands as a source
    # write rather than a derived one and carries no derived provenance to
    # declare. Behavior under test is lock contention, not admission.
    declares_node_provenance = True

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.results: list[str] = []

    def enrich(self, conn, *, generation_id: int, incremental=None) -> EnrichResult:
        del generation_id, incremental
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('okg.chronos_branch', true)")
            branch_name = cur.fetchone()[0]
        if branch_name != "default":
            return EnrichResult(self.name, n_edges_emitted=0)

        def write_default() -> None:
            import psycopg

            try:
                with psycopg.connect(self.dsn, autocommit=True) as other:
                    other.execute("SET lock_timeout = '200ms'")
                    other_store = ChronosGraphStore.from_connection(other)
                    other_store.apply_facts(
                        [
                            NodeFact(
                                "n:concurrent",
                                "person",
                                {},
                                {"id": "n:concurrent"},
                                {"v": 1},
                            ),
                        ],
                        source="default",
                        strict_endpoints=True,
                    )
            except Exception as exc:  # noqa: BLE001 - exact driver type is not stable
                self.results.append(f"blocked:{type(exc).__name__}:{exc}")
            else:
                self.results.append("wrote")

        thread = threading.Thread(target=write_default)
        thread.start()
        thread.join(timeout=5)
        if thread.is_alive():
            self.results.append("hung")
        return EnrichResult(self.name, n_edges_emitted=0)


class _MergeBackDerivedEdgeEnricher:
    name = "mergeback_link"
    reads_subtypes = ("person",)
    reads_edge_types = ()
    emits_edge_types = ("relates_to",)
    requires_narrowings = (("person", "relates_to", "person"),)

    def enrich(self, conn, *, generation_id: int, incremental=None) -> EnrichResult:
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="n:merged",
                        edge_type="relates_to",
                        dst="n:base",
                        attrs={"kind": "merge_back_test"},
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_merge_back_restore_preserves_enricher_health_columns(
    clean_runtime: str,
) -> None:
    """The gate-cycle snapshot/restore round-trips liveness and failure
    state. A
    productive enricher gated during a fork merge-back must come back at
    its pre-gate lifetime, not the column DEFAULT 0 — otherwise every
    merge-back resets the liveness signal and falsely flags the enricher
    wired-but-silent."""
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.publisher import Publisher

    class _Stub:
        name = "lively_ex"

    publisher = Publisher("postgres://unused", "test")
    with psycopg.connect(clean_runtime) as conn:
        # Seed a productive enricher: lifetime=9, dormant_ok True.
        publisher._advance_enricher_progress(
            conn, "lively_ex",
            success=True, failure_repr=None,
            generation_id=1, catalog_version_id=1,
            current_fact_watermark=10, current_edge_watermark=5,
            emitted_count=9, dormant_ok=True,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.enricher_progress
                   SET consecutive_failure_count = 3
                 WHERE deployment_name = 'test' AND enricher_name = 'lively_ex'
                """
            )
        conn.commit()

        snapshot = merge_back_module._snapshot_gate_side_effects(
            conn,
            deployment_name="test",
            gate_branch="experiment:w1",
            enrichers=[_Stub()],
        )
        # The snapshot includes the liveness fields and active failure count.
        assert snapshot.enricher_progress_rows
        assert len(snapshot.enricher_progress_rows[0]) == 11

        # Simulate the gate publish cycle clobbering the row to defaults.
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.enricher_progress
                   SET lifetime_edges_emitted = 0,
                       dormant_ok = FALSE,
                       consecutive_failure_count = 0
                 WHERE deployment_name = 'test' AND enricher_name = 'lively_ex'
                """
            )
        conn.commit()

        merge_back_module._restore_gate_side_effects(conn, snapshot)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT lifetime_edges_emitted, dormant_ok,
                       consecutive_failure_count
                  FROM okg.enricher_progress
                 WHERE deployment_name = 'test' AND enricher_name = 'lively_ex'
                """
            )
            row = cur.fetchone()
    assert row == (9, True, 3)


def test_merge_back_clean_fork_lands_on_default(clean_runtime: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        # a fork with a valid node
        store.create_branch(
            "experiment:w1", from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [NodeFact("n:merged", "person", {"promote_intent": "durable"},
                      {"id": "n:merged"}, {"v": 1})],
            branch="experiment:w1", source="fork.src", strict_endpoints=True,
        )

        result = merge_back(
            store, "experiment:w1", dsn=clean_runtime, base_generation_id=base_gen,
            decider=DECIDER,
        )
        assert result.status == "merged", result
        # the fork's node landed on default (alongside the inherited base).
        assert {"n:base", "n:merged"} <= _default_node_ids(store)
        # the staging branch + its gate-publish checkpoint are cleaned up (no unbounded leak).
        assert not _branch_exists(store, result.staging_branch)

        # the next default publish does NOT phantom-retract the merged row.
        store.apply_facts(
            [NodeFact("n:after", "person", {}, {"id": "n:after"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        g2 = _default_publisher(clean_runtime).cycle()
        assert g2["status"] == "published", g2
        assert g2["nodes_retracted"] == 0, g2
        assert {"n:base", "n:merged", "n:after"} <= _default_node_ids(store)


def test_merge_back_holds_default_write_lock_through_land_publish(
    clean_runtime: str,
    monkeypatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.consistency import TransformConsistencyGate
    from tests.pytest.catalog_fixture_builders import seed_catalog

    probe = _ConcurrentDefaultWriterProbe(clean_runtime)
    _patch_merge_back_publisher_stack(
        monkeypatch,
        enrichers=[probe],
        gates=[TransformConsistencyGate()],
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        store.create_branch(
            "experiment:locked",
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:merged",
                    "person",
                    {"promote_intent": "durable"},
                    {"id": "n:merged"},
                    {"v": 1},
                )
            ],
            branch="experiment:locked",
            source="fork.src",
            strict_endpoints=True,
        )

        result = merge_back(
            store,
            "experiment:locked",
            dsn=clean_runtime,
            base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "merged", result
        assert probe.results, "LAND did not run the concurrent writer probe"
        assert all(item.startswith("blocked:") for item in probe.results), probe.results
        assert "n:concurrent" not in _default_node_ids(store)


def test_merge_back_rolls_default_back_when_land_publish_blocks(
    clean_runtime: str,
    monkeypatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    gate = _BranchSensitiveGate(block_default=True)
    _patch_merge_back_publisher_stack(monkeypatch, gates=[gate])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        before = _default_node_ids(store)
        before_head = store._branch_head_segment("default")
        before_width = _default_live_width(store)

        store.create_branch(
            "experiment:rollback",
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:rolled-back",
                    "person",
                    {"promote_intent": "durable"},
                    {"id": "n:rolled-back"},
                    {"v": 1},
                )
            ],
            branch="experiment:rollback",
            source="fork.src",
            strict_endpoints=True,
        )

        result = merge_back(
            store,
            "experiment:rollback",
            dsn=clean_runtime,
            base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "blocked", result
        assert str(result.blocked_reason).startswith(
            "land_gate_blocked_rolled_back:"
        )
        assert "default" in gate.calls
        assert _default_node_ids(store) == before
        assert "n:rolled-back" not in _default_node_ids(store)
        assert store.latest_generation(
            deployment_name="default",
            branch="default",
            status="published",
        ).checkpoint_id == base_gen
        # Issue #2862: the rollback writes the reverse edit onto default's own
        # head instead of re-rooting it under the pre-land checkpoint. The head
        # moves only by the staging fork and the blocked LAND checkpoint, so it
        # stays a descendant of the pre-merge head and keeps its width.
        head = store._branch_head_segment("default")
        assert before_head in store.context._backend._segment_ancestry_ids(head)
        assert _default_live_width(store) >= before_width // 2


def test_restore_branch_to_generation_reverts_edits_in_place(
    clean_runtime: str,
) -> None:
    # Issue #2862: restore writes the reverse edit on the branch's own head.
    # Changed attributes, new and deleted nodes, and new, changed and deleted
    # edges must all read exactly as the generation does, and the restore must
    # not move the head segment or narrow its interval.
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["person"],
            narrowings=[("person", "relates_to", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("n:a", "person", {"value": "before"}, {"id": "n:a"}, {"v": 1}),
                NodeFact("n:b", "person", {}, {"id": "n:b"}, {"v": 1}),
                NodeFact("n:gone", "person", {}, {"id": "n:gone"}, {"v": 1}),
                EdgeFact(
                    "n:a", "n:b", "relates_to",
                    attrs={"value": "before"},
                    source_record_id={"id": "e:a-b"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "n:b", "n:a", "relates_to",
                    attrs={},
                    source_record_id={"id": "e:b-a"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        def snapshot(view) -> tuple[list[dict], list[dict]]:
            return (
                view.query("SELECT * FROM okg.graph_nodes ORDER BY node_id"),
                view.query("SELECT * FROM okg.graph_edges ORDER BY src, dst, edge_type"),
            )

        published = snapshot(store.checkout_generation(gen))

        # Edits after the generation: change an attribute, add a node and an
        # edge, change an edge, delete a node and an edge.
        store.apply_facts(
            [
                NodeFact("n:a", "person", {"value": "after"}, {"id": "n:a"}, {"v": 2}),
                NodeFact("n:new", "person", {}, {"id": "n:new"}, {"v": 1}),
                EdgeFact(
                    "n:a", "n:b", "relates_to",
                    attrs={"value": "after"},
                    source_record_id={"id": "e:a-b"},
                    source_revision={"v": 2},
                ),
                EdgeFact(
                    "n:new", "n:a", "relates_to",
                    attrs={},
                    source_record_id={"id": "e:new-a"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        head = store.checkout("default")
        doomed_edge = head.query(
            "SELECT edge_id FROM okg.graph_edges WHERE src = 'n:b' AND dst = 'n:a'"
        )
        head.delete_keys("okg.graph_edges", [dict(doomed_edge[0])])
        head.delete_keys("okg.graph_nodes", [{"node_id": "n:gone"}])
        edited = snapshot(store.checkout("default"))
        assert edited != published

        head_before = store._branch_head_segment("default")
        width_before = _default_live_width(store)
        store.restore_branch_to_generation(gen, branch="default", deployment_name="default")

        assert snapshot(store.checkout("default")) == published
        assert store._branch_head_segment("default") == head_before
        assert _default_live_width(store) == width_before


def _declarer_rows(count: int, *, prefix: str) -> list[dict]:
    return [
        {
            "node_id": f"{prefix}:{index}",
            "source": "bulk.src",
            "subtype": "person",
            "attrs": {},
            "text_hash": None,
            "source_record_id": {"id": f"{prefix}:{index}"},
            "source_revision": {"v": 1},
            "run_id": None,
            "observed_at": None,
        }
        for index in range(count)
    ]


def test_restore_reverts_composite_key_rows_above_the_parameter_limit(
    clean_runtime: str,
) -> None:
    # okg.graph_node_declarers has a two-column key, so one lookup of 33,000
    # keys binds 66,000 parameters, over PostgreSQL's 65,535 limit. The restore
    # must chunk its reads (review finding on #2868) or the rollback fails and
    # the rows stay on default.
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.graph_store_internal.node_declarers import NODE_DECLARERS_TABLE

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store._require_node_declarers_table()
        # Batched writes stage rows in an ON COMMIT DROP temp table, so they
        # run inside one session transaction, as product writers do.
        session = store.checkout("default")
        with session.transaction():
            session.upsert_rows(NODE_DECLARERS_TABLE, _declarer_rows(2, prefix="n:kept"))
        gen = store.checkpoint_generation("composite-limit", deployment_name="default")

        session = store.checkout("default")
        with session.transaction():
            session.upsert_rows(
                NODE_DECLARERS_TABLE, _declarer_rows(33_000, prefix="n:bulk")
            )

        def count() -> int:
            return store.checkout("default").query(
                "SELECT count(*) AS n FROM okg.graph_node_declarers"
            )[0]["n"]

        assert count() == 33_002
        store.restore_branch_to_generation(gen, deployment_name="default")
        assert count() == 2


def test_restore_reverts_a_table_registered_after_the_store_was_built(
    clean_runtime: str,
) -> None:
    # A maintained derivation can register its table during the landing
    # publish. The restoring store loaded its table registry earlier, so it
    # must reload it or those rows silently stay on default.
    psycopg = pytest.importorskip("psycopg")
    table = f"okg_test_late_{uuid4().hex[:8]}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        gen = store.checkpoint_generation("late-table", deployment_name="default")
        assert table not in store.context._backend.tables

        with psycopg.connect(clean_runtime, autocommit=True) as other_conn:
            other = ChronosGraphStore.from_connection(other_conn)
            other.context.db.execute(
                f"CREATE TABLE {table} (k TEXT PRIMARY KEY, v TEXT)"
            )
            other.context.register_table(table, ["k"])
            other.checkout("default").upsert_rows(table, [{"k": "a", "v": "landed"}])

        store.restore_branch_to_generation(gen, deployment_name="default")
        # Read through a fresh store: one that never loaded the table would
        # not route the query through Chronos at all.
        with psycopg.connect(clean_runtime, autocommit=True) as read_conn:
            reader = ChronosGraphStore.from_connection(read_conn)
            assert table in reader.context._backend.tables
            assert reader.checkout("default").query(f"SELECT * FROM {table}") == []


def test_restore_refuses_a_generation_off_the_head_lineage(
    clean_runtime: str,
) -> None:
    # After a rollover, default is re-rooted under main; an older generation's
    # segments are no longer on its lineage, so a reverse edit from it would
    # not be exact. The restore must refuse and say why.
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.errors import SubstrateError

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:1", "person", {}, {"id": "n:1"}, {"v": 1})],
            source="default",
        )
        first = store.checkpoint_generation("lineage-first", deployment_name="default")
        store.rollover_branch_from_generation(first, deployment_name="default")

        with pytest.raises(SubstrateError, match="not on the branch head's lineage"):
            store.restore_branch_to_generation(first, deployment_name="default")


_LAND_SIDE_TABLES = (
    "okg.graph_nodes",
    "okg.graph_edges",
    "okg.graph_node_declarers",
    "okg.ontology_rollup",
    "okg.console_count_rollup",
    "okg.claim_projection",
    "okg.claim_effective",
)


def _table_contents(store: ChronosGraphStore, view) -> dict[str, list[dict]]:
    registered = store.context._backend.tables
    contents: dict[str, list[dict]] = {}
    for table in _LAND_SIDE_TABLES:
        if table not in registered:
            continue
        rows = view.query(f"SELECT * FROM {table}")
        contents[table] = sorted(rows, key=repr)
    return contents


def test_blocked_land_restores_rollup_declarer_and_claim_tables(
    clean_runtime: str,
    monkeypatch,
) -> None:
    # The blocked LAND publish is a real publish on default. After the
    # rollback, every land-side table this database registers must read
    # exactly as the pre-land generation. In this synthetic deployment the
    # rollups are registered but the blocked landing leaves them unchanged,
    # and the claim tables are not registered; the reverse edit covers every
    # registered table regardless (see the two restore tests above).
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    gate = _BranchSensitiveGate(block_default=True)
    _patch_merge_back_publisher_stack(monkeypatch, gates=[gate])

    head_before_restore: dict[str, list[dict]] = {}
    real_restore = ChronosGraphStore.restore_branch_to_generation

    def spying_restore(self, generation_id, **kwargs):
        self.context._backend.refresh_registries()
        head_before_restore.update(
            _table_contents(self, self.checkout(kwargs.get("branch", "default")))
        )
        return real_restore(self, generation_id, **kwargs)

    monkeypatch.setattr(
        ChronosGraphStore, "restore_branch_to_generation", spying_restore
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        fork = "experiment:side-tables"
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    f"n:landed{index}",
                    "person",
                    {"promote_intent": "durable"},
                    {"id": f"n:landed{index}"},
                    {"v": 1},
                )
                for index in range(3)
            ],
            branch=fork,
            source="fork.src",
            strict_endpoints=True,
        )

        result = merge_back(
            store, fork, dsn=clean_runtime, base_generation_id=base_gen, decider=DECIDER,
        )
        assert str(result.blocked_reason).startswith(
            "land_gate_blocked_rolled_back:"
        ), result

        store.context._backend.refresh_registries()
        published = _table_contents(store, store.checkout_generation(base_gen))
        after = _table_contents(store, store.checkout("default"))
        assert set(after) >= {
            "okg.graph_nodes",
            "okg.ontology_rollup",
            "okg.console_count_rollup",
        }, sorted(after)
        # The landing really wrote to default, so equality below is a
        # rollback, not a no-op.
        changed = sorted(
            table
            for table in head_before_restore
            if head_before_restore[table] != published.get(table)
        )
        assert "okg.graph_nodes" in changed, changed
        assert after == published


def test_merge_back_rolls_back_modified_node_and_edge_live_rows(
    clean_runtime: str,
    monkeypatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    gate = _BranchSensitiveGate(block_default=True)
    _patch_merge_back_publisher_stack(monkeypatch, gates=[gate])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["person"],
            narrowings=[("person", "relates_to", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "n:base",
                    "person",
                    {"value": "before"},
                    {"id": "n:base"},
                    {"v": 1},
                ),
                NodeFact("n:dst", "person", {}, {"id": "n:dst"}, {"v": 1}),
                EdgeFact(
                    "n:base",
                    "n:dst",
                    "relates_to",
                    attrs={"value": "before"},
                    source_record_id={"id": "e:base-dst"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        before_node = _node_row(store, "default", "n:base")
        before_edge = _edge_row(
            store,
            "default",
            src="n:base",
            dst="n:dst",
            edge_type="relates_to",
        )

        fork = "experiment:rollback-modified"
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:base",
                    "person",
                    {"promote_intent": "durable", "value": "after"},
                    {"id": "n:base"},
                    {"v": 2},
                ),
                EdgeFact(
                    "n:base",
                    "n:dst",
                    "relates_to",
                    attrs={"promote_intent": "durable", "value": "after"},
                    source_record_id={"id": "e:base-dst"},
                    source_revision={"v": 2},
                ),
            ],
            branch=fork,
            source="fork.src",
            strict_endpoints=True,
        )

        result = merge_back(
            store,
            fork,
            dsn=clean_runtime,
            base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "blocked", result
        assert str(result.blocked_reason).startswith(
            "land_gate_blocked_rolled_back:"
        )
        assert _node_row(store, "default", "n:base") == before_node
        assert _edge_row(
            store,
            "default",
            src="n:base",
            dst="n:dst",
            edge_type="relates_to",
        ) == before_edge
        assert "default" in gate.calls


def test_merge_back_replays_source_only_and_derives_once_on_land(
    clean_runtime: str,
    monkeypatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.consistency import TransformConsistencyGate
    from tests.pytest.catalog_fixture_builders import seed_catalog

    _patch_merge_back_publisher_stack(
        monkeypatch,
        enrichers=[_MergeBackDerivedEdgeEnricher()],
        gates=[TransformConsistencyGate()],
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["person"],
            narrowings=[("person", "relates_to", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        store.create_branch(
            "experiment:derived",
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:merged",
                    "person",
                    {"promote_intent": "durable"},
                    {"id": "n:merged"},
                    {"v": 1},
                )
            ],
            branch="experiment:derived",
            source="fork.src",
            strict_endpoints=True,
        )

        result = merge_back(
            store,
            "experiment:derived",
            dsn=clean_runtime,
            base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "merged", result
        rows = store.checkout("default").query(
            """
            SELECT src, edge_type, dst, source, attrs
              FROM okg.graph_edges
             WHERE src = 'n:merged'
               AND edge_type = 'relates_to'
               AND dst = 'n:base'
            """
        )
        assert len(rows) == 1, rows
        # Derived live rows carry their enricher's producer id, as in
        # tests/substrate/fact_batch/test_derived_and_system_paths.py. This
        # expectation went stale while the file could not run (issue #2866).
        assert rows[0]["source"] == "_enricher:mergeback_link"
        assert rows[0]["attrs"]["kind"] == "merge_back_test"
        assert LINEAGE_ATTR in rows[0]["attrs"]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.edge_facts
                 WHERE source = '_enricher:mergeback_link'
                """
            )
            assert cur.fetchone()[0] == 1


def test_gate_side_effect_cleanup_keeps_foreign_derived_facts(
    clean_runtime: str,
) -> None:
    psycopg = pytest.importorskip("psycopg")

    gate_branch = "staging:cleanup:gate"
    gate_run_id = str(uuid4())
    foreign_run_id = str(uuid4())

    with psycopg.connect(clean_runtime) as conn:
        snapshot = merge_back_module._snapshot_gate_side_effects(
            conn,
            deployment_name="default",
            gate_branch=gate_branch,
            enrichers=[_MergeBackDerivedEdgeEnricher()],
        )
        with conn.cursor() as cur:
            gate_edge_fact = _insert_derived_edge_fact(
                cur,
                run_id=gate_run_id,
                dedupe_key="cleanup:gate:edge",
            )
            _insert_derived_node_fact(
                cur,
                run_id=gate_run_id,
                dedupe_key="cleanup:gate:node",
            )
            foreign_edge_fact = _insert_derived_edge_fact(
                cur,
                run_id=foreign_run_id,
                dedupe_key="cleanup:foreign:edge",
            )
            foreign_node_fact = _insert_derived_node_fact(
                cur,
                run_id=foreign_run_id,
                dedupe_key="cleanup:foreign:node",
            )
            _record_gate_edge_range(
                cur,
                gate_branch=gate_branch,
                run_id=gate_run_id,
                fact_id=gate_edge_fact,
            )

        merge_back_module._restore_gate_side_effects(conn, snapshot)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id::text, count(*)
                  FROM okg.edge_facts
                 WHERE fact_id = ANY(%s)
                 GROUP BY run_id
                """,
                ([gate_edge_fact, foreign_edge_fact],),
            )
            edge_counts = {row[0]: int(row[1]) for row in cur.fetchall()}
            cur.execute(
                """
                SELECT run_id::text, count(*)
                  FROM okg.node_facts
                 WHERE fact_id = ANY(%s)
                 GROUP BY run_id
                """,
                ([foreign_node_fact],),
            )
            node_counts = {row[0]: int(row[1]) for row in cur.fetchall()}
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE run_id = %s::uuid",
                (gate_run_id,),
            )
            gate_node_count = int(cur.fetchone()[0])

        assert gate_run_id not in edge_counts
        assert gate_node_count == 0
        assert edge_counts == {foreign_run_id: 1}
        assert node_counts == {foreign_run_id: 1}


def test_merge_back_composes_promote_scope_restamp_partition(clean_runtime: str) -> None:
    # compose-stage-guards: STAGE stages only the promote-scoped, re-stamped, subtype-legal,
    # non-conflicting subset; a scratch (unmarked) row is dropped, an illegal-subtype row is
    # quarantined, and the landed row carries merge-producer provenance on EVERY field.
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])  # "ghost" is NOT in the catalog
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        fork = "experiment:c1"
        store.create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                # (a) durable + in-catalog -> lands
                NodeFact("n:dur", "person", {"promote_intent": "durable", "k": 1},
                         {"id": "n:dur"}, {"v": 1}),
                # (b) scratch (unmarked) -> dropped
                NodeFact("n:scr", "person", {}, {"id": "n:scr"}, {"v": 1}),
                # (c) durable + illegal subtype -> quarantined
                NodeFact("n:ghost", "ghost", {"promote_intent": "durable"},
                         {"id": "n:ghost"}, {"v": 1}),
            ],
            branch=fork, source="fork.src", strict_endpoints=True,
        )

        result = merge_back(
            store, fork, dsn=clean_runtime, base_generation_id=base_gen, decider=DECIDER,
        )
        assert result.status == "merged", result

        # only the durable, in-catalog node landed (delta == 1 over the inherited base).
        default_ids = _default_node_ids(store)
        assert default_ids == {"n:base", "n:dur"}, default_ids

        # the landed row's provenance is fully re-stamped to the merge producer (not the
        # fork's), on every field, and the server-only promote marker is stripped.
        rows = store.checkout("default").query(
            "SELECT source, source_record_id, source_revision, attrs "
            "FROM okg.graph_nodes WHERE node_id = 'n:dur'"
        )
        assert len(rows) == 1, rows
        row = rows[0]
        assert row["source"] == f"merge:{fork}@{base_gen}", row["source"]
        assert row["source_record_id"] == {"merged_from": fork, "fork_tip_gen": str(base_gen)}
        attrs = row["attrs"] or {}
        assert "promote_intent" not in attrs, attrs  # server-only marker stripped
        assert attrs.get("k") == 1  # the rest of attrs is preserved

        # the illegal-subtype row is set aside on quarantine:<fork> (present there, not landed).
        q_ids = {
            r["node_id"]
            for r in store.checkout(f"quarantine:{fork}").query(
                "SELECT node_id FROM okg.graph_nodes"
            )
        }
        assert "n:ghost" in q_ids, q_ids
        assert "n:dur" not in q_ids  # accepted rows are NOT quarantined


def test_merge_back_quarantines_conflicting_durable_rows(clean_runtime: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "n:conflict",
                    "person",
                    {"value": "base"},
                    {"id": "n:conflict"},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        fork = "experiment:conflict"
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:conflict",
                    "person",
                    {"promote_intent": "durable", "value": "fork"},
                    {"id": "n:conflict"},
                    {"v": 2},
                )
            ],
            branch=fork,
            source="fork.src",
            strict_endpoints=True,
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:conflict",
                    "person",
                    {"value": "target"},
                    {"id": "n:conflict"},
                    {"v": 3},
                )
            ],
            source="default",
            strict_endpoints=True,
        )

        result = merge_back(
            store,
            fork,
            dsn=clean_runtime,
            base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "merged", result
        default_row = _node_row(store, "default", "n:conflict")
        assert default_row is not None
        assert default_row["attrs"]["value"] == "target"
        quarantine_row = _node_row(store, f"quarantine:{fork}", "n:conflict")
        assert quarantine_row is not None
        assert quarantine_row["attrs"]["value"] == "fork"
        assert "promote_intent" not in quarantine_row["attrs"]
        assert quarantine_row["source"] == f"merge:{fork}@{base_gen}"


def test_merge_back_stage_quarantine_fault_rolls_back_both_writes(
    clean_runtime: str,
    monkeypatch,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    def fail_quarantine_write(*_args, **_kwargs) -> None:
        raise RuntimeError("injected quarantine write fault")

    monkeypatch.setattr(
        merge_back_module,
        "_apply_upserts_to_branch",
        fail_quarantine_write,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        fork = "experiment:atomic-fault"
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:accepted",
                    "person",
                    {"promote_intent": "durable"},
                    {"id": "n:accepted"},
                    {"v": 1},
                ),
                NodeFact(
                    "n:conflict",
                    "person",
                    {"promote_intent": "durable", "value": "fork"},
                    {"id": "n:conflict"},
                    {"v": 1},
                ),
            ],
            branch=fork,
            source="fork.src",
            strict_endpoints=True,
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:conflict",
                    "person",
                    {"value": "target"},
                    {"id": "n:conflict"},
                    {"v": 2},
                )
            ],
            source="default",
            strict_endpoints=True,
        )

        with pytest.raises(RuntimeError, match="injected quarantine write fault"):
            merge_back(
                store,
                fork,
                dsn=clean_runtime,
                base_generation_id=base_gen,
                decider=DECIDER,
            )

        assert "n:accepted" not in _default_node_ids(store)
        assert not _branch_exists(store, f"quarantine:{fork}")


def test_merge_back_blocks_whole_merge_on_unmergeable_source(clean_runtime: str) -> None:
    # A durable fork row whose source is a reserved system prefix would launder a trusted/
    # derived label onto default -> restamp HARD-REFUSES the whole merge (batch deny-by-default),
    # default unchanged, nothing staged or quarantined.
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        before = _default_node_ids(store)

        fork = "experiment:sys"
        store.create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        # a durable node authored with a reserved system source (reads back trusted).
        store.apply_facts(
            [NodeFact("n:sys", "person", {"promote_intent": "durable"},
                      {
                          "id": "n:sys",
                          "derived_from": ["n:base"],
                          "derived_support": {
                              "schema": "okg.derived_support.v1",
                              "producer": "_enricher:dedup",
                              "members": ["n:base"],
                              "policy": "all_members",
                          },
                      }, {"v": 1})],
            branch=fork, source="_enricher:dedup", strict_endpoints=True,
        )

        result = merge_back(
            store, fork, dsn=clean_runtime, base_generation_id=base_gen, decider=DECIDER,
        )
        assert result.status == "blocked", result
        assert result.blocked_reason == "reserved_system_source", result
        assert _default_node_ids(store) == before  # default unchanged
        assert not _branch_exists(store, f"quarantine:{fork}")  # nothing quarantined


def test_merge_back_gate_violation_blocks_default_unchanged(clean_runtime: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # narrowing allows ONLY ticket--mentions-->person.
        seed_catalog(
            conn, subtypes=["person", "ticket"],
            narrowings=[("ticket", "mentions", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("n:p", "person", {}, {"id": "n:p"}, {"v": 1}),
                NodeFact("n:t", "ticket", {}, {"id": "n:t"}, {"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        before = _default_node_ids(store)

        # a fork adds a narrowing-VIOLATING edge person--mentions-->ticket (reverse).
        store.create_branch(
            "experiment:bad", from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [EdgeFact("n:p", "n:t", "mentions", attrs={"promote_intent": "durable"},
                      source_record_id={"id": "e"}, source_revision={"v": 1})],
            branch="experiment:bad", source="fork.src", strict_endpoints=True,
        )

        result = merge_back(
            store, "experiment:bad", dsn=clean_runtime, base_generation_id=base_gen,
            decider=DECIDER,
        )
        assert result.status == "blocked", result
        # the block has a recorded reason (the gate blocked, not a silent failure) and
        # staging was abandoned.
        assert result.blocked_reason, result
        assert not _branch_exists(store, result.staging_branch)
        # default is unchanged: no new generation advanced it, no illegal edge landed.
        assert _default_node_ids(store) == before
        edges = store.checkout("default").query(
            "SELECT src, dst FROM okg.graph_edges WHERE src = 'n:p' AND dst = 'n:t'"
        )
        assert edges == [], edges


def test_repeated_merge_backs_do_not_exhaust_interval(clean_runtime: str) -> None:
    # The point of the REPLAY landing (A): the landing is a normal write into default's own
    # head, which does not shrink default's interval -- so N sequential merge-backs all
    # succeed, where a fast-forward landing walls after ~5 cycles (see the chronos budget
    # test). This is the operational unblock for the 50+-worktree, frequent-merge-back case.
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]

        n_cycles = 8  # well past the ~5-cycle fast-forward exhaustion wall
        for i in range(n_cycles):
            fork = f"experiment:w{i}"
            store.create_branch(
                fork, from_branch="default",
                metadata={"base_branch": "default", "base_generation_id": base_gen},
            )
            store.apply_facts(
                [NodeFact(f"n:m{i}", "person", {"promote_intent": "durable"},
                          {"id": f"n:m{i}"}, {"v": 1})],
                branch=fork, source="fork.src", strict_endpoints=True,
            )
            result = merge_back(
                store, fork, dsn=clean_runtime, base_generation_id=base_gen, decider=DECIDER,
            )
            assert result.status == "merged", (i, result)
            base_gen = result.generation_id  # next fork bases on the landed generation

        # every merged-back node is live on default.
        assert {f"n:m{i}" for i in range(n_cycles)} <= _default_node_ids(store)


def _default_live_width(store: ChronosGraphStore) -> int:
    row = store.context.db.execute(
        """
        SELECT s.live_lo, s.live_hi
          FROM _chronos_branch_interval_branches b
          JOIN _chronos_branch_interval_segments s
            ON s.segment_id = b.current_segment_id
         WHERE b.branch_id = 'default'
        """
    ).fetchone()
    return int(row["live_hi"]) - int(row["live_lo"])


def test_blocked_merge_back_rollback_keeps_default_interval(
    clean_runtime: str,
    monkeypatch,
) -> None:
    # Issue #2862: a LAND-blocked merge-back used to roll default back by
    # re-rooting default's head under the pre-land checkpoint's segment. That
    # segment is only sqrt(W) wide, so default dropped from ~3e15 to ~3e7 and a
    # handful of later publishes or forks exhausted it. The rollback must leave
    # default able to keep publishing and forking, the way a successful replay
    # land does.
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    gate = _BranchSensitiveGate(block_default=True)
    _patch_merge_back_publisher_stack(monkeypatch, gates=[gate])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        widths = [_default_live_width(store)]

        n_blocked = 3
        for i in range(n_blocked):
            fork = f"experiment:blocked{i}"
            store.create_branch(
                fork,
                from_branch="default",
                metadata={"base_branch": "default", "base_generation_id": base_gen},
            )
            store.apply_facts(
                [
                    NodeFact(
                        f"n:blocked{i}",
                        "person",
                        {"promote_intent": "durable"},
                        {"id": f"n:blocked{i}"},
                        {"v": 1},
                    )
                ],
                branch=fork,
                source="fork.src",
                strict_endpoints=True,
            )
            result = merge_back(
                store,
                fork,
                dsn=clean_runtime,
                base_generation_id=base_gen,
                decider=DECIDER,
            )
            assert str(result.blocked_reason).startswith(
                "land_gate_blocked_rolled_back:"
            ), (i, result)
            widths.append(_default_live_width(store))

        # Later ordinary work on default: publishes and a fork. Stop blocking
        # default so these publishes can succeed on their own merits.
        gate.block_default = False
        outcomes: list[str] = []
        for j in range(8):
            store.apply_facts(
                [NodeFact(f"n:after{j}", "person", {}, {"id": f"n:after{j}"}, {"v": 1})],
                source="default",
                strict_endpoints=True,
            )
            try:
                out = _default_publisher(clean_runtime).cycle()
                outcomes.append(str(out.get("status")))
            except Exception as exc:  # noqa: BLE001 - record the failure for the assert
                outcomes.append(f"error:{exc}")
                break
            widths.append(_default_live_width(store))
        try:
            store.create_branch("experiment:after", from_branch="default")
            outcomes.append("fork:ok")
        except Exception as exc:  # noqa: BLE001 - record the failure for the assert
            outcomes.append(f"fork_error:{exc}")

        assert widths[n_blocked] >= widths[0] // 2, (
            "blocked merge-back rollback shrank default's interval",
            widths,
            outcomes,
        )
        assert outcomes == ["published"] * 8 + ["fork:ok"], (widths, outcomes)


def test_merge_back_without_decider_is_blocked(clean_runtime: str) -> None:
    # The LAND into default requires a named-human decider (merge_guard decider-boundary).
    # Without one, the merge-back blocks and default is unchanged (the guard refuses at the
    # mutation boundary, before applying).
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        before = _default_node_ids(store)
        store.create_branch(
            "experiment:nd", from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [NodeFact("n:nd", "person", {"promote_intent": "durable"},
                      {"id": "n:nd"}, {"v": 1})],
            branch="experiment:nd", source="fork.src", strict_endpoints=True,
        )

        result = merge_back(  # no decider
            store, "experiment:nd", dsn=clean_runtime, base_generation_id=base_gen,
        )
        assert result.status == "blocked", result
        assert result.blocked_reason == "missing_decider", result
        assert _default_node_ids(store) == before  # default unchanged
        assert not _branch_exists(store, result.staging_branch)  # staging abandoned


def test_blocked_land_rollback_keeps_later_ranked_declarations_with_live_nodes(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Item 92a, through merge_back's own call sites. After the last publish,
    a source rewrites a ranked edge's declaration only (s1 -> s2, no stored
    row changes, so no checkpoint has it) and declares an edge onto a node it
    added after the publish. The LAND publish blocks and default is rolled
    back to the pre-land generation: the s2 declaration must come back (its
    source has moved past it), while the one onto the node the rollback
    removed must not, or it would store a dangling edge."""
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_edge_declarers_db import _rank

    enricher, structure = "_enricher:code_reference", "code-structure"
    gate = _BranchSensitiveGate(block_default=True)
    _patch_merge_back_publisher_stack(monkeypatch, gates=[gate])

    def node(node_id: str) -> NodeFact:
        return NodeFact(node_id, "person", {}, {"id": node_id}, {"v": 1})

    def calls(dst: str, via: str) -> EdgeFact:
        return EdgeFact(
            "n:a", dst, "calls", {"via": via}, source_record_id={"via": via},
        )

    def declarations(store: ChronosGraphStore) -> list[tuple[str, str, str]]:
        return sorted(
            (row["dst"], row["source"], dict(row["attrs"] or {}).get("via"))
            for row in store.checkout("default").query(
                "SELECT dst, source, attrs FROM okg.graph_edge_declarers"
            )
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["person"],
            narrowings=[("person", "calls", "person")],
        )
    _rank(clean_runtime, {"calls": [enricher, structure]})
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [node("n:a"), node("n:b"), calls("n:b", "s1")],
            source=structure, strict_endpoints=True,
        )
        store.apply_facts([calls("n:b", "e1")], source=enricher, strict_endpoints=True)
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        store.apply_facts([calls("n:b", "s2")], source=structure, strict_endpoints=True)
        store.apply_facts(
            [node("n:late"), calls("n:late", "s1")],
            source=structure, strict_endpoints=True,
        )
        store.create_branch(
            "experiment:rank-rollback",
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [NodeFact("n:fork", "person", {"promote_intent": "durable"},
                      {"id": "n:fork"}, {"v": 1})],
            branch="experiment:rank-rollback", source="fork.src",
            strict_endpoints=True,
        )

        result = merge_back(
            store,
            "experiment:rank-rollback",
            dsn=clean_runtime,
            base_generation_id=base_gen,
            decider=DECIDER,
        )

        assert result.status == "blocked", result
        assert result.blocked_reason.startswith("land_gate_blocked_rolled_back:")
        assert not result.blocked_reason.endswith("declarations_not_reapplied")
        assert declarations(store) == [
            ("n:b", enricher, "e1"),
            ("n:b", structure, "s2"),
        ]
        assert "n:late" not in _default_node_ids(store)
        assert [
            (row["dst"], row["source"])
            for row in store.checkout("default").query(
                "SELECT dst, source FROM okg.graph_edges WHERE edge_type = 'calls'"
            )
        ] == [("n:b", enricher)]
