"""M1.2 tests: Publisher — advisory lock + Phase A/B/C skeleton."""
from __future__ import annotations

import json
import threading
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.catalog import ownership
from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.ownership import (
    catalog_identity_for,
    claim_catalog_ownership,
    diagnose_catalog_ownership,
    ensure_catalog_write_allowed,
)
from okg.substrate.cli.main import main
from okg.substrate.errors import (
    CatalogOwnershipMismatch,
    PublisherLockHeld,
    SubstrateError,
)
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers._fact_emitters import emit_node_fact
from okg.substrate.consistency.transform_gate import TransformConsistencyGate
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.runtime.source_sync_priority import (
    SOURCE_SYNC_ADMISSION_INTENT_ID,
    _local_source_sync_admission_lock,
    source_sync_admission_intent_lock,
)
from okg.views.assembly import assemble as default_assemble
from okg.views.planners.base import QuoteSpec, ViewDefinition
from okg.views.types import Anchor
from tests.helpers.governed_view_materializer import ViewMaterializer
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.test_catalog_ownership import (
    _catalog_version_count,
    _composed,
    _deployment_dir,
    _identity,
    _reset_catalog,
)


pytestmark = pytest.mark.postgres


def _seed_concept_catalog(cur) -> None:
    seed_catalog(
        cur.connection,
        subtypes=["concept"],
        narrowings=[("concept", "contains", "concept")],
    )


def _insert_concept_fact(cur, node_id: str) -> None:
    run_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO okg.node_facts
          (node_id, subtype, attrs, source, source_record_id,
           source_revision, observed_at, dedupe_key, run_id, op)
        VALUES (%s, 'concept', %s::jsonb, 'fixture',
                %s::jsonb, %s::jsonb, now(), %s, %s, 'I')
        """,
        (
            node_id,
            json.dumps({"text": node_id}),
            json.dumps({"node_id": node_id}),
            json.dumps({"run_id": run_id}),
            f"fixture:{node_id}:{run_id}",
            run_id,
        ),
    )


def _insert_contains_edge_fact(cur) -> None:
    run_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO okg.edge_facts
          (edge_id, src, dst, edge_type, attrs, provenance,
           confidence, source, source_record_id, source_revision,
           observed_at, dedupe_key, run_id, op)
        VALUES (9001, 'n:first', 'n:second', 'contains',
                '{}'::jsonb, 'explicit', 1.0, 'fixture',
                '{}'::jsonb, '{}'::jsonb, now(), %s, %s, 'I')
        """,
        (f"fixture:contains:{run_id}", run_id),
    )


_ISSUE_713_SOURCE_ID = "issue713:source"
_ISSUE_713_MID_ID = "issue713:mid"
_ISSUE_713_FINAL_ID = "issue713:final"


def _seed_issue_713_catalog(conn: psycopg.Connection) -> None:
    seed_catalog(
        conn,
        subtypes=[
            "issue713_source",
            "issue713_mid",
            "issue713_final",
            "issue713_unread",
        ],
        narrowings=[],
    )


def _insert_issue_713_node_fact(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
    op: str = "I",
) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, '{}'::jsonb, 'issue713_fixture',
                    %s::jsonb, %s::jsonb, now(), %s, %s, %s)
            """,
            (
                node_id,
                subtype,
                json.dumps({"node_id": node_id}),
                json.dumps({"run_id": run_id}),
                f"issue713:{node_id}:{run_id}:{op}",
                run_id,
                op,
            ),
        )


class _Issue713NodeEmitter:
    publisher_skip_when_clean = True
    reads_edge_types = ()
    emits_edge_types = ()
    requires_narrowings = ()
    declares_node_provenance = True
    nondeterministic = False

    def __init__(
        self,
        *,
        name: str,
        reads_subtype: str,
        input_node_id: str,
        output_subtype: str,
        output_node_id: str,
        always_emit: bool = False,
    ) -> None:
        self.name = name
        self.reads_subtypes = (reads_subtype,)
        self.input_node_id = input_node_id
        self.emits_typed_subtype = output_subtype
        self.output_node_id = output_node_id
        self.always_emit = always_emit
        self.calls = 0
        self.contexts = []

    def enrich(self, conn, *, generation_id, incremental=None):
        self.calls += 1
        self.contexts.append(incremental)
        should_emit = self.always_emit or (
            incremental is not None
            and self.input_node_id in incremental.dirty_node_ids
        )
        if not should_emit:
            return EnrichResult(self.name, n_edges_emitted=0)
        outcome = emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id=self.output_node_id,
            subtype=self.emits_typed_subtype,
            attrs={"derived": self.output_node_id},
            dedupe_key=(
                f"_enricher:{self.name}:{self.output_node_id}:"
                f"{generation_id}:I"
            ),
            source_record_id={"node_id": self.output_node_id},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid.uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=[self.input_node_id],
            support_policy="all_members",
        )
        return EnrichResult(
            self.name,
            n_edges_emitted=0,
            rejected_fact_count=int(not outcome.emitted),
            incomplete=not outcome.emitted,
        )


class _Issue713CountingEnricher:
    publisher_skip_when_clean = True
    reads_edge_types = ()
    emits_edge_types = ()
    emits_typed_subtype = "issue713_final"
    requires_narrowings = ()
    declares_node_provenance = True
    nondeterministic = False

    def __init__(
        self,
        name: str,
        reads_subtypes: tuple[str, ...],
    ) -> None:
        self.name = name
        self.reads_subtypes = reads_subtypes
        self.calls = 0
        self.contexts = []

    def enrich(self, conn, *, generation_id, incremental=None):
        del conn, generation_id
        self.calls += 1
        self.contexts.append(incremental)
        return EnrichResult(self.name, n_edges_emitted=0)


def _issue_713_generation_node_count(
    dsn: str,
    generation_id: str,
    node_id: str,
) -> int:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout_generation(generation_id).query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": node_id},
        )
        return int(rows[0]["n"])
    finally:
        store.close()


def test_cycle_publishes_on_empty_graph(clean_runtime: str) -> None:
    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    out = pub.cycle()

    assert out["status"] in {"published", "no_op"}
    assert out["phase_a_converged"] is True
    assert out["phase_b_blocked"] is False
    assert out["correlation_id"] == out["generation_id"]
    assert out["generation_id"].startswith("okg:test-pub:branch:default:gen:")


def test_cycle_surfaces_noop_on_empty_publish(clean_runtime: str) -> None:
    """A publish cycle with no pending facts and no source runs
    SHALL be visible as `noop: True` with zero-count deltas in the
    cycle output. Closes the silent-success bug where
    `phase_a_converged: True` was indistinguishable from a real
    publish vs. an empty no-op generation.
    """
    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    out = pub.cycle()

    assert out["nodes_added"] == 0
    assert out["nodes_retracted"] == 0
    assert out["edges_added"] == 0
    assert out["edges_retracted"] == 0
    assert out["per_source_status"] == {}
    assert out["noop"] is True


def test_cycle_output_carries_count_and_status_fields(
    clean_runtime: str,
) -> None:
    """Every cycle output SHALL carry the projection-delta fields
    so callers can distinguish no-op publishes from real ones.
    """
    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    out = pub.cycle()

    for key in (
        "nodes_added",
        "nodes_retracted",
        "edges_added",
        "edges_retracted",
        "per_source_status",
        "noop",
    ):
        assert key in out, f"cycle output missing {key!r}"
    assert isinstance(out["nodes_added"], int)
    assert isinstance(out["per_source_status"], dict)
    assert isinstance(out["noop"], bool)


def test_cycle_nomos_generation_publish_denial_blocks_generation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _seed_concept_catalog(cur)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact(
                node_id="n:nomos-publish",
                subtype="concept",
                attrs={"text": "publish guard"},
                source_record_id={"id": "n:nomos-publish"},
                source_revision={"v": 1},
            )
        ])
    _activate_generation_publish_policy(clean_runtime, rules=[])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    assert out["blocked_reason"]["reason"] == "nomos_generation_publish_denied"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        metadata = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).get_generation(out["generation_id"]).metadata
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT action, effect, target->>'kind'
                  FROM okg.nomos_decisions
                 WHERE deployment_name = 'default'
                   AND action = 'generation.publish'
                 ORDER BY created_at, decision_row_id
                """
            )
            decisions = cur.fetchall()

    assert dict(metadata or {})["blocked_reason"]["reason"] == (
        "nomos_generation_publish_denied"
    )
    assert decisions == [("generation.publish", "deny", "generation_publish")]


def test_chronos_counts_after_delta_uses_previous_metadata() -> None:
    class CountingBranch:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def query(self, sql: str) -> list[dict[str, int]]:
            self.queries.append(sql)
            raise AssertionError("full graph count should not be queried")

    counts = Publisher._chronos_counts_after_delta(
        (10, 20),
        {
            "nodes_added": 3,
            "nodes_retracted": 1,
            "edges_added": 5,
            "edges_retracted": 2,
        },
        branch=CountingBranch(),
    )

    assert counts == (12, 23)


def test_chronos_counts_after_delta_falls_back_without_previous_metadata() -> None:
    class CountingBranch:
        def query(self, sql: str) -> list[dict[str, int]]:
            if "graph_nodes" in sql:
                return [{"n": 12}]
            if "graph_edges" in sql:
                return [{"n": 23}]
            raise AssertionError(sql)

    counts = Publisher._chronos_counts_after_delta(
        None,
        {
            "nodes_added": 3,
            "nodes_retracted": 1,
            "edges_added": 5,
            "edges_retracted": 2,
        },
        branch=CountingBranch(),
    )

    assert counts == (12, 23)


def test_chronos_segment_delta_query_is_continuation_confined() -> None:
    class CapturingCursor:
        def __init__(self) -> None:
            self.query = ""

        def __enter__(self):
            return self

        def __exit__(self, *exc_info) -> None:
            return None

        def execute(self, query, _params) -> None:
            self.query = query.as_string(None)

        def fetchone(self) -> tuple[int, int, int]:
            return (0, 0, 0)

    class CapturingConnection:
        def __init__(self) -> None:
            self.cursor_obj = CapturingCursor()

        def cursor(self):
            return self.cursor_obj

    conn = CapturingConnection()

    Publisher._chronos_segment_table_delta(
        conn,  # type: ignore[arg-type]
        physical_table="_chronos_b_interval_okg_graph_nodes_fixture",
        key_column="node_id",
        compare_columns=(),
        branch_name="default",
        previous_segment_id="seg_previous",
    )

    assert "t.live_hi <= s.live_hi" in conn.cursor_obj.query
    assert "t.live_lo < s.live_hi" not in conn.cursor_obj.query


def test_chronos_segment_delta_counts_only_current_continuation(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=True,
            interval_continuation_percent=50,
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:old",
                    "concept",
                    {"text": "old"},
                    {"node_id": "n:old"},
                    {"rev": "initial"},
                )
            ],
            source="fixture",
        )
        previous_id = store.checkpoint_generation(
            "delta-continuation-previous",
            deployment_name="test-pub",
            catalog_version_id=1,
        )
        previous = store.get_generation(previous_id)

        assert Publisher._chronos_current_segment_delta_counts(
            conn,
            branch_name="default",
            previous=previous,
        ) == {
            "nodes_added": 0,
            "nodes_retracted": 0,
            "nodes_updated": 0,
            "edges_added": 0,
            "edges_retracted": 0,
            "edges_updated": 0,
        }

        store.apply_facts(
            [
                NodeFact(
                    "n:new",
                    "concept",
                    {"text": "new"},
                    {"node_id": "n:new"},
                    {"rev": "new"},
                ),
                NodeFact(
                    "n:old",
                    "concept",
                    {},
                    {"node_id": "n:old"},
                    {"rev": "retired"},
                    op="R",
                ),
            ],
            source="fixture",
        )

        assert Publisher._chronos_current_segment_delta_counts(
            conn,
            branch_name="default",
            previous=previous,
        ) == {
            "nodes_added": 1,
            "nodes_retracted": 1,
            "nodes_updated": 0,
            "edges_added": 0,
            "edges_retracted": 0,
            "edges_updated": 0,
        }


def test_clean_opt_in_enricher_skips_until_relevant_fact_changes(
    clean_runtime: str,
) -> None:
    class CountingEnricher:
        name = "counting"
        publisher_skip_when_clean = True
        reads_subtypes = ("concept",)
        reads_edge_types = ("contains",)
        emits_edge_types = ()
        requires_narrowings = ()
        # Explicit minimal envelope: the boot guard demands a declared
        # write surface; this fixture's behavior under test is
        # skip-when-clean gating, not admission.
        write_envelope = {
            "edges": [{"src_subtype": "concept", "edge_type": "contains", "dst_subtype": "concept"}],
            "nondeterministic": False,
        }

        def __init__(self) -> None:
            self.calls = 0

        def enrich(self, conn, *, generation_id, incremental=None):
            del conn, generation_id, incremental
            self.calls += 1
            return EnrichResult(self.name, n_edges_emitted=0)

    enricher = CountingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    pub = Publisher(clean_runtime, "test-pub", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = pub.cycle()
    second = pub.cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _insert_concept_fact(cur, "n:second")

    third = pub.cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _insert_contains_edge_fact(cur)

    fourth = pub.cycle()

    assert first["status"] == "published"
    # Chronos-native clean cycles reuse the last checkpoint, signalling an
    # unchanged ("no-op") cycle via the `noop` flag rather than a status="no_op"
    # (the legacy projector's no-op-skip signal).
    assert second["status"] == "published"
    assert second["noop"] is True
    assert third["status"] == "published"
    assert fourth["status"] == "published"
    # The native publisher no longer consumes legacy edge_facts as graph
    # input. It runs on first publish and after the new node fact, but the
    # direct edge_facts insert remains audit-only.
    assert enricher.calls == 2


def test_clean_always_run_enricher_prevents_pre_enrichment_noop_reuse(
    clean_runtime: str,
) -> None:
    class _CountingEnricher:
        reads_subtypes = ("concept",)
        reads_edge_types = ()
        emits_edge_types = ()
        requires_narrowings = ()
        write_envelope = {
            "nodes": [{"subtype": "concept"}],
            "nondeterministic": False,
        }
        declares_node_provenance = True

        def __init__(self, name: str) -> None:
            self.name = name
            self.calls = 0

        def enrich(self, conn, *, generation_id, incremental=None):
            del conn, generation_id, incremental
            self.calls += 1
            return EnrichResult(self.name, n_edges_emitted=0)

    class _AlwaysRun(_CountingEnricher):
        publisher_skip_when_clean = False

    class _CleanOptIn(_CountingEnricher):
        publisher_skip_when_clean = True

    class _HookVeto(_CleanOptIn):
        def can_skip_clean(self, conn, ctx):
            del conn, ctx
            return False

    always_run = _AlwaysRun("always_run")
    clean_opt_in = _CleanOptIn("clean_opt_in")
    hook_veto = _HookVeto("hook_veto")
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    publisher = Publisher(
        clean_runtime,
        "test-pub",
        enrichers=[always_run, clean_opt_in, hook_veto],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"),
    )
    first = publisher.cycle()
    second = publisher.cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert second["noop"] is True
    assert second["reused_generation"] is True
    assert second["generation_id"] == first["generation_id"]
    assert second["noop_reuse_stage"] == "post_enrichment_delta_counts"
    assert second["noop_reuse_invalidation"] == {
        "reason": "enricher_requires_run",
        "owner": "always_run",
    }
    assert second["noop_reuse_invalidation_reason"] == "enricher_requires_run"
    assert always_run.calls == 2
    assert clean_opt_in.calls == 1
    assert hook_veto.calls == 2


def test_publish_blocker_prevents_post_enrichment_noop_reuse(
    clean_runtime: str,
) -> None:
    """A run blocker cuts a durable checkpoint after clean enrichment."""
    class _AlwaysRun:
        name = "always_run"
        publisher_skip_when_clean = False
        reads_subtypes = ("concept",)
        reads_edge_types = ()
        emits_edge_types = ()
        requires_narrowings = ()
        write_envelope = {
            "nodes": [{"subtype": "concept"}],
            "nondeterministic": False,
        }
        declares_node_provenance = True

        def __init__(self) -> None:
            self.calls = 0

        def enrich(self, conn, *, generation_id, incremental=None):
            del conn, generation_id, incremental
            self.calls += 1
            return EnrichResult(self.name, n_edges_emitted=0)

    enricher = _AlwaysRun()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    publisher = Publisher(clean_runtime, "test-pub", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = publisher.cycle()
    blocked = publisher.cycle(publish_blocker={
        "schema": "okg.ingest_completeness.v1",
        "status": "incomplete",
        "reason": "ingest_source_incomplete",
    })

    assert first["status"] == "published"
    assert blocked["status"] == "blocked"
    assert blocked["generation_id"] != first["generation_id"]
    assert blocked.get("reused_generation") is not True
    assert blocked["blocked_reason"]["reason"] == "ingest_source_incomplete"
    assert enricher.calls == 2


def test_locked_redrive_guard_refuses_changed_checkpoint(
    clean_runtime: str,
) -> None:
    """A blocked checkpoint injected after the probe cannot be published."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    publisher = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    baseline = publisher.cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        baseline_branch_token = Publisher.chronos_graph_branch_mutation_token(
            conn,
            branch_name="default",
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(
                "n:partial",
                "concept",
                {"text": "must remain reader-invisible"},
                {},
                {},
            ),
        ])
    injected = publisher.cycle(publish_blocker={
        "schema": "okg.ingest_completeness.v1",
        "status": "incomplete",
        "reason": "ingest_source_incomplete",
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        before = store.list_generations(
            deployment_name="test-pub",
            status=None,
        )

    with pytest.raises(SubstrateError, match="predecessor changed"):
        publisher.cycle(
            expected_latest_checkpoint_id=baseline["generation_id"],
            expected_branch_mutation_token=baseline_branch_token,
            require_expected_latest_checkpoint=True,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        after = store.list_generations(
            deployment_name="test-pub",
            status=None,
        )
        latest_published = store.latest_generation(
            deployment_name="test-pub",
            status="published",
        )
    assert injected["status"] == "blocked"
    assert [generation.checkpoint_id for generation in after] == [
        generation.checkpoint_id for generation in before
    ]
    assert latest_published is not None
    assert latest_published.checkpoint_id == baseline["generation_id"]


def test_locked_redrive_guard_fences_uncheckpointed_source_write(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source write entering after validation stays out of the checkpoint."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    publisher = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    baseline = publisher.cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(conn)
        graph.apply_facts([
            NodeFact("n:staged", "concept", {"text": "staged"}, {}, {}),
        ])
        expected_token = Publisher.chronos_graph_branch_mutation_token(
            conn,
            branch_name="default",
        )

    token_observed = threading.Event()
    writer_reached_fence = threading.Event()
    writer_finished = threading.Event()
    writer_errors: list[BaseException] = []
    original_token = Publisher.chronos_graph_branch_mutation_token
    original_write_lock = ChronosGraphStore._lock_branch_write

    def observed_token(conn, *, branch_name: str) -> str:
        token = original_token(conn, branch_name=branch_name)
        token_observed.set()
        assert writer_reached_fence.wait(timeout=5)
        assert not writer_finished.is_set()
        return token

    def observed_write_lock(self, branch: str) -> None:
        if threading.current_thread().name == "late-source-writer":
            writer_reached_fence.set()
        original_write_lock(self, branch)

    monkeypatch.setattr(
        Publisher,
        "chronos_graph_branch_mutation_token",
        staticmethod(observed_token),
    )
    monkeypatch.setattr(
        ChronosGraphStore,
        "_lock_branch_write",
        observed_write_lock,
    )

    def write_late_source_fact() -> None:
        try:
            assert token_observed.wait(timeout=5)
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                ChronosGraphStore.from_connection(conn).apply_facts([
                    NodeFact(
                        "n:late",
                        "concept",
                        {"text": "must wait for the checkpoint"},
                        {},
                        {},
                    ),
                ])
        except Exception as exc:  # noqa: BLE001 - surface thread failures
            writer_errors.append(exc)
        finally:
            writer_finished.set()

    writer = threading.Thread(
        target=write_late_source_fact,
        name="late-source-writer",
    )
    writer.start()
    redrive = publisher.cycle(
        expected_latest_checkpoint_id=baseline["generation_id"],
        expected_branch_mutation_token=expected_token,
        require_expected_latest_checkpoint=True,
    )
    writer.join(timeout=5)

    assert redrive["status"] == "published"
    assert redrive["generation_id"] != baseline["generation_id"]
    assert writer_finished.is_set()
    assert writer_errors == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published_rows = store.checkout_generation(
            redrive["generation_id"],
        ).query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id IN ('n:staged', 'n:late')"
        )
        current_rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id IN ('n:staged', 'n:late')"
        )
    assert {row["node_id"] for row in published_rows} == {"n:staged"}
    assert {row["node_id"] for row in current_rows} == {
        "n:staged",
        "n:late",
    }


def test_clean_hook_veto_prevents_pre_enrichment_noop_reuse(
    clean_runtime: str,
) -> None:
    class HookVetoEnricher:
        name = "hook_veto_only"
        publisher_skip_when_clean = True
        reads_subtypes = ("concept",)
        reads_edge_types = ()
        emits_edge_types = ()
        requires_narrowings = ()
        write_envelope = {
            "nodes": [{"subtype": "concept"}],
            "nondeterministic": False,
        }
        declares_node_provenance = True

        def __init__(self) -> None:
            self.calls = 0
            self.hook_checks = 0

        def can_skip_clean(self, conn, ctx):
            del conn, ctx
            self.hook_checks += 1
            return False

        def enrich(self, conn, *, generation_id, incremental=None):
            del conn, generation_id, incremental
            self.calls += 1
            return EnrichResult(self.name, n_edges_emitted=0)

    enricher = HookVetoEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    publisher = Publisher(clean_runtime, "test-pub", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = publisher.cycle()
    second = publisher.cycle()

    assert second["status"] == "published"
    assert second["reused_generation"] is True
    assert second["generation_id"] == first["generation_id"]
    assert second["noop_reuse_stage"] == "post_enrichment_delta_counts"
    assert second["noop_reuse_invalidation"] == {
        "reason": "enricher_requires_run",
        "owner": enricher.name,
    }
    assert enricher.calls == 2
    assert enricher.hook_checks == 1


def test_chronos_success_progress_with_zero_fact_watermark_can_skip_clean(
    clean_runtime: str,
) -> None:
    class CountingEnricher:
        name = "counting"
        publisher_skip_when_clean = True
        reads_subtypes = ("concept",)
        reads_edge_types = ()
        emits_edge_types = ()
        requires_narrowings = ()
        # Explicit minimal envelope: the boot guard demands a declared
        # write surface; this fixture's behavior under test is
        # skip-when-clean gating, not admission.
        write_envelope = {
            "edges": [{"src_subtype": "concept", "edge_type": "contains", "dst_subtype": "concept"}],
            "nondeterministic": False,
        }

        def __init__(self) -> None:
            self.calls = 0

        def enrich(self, conn, *, generation_id, incremental=None):
            del conn, generation_id, incremental
            self.calls += 1
            return EnrichResult(self.name, n_edges_emitted=0)

    enricher = CountingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test-pub")
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
               AND deployment_name = 'test-pub'
            """
        )
        cv = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.enricher_progress
              (deployment_name, enricher_name, fact_watermark, edge_watermark,
               generation_id, catalog_version_id, last_status, last_run_at)
            VALUES ('test-pub', 'counting', 0, 0, 0, %s, 'success', now())
            """,
            (cv,),
        )

    out = Publisher(
        clean_runtime,
        "test-pub",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    assert out["status"] == "published"
    assert out["noop"] is True
    assert enricher.calls == 0


def test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains(
    clean_runtime: str,
) -> None:
    """Issue #713: same-cycle upstream output drains on the next cycle."""
    upstream = _Issue713NodeEmitter(
        name="issue713_upstream",
        reads_subtype="issue713_source",
        input_node_id=_ISSUE_713_SOURCE_ID,
        output_subtype="issue713_mid",
        output_node_id=_ISSUE_713_MID_ID,
    )
    downstream = _Issue713NodeEmitter(
        name="issue713_downstream",
        reads_subtype="issue713_mid",
        input_node_id=_ISSUE_713_MID_ID,
        output_subtype="issue713_final",
        output_node_id=_ISSUE_713_FINAL_ID,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_issue_713_catalog(conn)

    publisher = Publisher(
        clean_runtime,
        "issue713",
        enrichers=[upstream, downstream],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713"),
    )
    warmup = publisher.cycle()
    assert warmup["status"] == "published"
    assert upstream.calls == 1
    assert downstream.calls == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    _ISSUE_713_SOURCE_ID,
                    "issue713_source",
                    {"text": "source"},
                    {"node_id": _ISSUE_713_SOURCE_ID},
                    {"revision": 1},
                ),
            ],
            source="issue713_fixture",
            run_id=str(uuid.uuid4()),
        )

    upstream_cycle = publisher.cycle()
    assert upstream_cycle["status"] == "published"
    assert _issue_713_generation_node_count(
        clean_runtime,
        upstream_cycle["generation_id"],
        _ISSUE_713_MID_ID,
    ) == 1
    assert _issue_713_generation_node_count(
        clean_runtime,
        upstream_cycle["generation_id"],
        _ISSUE_713_FINAL_ID,
    ) == 0

    drained = publisher.cycle()
    assert drained["status"] == "published"
    assert drained.get("reused_generation") is not True
    assert drained["generation_id"] != upstream_cycle["generation_id"]
    assert drained["noop_reuse_invalidation_reason"] == (
        "pending_enricher_backlog"
    )
    assert _ISSUE_713_MID_ID in downstream.contexts[-1].dirty_node_ids
    assert _issue_713_generation_node_count(
        clean_runtime,
        drained["generation_id"],
        _ISSUE_713_FINAL_ID,
    ) == 1

    settled = publisher.cycle()
    assert settled["status"] == "published"
    assert settled.get("reused_generation") is True
    assert settled["generation_id"] == drained["generation_id"]
    assert settled["noop_reuse_stage"] == "pre_enrichment_delta_probe"


def test_enricher_backlog_probe_ignores_unread_subtypes(
    clean_runtime: str,
) -> None:
    enricher = _Issue713CountingEnricher(
        "issue713_subtype_filter",
        ("issue713_source",),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_issue_713_catalog(conn)

    publisher = Publisher(clean_runtime, "issue713", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713"))
    first = publisher.cycle()
    assert first["status"] == "published"
    assert enricher.calls == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_issue_713_node_fact(
            conn,
            node_id="issue713:unread",
            subtype="issue713_unread",
        )
        assert publisher._has_pending_enricher_backlog(conn) is False
    assert enricher.calls == 1


def test_enricher_backlog_probe_treats_empty_node_selector_as_every_subtype(
    clean_runtime: str,
) -> None:
    """An edge-only selector must not fail open.

    ``_query_dirty_from_node_facts`` reads an empty ``reads_subtypes`` as
    "every subtype". If the backlog probe read it as "no subtype" instead,
    an enricher declaring only ``reads_edge_types`` would be reused past
    while holding real node backlog -- the #713 defect in a narrower shape.
    The fully selectorless case is handled separately by
    ``_noop_reuse_enricher_requires_run_name``; this one declares an edge
    selector, so that clause does not fire.
    """
    enricher = _Issue713CountingEnricher("issue713_edge_only", ())
    enricher.reads_edge_types = ("issue713_contains",)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_issue_713_catalog(conn)

    publisher = Publisher(clean_runtime, "issue713", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713"))
    first = publisher.cycle()
    assert first["status"] == "published"

    assert publisher._noop_reuse_enricher_requires_run_name() is None

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_issue_713_node_fact(
            conn,
            node_id="issue713:edge_only_backlog",
            subtype="issue713_unread",
        )
        assert publisher._has_pending_enricher_backlog(conn) is True

    # The predicate is only half the claim: prove it actually changes the
    # reuse decision and hands the enricher another run.
    second = publisher.cycle()
    assert second["status"] == "published"
    assert second.get("reused_generation") is not True
    assert second["noop_reuse_invalidation"]["reason"] == (
        "pending_enricher_backlog"
    )
    assert enricher.calls == 2


def test_unprovable_enricher_cleanliness_prevents_early_noop_reuse(
    clean_runtime: str,
) -> None:
    selectorless = _Issue713CountingEnricher(
        "issue713_selectorless",
        (),
    )
    stale = _Issue713CountingEnricher(
        "issue713_stale",
        ("issue713_source",),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_issue_713_catalog(conn)

    selectorless_publisher = Publisher(
        clean_runtime,
        "issue713-selectorless",
        enrichers=[selectorless],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713-selectorless"),
    )
    selectorless_first = selectorless_publisher.cycle()
    selectorless_second = selectorless_publisher.cycle()
    assert selectorless_second.get("reused_generation") is True
    assert selectorless_second["generation_id"] == (
        selectorless_first["generation_id"]
    )
    assert selectorless_second["noop_reuse_stage"] == (
        "post_enrichment_delta_counts"
    )
    assert selectorless_second["noop_reuse_invalidation"] == {
        "reason": "enricher_requires_run",
        "owner": selectorless.name,
    }

    stale_publisher = Publisher(
        clean_runtime,
        "issue713-stale",
        enrichers=[stale],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713-stale"),
    )
    stale_first = stale_publisher.cycle()
    assert stale_first["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.enricher_progress
                   SET last_run_at = now() - interval '181 days'
                 WHERE deployment_name = 'issue713-stale'
                   AND enricher_name = %s
                """,
                (stale.name,),
            )

    stale_second = stale_publisher.cycle()
    assert stale.calls == 2
    assert stale.contexts[-1].is_first_run is True
    assert stale_second["noop_reuse_stage"] == "post_enrichment_delta_counts"
    assert stale_second["noop_reuse_invalidation"] == {
        "reason": "pending_enricher_backlog",
        "owner": "enrichers",
    }


def test_pending_enricher_backlog_prevents_post_enrichment_noop_reuse(
    clean_runtime: str,
) -> None:
    """The final reuse decision must catch lag created after phase start."""
    upstream = _Issue713NodeEmitter(
        name="issue713_always_upstream",
        reads_subtype="issue713_source",
        input_node_id=_ISSUE_713_SOURCE_ID,
        output_subtype="issue713_mid",
        output_node_id=_ISSUE_713_MID_ID,
        always_emit=True,
    )
    upstream.publisher_skip_when_clean = False
    downstream = _Issue713NodeEmitter(
        name="issue713_post_downstream",
        reads_subtype="issue713_mid",
        input_node_id=_ISSUE_713_MID_ID,
        output_subtype="issue713_final",
        output_node_id=_ISSUE_713_FINAL_ID,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_issue_713_catalog(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    _ISSUE_713_SOURCE_ID,
                    "issue713_source",
                    {"text": "source"},
                    {"node_id": _ISSUE_713_SOURCE_ID},
                    {"revision": 1},
                ),
            ],
            source="issue713_fixture",
            run_id=str(uuid.uuid4()),
        )

    publisher = Publisher(
        clean_runtime,
        "issue713-post",
        enrichers=[upstream, downstream],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713-post"),
    )
    first = publisher.cycle()
    second = publisher.cycle()
    third = publisher.cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert third["status"] == "published"
    assert third.get("reused_generation") is not True
    assert third["generation_id"] != second["generation_id"]
    assert third["noop"] is True
    assert third["noop_reuse_invalidation"] == {
        "reason": "pending_enricher_backlog",
        "owner": "enrichers",
    }


def test_matching_retraction_is_pending_enricher_backlog(
    clean_runtime: str,
) -> None:
    enricher = _Issue713CountingEnricher(
        "issue713_retraction",
        ("issue713_source",),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_issue_713_catalog(conn)

    publisher = Publisher(clean_runtime, "issue713", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "issue713"))
    first = publisher.cycle()
    assert first["status"] == "published"
    assert enricher.calls == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_issue_713_node_fact(
            conn,
            node_id=_ISSUE_713_SOURCE_ID,
            subtype="issue713_source",
            op="R",
        )

    second = publisher.cycle()
    assert second["status"] == "published"
    assert enricher.calls == 2
    assert _ISSUE_713_SOURCE_ID in enricher.contexts[-1].dirty_node_ids
    assert second["noop_reuse_invalidation_reason"] == (
        "pending_enricher_backlog"
    )


def test_p6_force_enrichers_cuts_checkpoint_on_zero_delta(
    clean_runtime: str,
) -> None:
    """P6 (specify-publish-commit-protocol #708): ``--force-enrichers``
    must cut a fresh checkpoint even when the forced recompute is
    zero-delta (an idempotent re-derive on an unchanged, zero-source-work
    deployment).

    Regression guard for the review finding: the POST-enrichment
    noop-reuse early-return (``publisher.py`` reuse guard,
    ``reuse_stage="post_enrichment_delta_counts"``) was not force-aware.
    A forced cycle bypasses the PRE-enrichment skip, runs enrichers,
    recomputes a zero delta, and -- without the ``and not force_enrichers``
    clause -- short-circuited into the reuse result, skipping ``gates``
    and ``checkpoint_generation``. That contradicts the force-lever
    acceptance ("runs enrichers, gates, checkpoint; skipped_stages
    empty"). This test drives a full forced cycle and asserts no stage is
    skipped and a new generation is cut; it fails on the pre-fix code
    (forced cycle reuses) and passes with the fix.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = pub.cycle()
    assert first["status"] == "published"

    # Control: an unforced second cycle on the now zero-delta deployment
    # reuses the prior generation and SKIPS the checkpoint stage.
    control = pub.cycle()
    assert control["status"] == "published"
    assert control["noop"] is True
    assert control.get("reused_generation") is True
    assert "checkpoint_generation" in (
        control.get("publish_skipped_stages") or []
    )
    assert control["generation_id"] == first["generation_id"]

    # Forced: the same zero-delta cycle must NOT reuse. It runs the full
    # path (gates + checkpoint), so no stage is skipped and a fresh
    # checkpoint is cut.
    forced = pub.cycle(force_enrichers=True)
    assert forced["status"] == "published"
    assert forced.get("reused_generation") is not True
    assert not (forced.get("publish_skipped_stages") or [])
    assert forced.get("noop_reuse_stage") is None
    assert forced["generation_id"] != control["generation_id"]


def test_p6_force_enrichers_runs_clean_enricher_as_first_run(
    clean_runtime: str,
) -> None:
    """P6: force mode reaches the enricher context, not only reuse gates."""

    class CleanSkippableEnricher:
        name = "forced-clean"
        publisher_skip_when_clean = True
        reads_subtypes = ("concept",)
        reads_edge_types = ()
        emits_edge_types = ()
        requires_narrowings = ()
        write_envelope = {
            "nodes": [{"subtype": "concept"}],
            "nondeterministic": False,
        }
        # Node surface declared only to satisfy the write-envelope boot
        # guard; this fixture emits no node facts, so the provenance
        # declaration is vacuously satisfied. Behavior under test is the
        # force-enrichers lever, not admission.
        declares_node_provenance = True

        def __init__(self) -> None:
            self.contexts = []

        def enrich(self, conn, *, generation_id, incremental=None):
            del conn, generation_id
            self.contexts.append(incremental)
            return EnrichResult(self.name, n_nodes_emitted=0)

    enricher = CleanSkippableEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test-pub")
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
               AND deployment_name = 'test-pub'
            """
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.enricher_progress
              (deployment_name, enricher_name, fact_watermark, edge_watermark,
               generation_id, catalog_version_id, last_status, last_run_at)
            VALUES
              ('test-pub', 'forced-clean', 0, 0, 0, %s, 'success', now())
            """,
            (catalog_version_id,),
        )

    pub = Publisher(
        clean_runtime,
        "test-pub",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    )
    control = pub.cycle()
    assert control["noop"] is True
    assert enricher.contexts == []

    forced = pub.cycle(force_enrichers=True)
    assert forced["status"] == "published"
    assert len(enricher.contexts) == 1
    assert enricher.contexts[0].is_first_run is True
    assert enricher.contexts[0].change_basis == "operator_forced"


def test_p2_reconcile_failure_rolls_back_and_refuses_checkpoint(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2: a cycle-level reconcile failure leaves no ordinal/checkpoint."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")
        cur.execute("SELECT count(*) FROM okg.generation_ordinals")
        ordinals_before = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM _chronos_branch_interval_checkpoints")
        checkpoints_before = int(cur.fetchone()[0])

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))

    def fail_reconcile(*_args, **_kwargs):
        raise SubstrateError("induced interval reconcile failure")

    monkeypatch.setattr(
        pub,
        "_reconcile_effective_interval_view",
        fail_reconcile,
    )

    with pytest.raises(SubstrateError, match="induced interval reconcile"):
        pub.cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.generation_ordinals")
        assert int(cur.fetchone()[0]) == ordinals_before
        cur.execute("SELECT count(*) FROM _chronos_branch_interval_checkpoints")
        assert int(cur.fetchone()[0]) == checkpoints_before


def test_chronos_dirty_scope_fails_loud_when_interval_metadata_is_corrupt(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    assert pub.cycle()["status"] == "published"

    with psycopg.connect(clean_runtime) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE _chronos_branch_tables
                       SET physical_table = 'missing_edge_table_for_test'
                     WHERE table_name = 'okg.graph_edges'
                    """
                )
            with pytest.raises(psycopg.errors.UndefinedTable):
                Publisher._query_dirty_from_chronos_current_segment(
                    conn,
                    ("concept",),
                    (),
                    branch_name="default",
                )


def test_two_sequential_clean_cycles_reuse_generation(clean_runtime: str) -> None:
    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    a = pub.cycle()
    b = pub.cycle()
    assert b["generation_id"] == a["generation_id"]
    assert b["noop"] is True
    assert b["reused_generation"] is True


def test_concurrent_cycle_raises_publisher_lease_held(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two publishers on the same deployment: second sees PublisherLeaseHeld."""
    monkeypatch.setenv("OKG_DBOS_PUBLISH_LOCK_WAIT_SECONDS", "0")
    held = threading.Event()
    released = threading.Event()
    holder_result: dict[str, Any] = {}

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:lock-holder")

    class SlowPublisher(Publisher):
        def _cycle_chronos_native(
            self,
            conn=None,
            **_kwargs,
        ):
            del _kwargs
            if conn is None:
                with psycopg.connect(self.dsn, autocommit=True) as local_conn:
                    with self._locked_connection(local_conn):
                        held.set()
                        released.wait(timeout=5.0)
            else:
                with self._locked_connection(conn):
                    held.set()
                    released.wait(timeout=5.0)
            return {"status": "published"}

        def run_gates(self, g, phase_a):
            held.set()
            released.wait(timeout=5.0)
            return super().run_gates(g, phase_a)

    def hold_thread() -> None:
        try:
            holder_result["ok"] = SlowPublisher(clean_runtime, "test-pub").cycle()
        except Exception as e:
            holder_result["err"] = e

    t = threading.Thread(target=hold_thread)
    t.start()
    try:
        assert held.wait(timeout=5.0), holder_result.get("err")
        with pytest.raises(PublisherLockHeld) as excinfo:
            Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub")).cycle()
        assert excinfo.value.code == "publisher_lease_held"
        assert excinfo.value.data["coordination_mode"] == "dbos"
        assert excinfo.value.data["advisory_key"]
    finally:
        released.set()
        t.join(timeout=5.0)
    assert "err" not in holder_result, holder_result.get("err")


def test_dbos_mode_uses_publisher_lock_across_callers(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DBOS queue isolation must not allow overlapping publisher cycles."""
    monkeypatch.setenv("OKG_DBOS_PUBLISH_LOCK_WAIT_SECONDS", "0")
    held = threading.Event()
    released = threading.Event()
    holder_result: dict[str, Any] = {}

    class SlowPublisher(Publisher):
        def _cycle_chronos_native(
            self,
            conn=None,
            **_kwargs,
        ):
            del _kwargs
            if conn is None:
                with psycopg.connect(self.dsn, autocommit=True) as local_conn:
                    with self._locked_connection(local_conn):
                        held.set()
                        released.wait(timeout=5.0)
            else:
                with self._locked_connection(conn):
                    held.set()
                    released.wait(timeout=5.0)
            return {"status": "published"}

    def hold_thread() -> None:
        try:
            holder_result["ok"] = SlowPublisher(
                clean_runtime,
                "test-pub",
                coordination_mode="dbos",
            ).cycle()
        except Exception as e:
            holder_result["err"] = e

    t = threading.Thread(target=hold_thread)
    t.start()
    try:
        assert held.wait(timeout=5.0), holder_result.get("err")
        with pytest.raises(PublisherLockHeld) as excinfo:
            Publisher(
                clean_runtime,
                "test-pub",
                coordination_mode="dbos",
                catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"),
            ).cycle()
        assert excinfo.value.code == "publisher_lease_held"
        assert excinfo.value.data["coordination_mode"] == "dbos"
    finally:
        released.set()
        t.join(timeout=5.0)

    assert "err" not in holder_result, holder_result.get("err")
    assert holder_result["ok"]["status"] == "published"


def test_source_sync_publish_context_waits_out_source_sync_admission_intent(
    clean_runtime: str,
) -> None:
    """A source-sync publish sees another holder of the source-sync signal
    (#3003) but waits for it instead of aborting: a short enqueue releases
    the admission-intent lock and the publish proceeds."""
    released = threading.Event()

    def hold_briefly() -> None:
        with source_sync_admission_intent_lock(
            dsn=clean_runtime,
            deployment="test-pub",
        ):
            held.set()
            released.wait(timeout=0.5)

    held = threading.Event()
    holder = threading.Thread(target=hold_briefly)
    holder.start()
    try:
        assert held.wait(timeout=5.0)
        with psycopg.connect(clean_runtime) as conn:
            scheduled = Publisher(clean_runtime, "test-pub")
            assert scheduled._blocking_source_sync_workflow_id(
                conn=conn,
            ) == SOURCE_SYNC_ADMISSION_INTENT_ID

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            source_sync = Publisher(
                clean_runtime,
                "test-pub",
                source_sync_publish_context=True,
            )
            assert source_sync._blocking_source_sync_workflow_id(
                conn=conn,
            ) == SOURCE_SYNC_ADMISSION_INTENT_ID
            key = source_sync._acquire_dbos_publish_lock(conn, wait_seconds=10)
            # It got the mutex only once the holder had let go.
            holder.join(timeout=5.0)
            assert source_sync._blocking_source_sync_workflow_id(
                conn=conn,
            ) is None
            source_sync._release_dbos_publish_lock(conn, key)
    finally:
        released.set()
        holder.join(timeout=5.0)


def test_source_sync_admission_serializes_locally_before_connecting(
    clean_runtime: str,
) -> None:
    """Resume bursts must wait in-process, not consume one DB slot per poller."""
    deployment = f"test-pub-local-{uuid.uuid4()}"
    local_lock = _local_source_sync_admission_lock(deployment)
    entered = threading.Event()
    finished = threading.Event()
    errors: list[Exception] = []

    def contender() -> None:
        try:
            with source_sync_admission_intent_lock(
                dsn=clean_runtime,
                deployment=deployment,
            ):
                entered.set()
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    local_lock.acquire()
    thread = threading.Thread(target=contender)
    thread.start()
    try:
        assert not entered.wait(timeout=0.2)
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND query LIKE 'SELECT pg_advisory_lock%%'
                """
            )
            assert int(cur.fetchone()[0]) == 0
    finally:
        local_lock.release()
        thread.join(timeout=5.0)

    assert finished.is_set()
    assert entered.is_set()
    assert not errors


def test_different_deployments_do_not_conflict(clean_runtime: str) -> None:
    """Two publishers on DIFFERENT deployment names can run
    concurrently because they hash to different advisory keys."""
    a = Publisher(clean_runtime, "dep-a", catalog_identity=bind_test_catalog_writer(clean_runtime, "dep-a"))
    b = Publisher(clean_runtime, "dep-b", catalog_identity=bind_test_catalog_writer(clean_runtime, "dep-b"))
    ra = a.cycle()
    rb = b.cycle()
    assert ra["status"] == "published"
    assert rb["status"] == "published"


# ---------- M1.5 regression tests ----------

def test_one_pending_per_deployment_enforced_by_schema(
    clean_runtime: str,
) -> None:
    """Per Dijkstra M1 review #5: the 'one pending per deployment'
    invariant MUST be enforced by a partial UNIQUE index, not just
    publisher discipline."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "  (deployment_name, status, catalog_version_id, "
            "   correlation_id) "
            "VALUES ('dep-x', 'pending', 1, 'first');"
        )
        # A second pending row for the same deployment must violate
        # the partial unique index.
        with pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute(
                "INSERT INTO okg.graph_generations "
                "  (deployment_name, status, catalog_version_id, "
                "   correlation_id) "
                "VALUES ('dep-x', 'pending', 1, 'second');"
            )


def test_advisory_key_distinguishes_deployment_names(
    clean_runtime: str,
) -> None:
    """Per the M1 design review #3 + Dijkstra #10 (3): the
    advisory-lock key MUST give different deployments different
    locks (64-bit derivation, not 32-bit hashtext)."""
    from okg.substrate.publisher.publisher import _advisory_keys
    a = _advisory_keys("dep-a")
    b = _advisory_keys("dep-b")
    assert a != b
    # Sanity: deterministic.
    assert _advisory_keys("dep-a") == a


def _activate_generation_publish_policy(
    dsn: str,
    *,
    rules: list[dict[str, object]],
) -> None:
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:generation-publish",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules or [{
            "rule_id": "allow_other_publisher",
            "effect": "allow",
            "actions": ["generation.publish"],
            "subjects": ["okg_service:someone-else"],
            "priority": 1,
            "reason_code": "nomos.test.unmatched_generation_publish",
        }],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


# --- #2229: writer identity is the deployment and its claim, not the schema.
# These PostgreSQL proofs live here, not in test_catalog_ownership.py, so the
# per-PR core-db-contracts step runs them (that file has no per-PR owner).


def _published_generation_ids(dsn: str, name: str) -> list[str]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return [
            str(item.checkpoint_id)
            for item in store.list_generations(
                deployment_name=name, status="published",
            )
        ]
    finally:
        store.close()


def _owner_row(conn: psycopg.Connection, name: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT owner_fingerprint, schema_fingerprint, metadata
              FROM okg.deployment_write_ownership
             WHERE deployment_name = %s
            """,
            (name,),
        )
        row = cur.fetchone()
    assert row is not None
    return {
        "owner_fingerprint": row[0],
        "schema_fingerprint": row[1],
        "metadata": row[2] or {},
    }


def _publish(dsn: str, name: str, identity) -> dict:
    return Publisher(
        dsn, name, catalog_identity=identity, sync_chronos_graph=False,
    ).cycle()


def _rewrite_as_v1_writer(conn: psycopg.Connection, identity) -> str:
    """Put the database in its pre-#2229 state for this writer.

    The owner row and this writer's applied catalog versions carry the v1
    key, which bound the schema fingerprint the row records.
    """
    # Imported here so the rest of the module still collects against a
    # tree that predates the helper.
    from okg.substrate.catalog.ownership import legacy_writer_fingerprint_for

    legacy = legacy_writer_fingerprint_for(identity, identity.schema_fingerprint)
    assert legacy and legacy != identity.writer_fingerprint
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.deployment_write_ownership
               SET owner_fingerprint = %s,
                   metadata = metadata - 'writer_fingerprint_rekey'
             WHERE deployment_name = %s
               AND schema_fingerprint = %s
            """,
            (legacy, identity.deployment_name, identity.schema_fingerprint),
        )
        assert cur.rowcount == 1
        cur.execute(
            """
            UPDATE okg.catalog_versions
               SET writer_fingerprint = %s
             WHERE deployment_name = %s
               AND writer_fingerprint = %s
            """,
            (legacy, identity.deployment_name, identity.writer_fingerprint),
        )
        assert cur.rowcount >= 1
    return legacy


def test_claimed_deployment_adds_a_module_then_loads_and_publishes(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2229 acceptance: adding a module is a catalog load, not a new writer."""
    name = "catalog-module-add"
    deployment_dir = _deployment_dir(tmp_path, name)
    manifest = deployment_dir / "deployment.yaml"
    manifest.write_text(
        f"name: {name}\nmodules: []\npostgres:\n  dsn: {clean_runtime}\n",
    )
    monkeypatch.delenv("OKG_AGENT", raising=False)
    load_args = [
        "--json", "catalog", "load",
        "--deployment", str(deployment_dir), "--apply",
    ]

    def _identity_now(command: str):
        return catalog_identity_for(
            composed=compose_catalog(deployment_dir),
            deployment_dir=deployment_dir,
            command=command,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
    assert main(load_args) == 0, capsys.readouterr()
    capsys.readouterr()
    before = _identity_now("publish before module add")
    first = _publish(clean_runtime, name, before)
    assert first["status"] == "published", first

    manifest.write_text(
        f"name: {name}\nmodules: [extraction]\n"
        f"postgres:\n  dsn: {clean_runtime}\n",
    )
    after = _identity_now("publish after module add")
    assert after.catalog_fingerprint != before.catalog_fingerprint
    assert after.writer_fingerprint == before.writer_fingerprint

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        diagnosed = diagnose_catalog_ownership(conn, identity=after)
        count_before_load = _catalog_version_count(conn)
    # A schema difference reads as "catalog load needed", not a foreign writer.
    assert diagnosed["status"] == "latest_incompatible", diagnosed["status"]

    assert main(load_args) == 0, capsys.readouterr()
    loaded = json.loads(capsys.readouterr().out)
    assert loaded["applied"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        owner = _owner_row(conn, name)
        assert _catalog_version_count(conn) == count_before_load + 1
        # `legacy_history` is the seeded pre-provenance bootstrap row.
        assert diagnose_catalog_ownership(conn, identity=after)["status"] in {
            "ok", "legacy_history",
        }
    assert owner["owner_fingerprint"] == after.writer_fingerprint
    assert owner["schema_fingerprint"] == after.schema_fingerprint

    second = _publish(clean_runtime, name, after)
    assert second["status"] == "published", second
    assert second["generation_id"] != first["generation_id"]
    assert set(_published_generation_ids(clean_runtime, name)) >= {
        str(second["generation_id"]), str(first["generation_id"]),
    }


@pytest.mark.parametrize("owner_key", ["current", "legacy"])
def test_a_load_over_a_retired_manifest_loads_but_cannot_publish(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    owner_key: str,
) -> None:
    """#2229 review: the write gate does not refuse manifest drift (guided
    add-source loads a changed manifest before it re-claims), so a direct
    load over a retired manifest succeeds as this writer. The retired
    manifest still cannot publish: the publish gate's continuity check
    refuses it with ``catalog_manifest_drifted``, the load does not restamp
    the recorded manifest, and only the audited re-claim re-arms it."""
    name = f"catalog-retired-manifest-{owner_key}"
    deployment_dir = _deployment_dir(tmp_path, name)
    manifest = deployment_dir / "deployment.yaml"
    manifest.write_text(
        f"name: {name}\nmodules: []\npostgres:\n  dsn: {clean_runtime}\n",
    )
    monkeypatch.delenv("OKG_AGENT", raising=False)
    load_args = [
        "--json", "catalog", "load",
        "--deployment", str(deployment_dir), "--apply",
    ]

    def _identity_now(command: str):
        return catalog_identity_for(
            composed=compose_catalog(deployment_dir),
            deployment_dir=deployment_dir,
            command=command,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
    assert main(load_args) == 0, capsys.readouterr()
    capsys.readouterr()
    before = _identity_now("before the ontology edit")
    retired = "sha256:" + "0" * 64
    assert before.manifest_fingerprint not in (None, retired)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        if owner_key == "legacy":
            _rewrite_as_v1_writer(conn, before)
        with conn.cursor() as cur:
            # The claim recorded a manifest this checkout no longer has
            # (a branch flip or an unclaimed manifest edit).
            cur.execute(
                "UPDATE okg.deployment_write_ownership "
                "SET manifest_fingerprint = %s WHERE deployment_name = %s",
                (retired, name),
            )
            assert cur.rowcount == 1

    manifest.write_text(
        f"name: {name}\nmodules: [extraction]\n"
        f"postgres:\n  dsn: {clean_runtime}\n",
    )
    after = _identity_now("load over a retired manifest")
    assert after.writer_fingerprint == before.writer_fingerprint
    assert after.manifest_fingerprint == before.manifest_fingerprint

    def _state(conn: psycopg.Connection) -> tuple:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT owner_fingerprint, manifest_fingerprint, "
                "schema_fingerprint, metadata "
                "FROM okg.deployment_write_ownership "
                "WHERE deployment_name = %s",
                (name,),
            )
            owner = cur.fetchone()
            cur.execute(
                "SELECT catalog_version_id, status FROM okg.catalog_versions "
                "ORDER BY catalog_version_id"
            )
            return owner, cur.fetchall()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        state_before = _state(conn)
        assert state_before[0][1] == retired

    # The write gate recognizes this writer and lets the load through.
    assert main(load_args) == 0, capsys.readouterr()
    assert json.loads(capsys.readouterr().out)["applied"] is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        owner_loaded, versions_loaded = _state(conn)
    assert owner_loaded[0] == after.writer_fingerprint
    assert owner_loaded[2] == after.schema_fingerprint
    assert len(versions_loaded) == len(state_before[1]) + 1
    # The load does not restamp the recorded manifest.
    assert owner_loaded[1] == retired

    # The publish gate refuses the retired manifest, and writes nothing.
    with pytest.raises(CatalogOwnershipMismatch) as publish_refused:
        Publisher(
            clean_runtime, name,
            catalog_identity=after, sync_chronos_graph=False,
        )
    assert publish_refused.value.code == "catalog_manifest_drifted"
    assert f"recorded={retired}" in str(publish_refused.value)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _state(conn) == (owner_loaded, versions_loaded)

    # The audited re-claim re-arms the live manifest, and the publish then
    # succeeds as this writer.
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(deployment_dir),
    ]) == 0, capsys.readouterr()
    capsys.readouterr()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        owner_after, _versions_after = _state(conn)
    assert owner_after[0] == after.writer_fingerprint
    assert owner_after[1] == after.manifest_fingerprint
    published = _publish(clean_runtime, name, after)
    assert published["status"] == "published", published


def test_a_foreign_path_or_environment_is_still_refused(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    name = "catalog-foreign-still-refused"
    mine = _identity(tmp_path, name)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    foreign_path = catalog_identity_for(
        composed=_composed(name),
        deployment_dir=_deployment_dir(elsewhere, name),
        command="foreign checkout",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=mine)
        apply_composed_catalog(conn, _composed(name), identity=mine)
        owner_before = _owner_row(conn, name)
        versions_before = _catalog_version_count(conn)

        with pytest.raises(CatalogOwnershipMismatch) as refused:
            ensure_catalog_write_allowed(conn, identity=foreign_path)
        assert refused.value.code == "catalog_ownership_foreign_writer"
        assert _owner_row(conn, name) == owner_before
        assert _catalog_version_count(conn) == versions_before

    with pytest.raises(CatalogOwnershipMismatch) as publish_refused:
        Publisher(
            clean_runtime, name,
            catalog_identity=foreign_path, sync_chronos_graph=False,
        )
    assert publish_refused.value.code == "catalog_ownership_foreign_writer"

    env_name = "catalog-foreign-environment"
    env_dir = _deployment_dir(tmp_path, env_name)

    def _env_identity(environment_id: str):
        return catalog_identity_for(
            composed=_composed(env_name),
            deployment_dir=env_dir,
            command="environment writer",
            writer_scope="environment",
            environment_id=environment_id,
        )

    production = _env_identity("env-production")
    staging = _env_identity("env-staging")
    assert production.writer_fingerprint != staging.writer_fingerprint
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_catalog_write_allowed(conn, identity=production)
        env_owner_before = _owner_row(conn, env_name)
        with pytest.raises(CatalogOwnershipMismatch) as env_refused:
            ensure_catalog_write_allowed(conn, identity=staging)
        # The scope comparison runs before the owner-key check and names
        # the environment difference (unchanged by #2229).
        assert env_refused.value.code == "catalog_writer_scope_change"
        assert _owner_row(conn, env_name) == env_owner_before


def test_a_v1_owner_row_is_recognized_and_rekeyed_once(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    name = "catalog-v1-rekey"
    base = _identity(tmp_path, name)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=base)
        apply_composed_catalog(conn, _composed(name), identity=base)
        legacy = _rewrite_as_v1_writer(conn, base)

        # Diagnose accepts the old key and writes nothing.
        report = diagnose_catalog_ownership(conn, identity=base)
        assert report["ok"] is True, report["status"]
        assert report["status"] in {"ok", "legacy_history"}
        assert report["owner_key"] == "legacy"
        assert report["latest_compatible"] is True
        assert _owner_row(conn, name)["owner_fingerprint"] == legacy

    # Publish accepts the old key (owner row and applied catalog version)
    # without writing.
    published = _publish(clean_runtime, name, base)
    assert published["status"] == "published", published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _owner_row(conn, name)["owner_fingerprint"] == legacy

    first_load = catalog_identity_for(
        composed=_composed(name, extra=True),
        deployment_dir=_deployment_dir(tmp_path, name),
        command="first load after upgrade",
    )
    second_load = replace(
        first_load,
        provenance={**first_load.provenance, "command": "second load"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_catalog_write_allowed(conn, identity=first_load)
        apply_composed_catalog(
            conn, _composed(name, extra=True), identity=first_load,
        )
        rekeyed = _owner_row(conn, name)
        assert rekeyed["owner_fingerprint"] == first_load.writer_fingerprint
        assert rekeyed["metadata"]["writer_fingerprint_rekey"] == {
            "from": legacy,
            "to": first_load.writer_fingerprint,
            "command": "first load after upgrade",
        }

        ensure_catalog_write_allowed(conn, identity=second_load)
        apply_composed_catalog(
            conn, _composed(name, extra=True), identity=second_load,
        )
        again = _owner_row(conn, name)
        assert again["owner_fingerprint"] == first_load.writer_fingerprint
        # No second re-key: the audit still names the first load.
        assert again["metadata"]["writer_fingerprint_rekey"] == (
            rekeyed["metadata"]["writer_fingerprint_rekey"]
        )

    republished = _publish(clean_runtime, name, first_load)
    assert republished["status"] == "published", republished


def test_a_claim_that_loses_the_rekey_race_keeps_the_winners_audit(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2229 review F2: the claim reads the owner row without a lock and sees
    this writer's v1 key; another process re-keys first. The claim's
    metadata-replacing upsert must keep the winner's audit, read under the
    row lock, not drop it for its own stale snapshot."""
    name = "catalog-v1-rekey-race"
    base = _identity(tmp_path, name)
    winner = replace(
        base, provenance={**base.provenance, "command": "competing process"},
    )
    loser = replace(
        base, provenance={**base.provenance, "command": "racing claim"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=base)
        apply_composed_catalog(conn, _composed(name), identity=base)
        legacy = _rewrite_as_v1_writer(conn, base)
        assert "writer_fingerprint_rekey" not in _owner_row(conn, name)[
            "metadata"
        ]

    real_rekey = ownership.rekey_legacy_owner
    competing: list[dict] = []

    def rekey_after_a_competitor(conn, *, identity):
        # Force the interleaving: the competitor's re-key commits between
        # the claim's unlocked read and its own locked re-key.
        if not competing:
            with psycopg.connect(clean_runtime, autocommit=True) as other:
                competing.append(real_rekey(other, identity=winner))
        return real_rekey(conn, identity=identity)

    monkeypatch.setattr(ownership, "rekey_legacy_owner", rekey_after_a_competitor)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=loser)
        row = _owner_row(conn, name)

    winners_audit = {
        "from": legacy,
        "to": base.writer_fingerprint,
        "command": "competing process",
    }
    assert competing == [{"rekeyed": True, **winners_audit}]
    assert row["owner_fingerprint"] == base.writer_fingerprint
    assert row["metadata"]["writer_fingerprint_rekey"] == winners_audit
    assert row["metadata"]["command"] == "racing claim"


def test_a_foreign_v1_owner_row_is_still_refused(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    name = "catalog-foreign-v1"
    mine = _identity(tmp_path, name)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    theirs = catalog_identity_for(
        composed=_composed(name),
        deployment_dir=_deployment_dir(elsewhere, name),
        command="foreign checkout",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=theirs)
        apply_composed_catalog(conn, _composed(name), identity=theirs)
        foreign_legacy = _rewrite_as_v1_writer(conn, theirs)
        before = _owner_row(conn, name)

        assert diagnose_catalog_ownership(
            conn, identity=mine,
        )["status"] == "owner_mismatch"
        with pytest.raises(CatalogOwnershipMismatch) as refused:
            ensure_catalog_write_allowed(conn, identity=mine)
        assert refused.value.code == "catalog_ownership_foreign_writer"
        after = _owner_row(conn, name)

    assert after == before
    assert after["owner_fingerprint"] == foreign_legacy
    with pytest.raises(CatalogOwnershipMismatch) as publish_refused:
        Publisher(
            clean_runtime, name,
            catalog_identity=mine, sync_chronos_graph=False,
        )
    assert publish_refused.value.code == "catalog_ownership_foreign_writer"


def test_a_stale_schema_writer_is_refused_at_publish(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Same writer, older catalog: loadable, never publishable (decision A)."""
    name = "catalog-stale-schema"
    stale = _identity(tmp_path, name)
    current = _identity(tmp_path, name, extra=True)
    assert stale.writer_fingerprint == current.writer_fingerprint
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=stale)
        apply_composed_catalog(conn, _composed(name), identity=stale)
        ensure_catalog_write_allowed(conn, identity=current)
        apply_composed_catalog(
            conn, _composed(name, extra=True), identity=current,
        )
    baseline = _published_generation_ids(clean_runtime, name)

    with pytest.raises(CatalogOwnershipMismatch) as refused:
        Publisher(
            clean_runtime, name,
            catalog_identity=stale, sync_chronos_graph=False,
        )
    assert refused.value.code == "catalog_applied_version_incompatible"
    assert _published_generation_ids(clean_runtime, name) == baseline

    published = _publish(clean_runtime, name, current)
    assert published["status"] == "published", published


# --- #2305: a pre-view prune followed by view output -------------------------


def _edge_emitting_view(
    emit: list[dict[str, str] | None],
) -> ViewMaterializer:
    """A native view that writes one valid derived edge during the view stage.

    The write lands after the pre-view consistency pass and before the
    checkpoint, so the publisher runs the post-view pass over it. ``emit[0]``
    selects the edge, or ``None`` for a cycle that emits nothing.
    """
    view_id = f"test.prune_then_view.{uuid.uuid4().hex[:12]}"

    def planner(corpus, view_def):
        return [
            QuoteSpec(
                id="summary-fragment",
                anchor_id="summary",
                display_label="Summary",
            )
        ]

    def assembler(view) -> str:
        return "# Prune then view\n\n" + default_assemble(view)

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=assembler,
            params={"audience": "operator"},
            view_def_hash=stable_hash({"view_id": view_id}),
            display_name="Prune then view",
        )

    def build_corpus(conn, *, generation_id: int) -> dict:
        spec = emit[0]
        if spec is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.edge_facts
                      (edge_id, src, dst, edge_type, attrs,
                       provenance, confidence, source,
                       source_record_id, source_revision,
                       observed_at, dedupe_key, run_id, op)
                    VALUES
                      (%(edge_id)s, %(src)s, %(dst)s, %(edge_type)s, '{}'::jsonb,
                       'derived_deterministic', 1.0, %(source)s,
                       %(record)s::jsonb, %(revision)s::jsonb,
                       now(), %(dedupe_key)s, gen_random_uuid(), 'I')
                    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                    """,
                    {
                        "edge_id": (
                            uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                f"{spec['src']}|{spec['edge_type']}|{spec['dst']}",
                            ).int
                            & ((1 << 63) - 1)
                        ),
                        "src": spec["src"],
                        "dst": spec["dst"],
                        "edge_type": spec["edge_type"],
                        "source": f"derived_artifact:{view_id}",
                        "record": json.dumps({"view_id": view_id}),
                        "revision": json.dumps(
                            {"generation_id": int(generation_id)}
                        ),
                        "dedupe_key": (
                            f"prune-then-view:{view_id}:{spec['src']}:"
                            f"{spec['edge_type']}:{spec['dst']}:{generation_id}"
                        ),
                    },
                )
        return {
            "summary": Anchor.from_content("summary", "prune then view fixture")
        }

    return ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
    )


def _gate_pass_removals(summaries: list[dict[str, Any]], gate_pass: str) -> int:
    """Dangling edges one consistency pass pruned, read from the cycle output."""
    total = 0
    for summary in summaries:
        details = summary.get("details") or {}
        candidates = [details]
        pre_view = details.get("pre_view_pass")
        if isinstance(pre_view, dict):
            candidates.append(pre_view.get("details") or {})
        for candidate in candidates:
            if candidate.get("gate_pass") == gate_pass:
                total += int(candidate.get("pruned_dangling_edge_count", 0) or 0)
    return total


def test_pre_view_prune_then_view_output_stamps_the_live_count(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2305: the pre-view gate prunes, then a view emits, then post-view prunes 0.

    Before the fix the post-view summary replaced the pre-view one, so the
    publisher counted no gate removals, skipped the post-gate full scan and
    stamped a count that still included the pruned edge. The incremental
    attribute receipt then refused the publish with "count delta disagrees
    with checkpoint".
    """
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    # Issue #2941: compare the view-keys scope merge with a rescan.
    monkeypatch.setenv("OKG_DIRTY_SCOPE_MERGE_VERIFY", "strict")
    monkeypatch.setenv("OKG_SCOPE_COUNTS_VERIFY", "strict")
    sfx = uuid.uuid4().hex[:8]
    case1, case2 = f"case:1:{sfx}", f"case:2:{sfx}"
    person1, person2 = f"person:1:{sfx}", f"person:2:{sfx}"
    ghost = f"person:missing:{sfx}"
    gate = TransformConsistencyGate(dangling_policy="prune")
    emit: list[dict[str, str] | None] = [None]
    view = _edge_emitting_view(emit)

    def publisher() -> Publisher:
        return Publisher(
            dsn,
            "default",
            consistency_gates=[gate],
            materializers=[view],
            catalog_identity=bind_test_catalog_writer(dsn, "default"),
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            # The native view writes its own `view` node.
            subtypes=["case", "person", "view"],
            narrowings=[("case", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
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

    first = publisher().cycle()
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
    # The view emits a valid edge, so the post-view pass runs and prunes 0.
    emit[0] = {"src": case2, "dst": person1, "edge_type": "reported_by"}

    second = publisher().cycle()
    assert second["status"] == "published", second

    # Non-vacuity: the shape this test exists for really happened.
    assert second["derived_fact_summary"]["view_applied_count"] >= 1, second
    assert second["timings_ms"].get("chronos_dirty_scope.view_keys_merged") == 1.0
    assert "chronos_dirty_scope.view_keys_mismatch" not in second["timings_ms"]
    gates = second["consistency_gates"]
    assert _gate_pass_removals(gates, "pre_view") == 1, gates
    assert _gate_pass_removals(gates, "post_view") == 0, gates
    assert any(
        (summary.get("details") or {}).get("gate_pass") == "post_view"
        for summary in gates
    ), gates

    assert "count_stamp_post_consistency_gate_full_scan" in second["timings_ms"]
    assert "count_stamp_self_heal_full_scan" not in second["timings_ms"]

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation_id = str(second["generation_id"])
        metadata = dict(
            getattr(store.get_generation(generation_id), "metadata", None) or {}
        )
        session = store.checkout_generation(generation_id)
        live_nodes = int(
            session.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        live_edges = int(
            session.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
        dangling_live = session.query(
            "SELECT 1 FROM okg.graph_edges WHERE dst = %(dst)s",
            {"dst": ghost},
        )

    assert not list(dangling_live)
    assert metadata["count_stamp_cadence"] == 1
    assert (int(metadata["node_count"]), int(metadata["edge_count"])) == (
        live_nodes,
        live_edges,
    )
    # One valid source edge plus one view edge; the dangling edge is gone.
    assert live_edges == int(first["edge_count"]) + 2
    # Written this cycle: the valid edge, the dangling edge and the view edge.
    # Visible at the checkpoint: all but the pruned one. Before #2305 the
    # receipt missed the pre-view prune and reported 3 visible.
    receipt = metadata["projection_deltas"]
    assert receipt["edges_projected_net"] == 3
    assert receipt["consistency_gate_mutations"]["dangling_edges_pruned"] == 1
    assert receipt["consistency_gate_mutations"]["edges_removed"] == 1
    assert receipt["edges_checkpoint_visible_net"] == 2
    assert receipt["edges_checkpoint_visible_net"] == (
        live_edges - int(first["edge_count"])
    )
    integrity = second["attribute_effective_view_refresh"]["integrity_receipt"]
    assert integrity.get("advanced") is True, integrity


def test_write_after_the_counts_on_a_native_view_cycle_is_never_cut_uncounted(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An outside write after the counts, on a cycle whose view writes rows.

    The view stage clears the cached scope and later passes re-read it, but
    the counts are only adjusted for the view's own keys. Before the fix a
    re-read moved the "last read" fingerprint forward, so a write made
    after the counts passed the checkpoint check uncounted and only the
    attribute receipt refused it. Now the publisher's view writes run under
    the branch write lock and first check the counted fingerprint: the
    attempt is refused (`before_view_writes`), the retry counts the row, and
    the stamped count equals the visible count.
    """
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    # Issue #2941: compare the view-keys scope merge with a rescan.
    monkeypatch.setenv("OKG_DIRTY_SCOPE_MERGE_VERIFY", "strict")
    monkeypatch.setenv("OKG_SCOPE_COUNTS_VERIFY", "strict")
    sfx = uuid.uuid4().hex[:8]
    case1, case2 = f"case:1:{sfx}", f"case:2:{sfx}"
    person1, person2 = f"person:1:{sfx}", f"person:2:{sfx}"
    outsider = f"person:outside:{sfx}"
    gate = TransformConsistencyGate(dangling_policy="prune")
    emit: list[dict[str, str] | None] = [None]
    view = _edge_emitting_view(emit)
    real_build_corpus = view.build_corpus
    outside_writes: list[str] = []

    def build_corpus_then_outside_write(conn, *, generation_id: int) -> dict:
        corpus = real_build_corpus(conn, generation_id=generation_id)
        if emit[0] is not None and not outside_writes:
            # Another process writes the branch head during the view stage:
            # after the cycle's counts, before its own view writes.
            with psycopg.connect(dsn, autocommit=True) as other:
                ChronosGraphStore.from_connection(other).apply_facts(
                    [NodeFact(outsider, "person", {}, {"id": outsider}, {"v": 1})],
                    source="default",
                    strict_endpoints=True,
                )
            outside_writes.append(outsider)
        return corpus

    monkeypatch.setattr(view, "build_corpus", build_corpus_then_outside_write)

    def publisher() -> Publisher:
        return Publisher(
            dsn,
            "default",
            consistency_gates=[gate],
            materializers=[view],
            catalog_identity=bind_test_catalog_writer(dsn, "default"),
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person", "view"],
            narrowings=[("case", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(case1, "case", {}, {"id": case1}, {"v": 1}),
                NodeFact(person1, "person", {}, {"id": person1}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    first = publisher().cycle()
    assert first["status"] == "published", first

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(case2, "case", {}, {"id": case2}, {"v": 1}),
                NodeFact(person2, "person", {}, {"id": person2}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    emit[0] = {"src": case2, "dst": person1, "edge_type": "reported_by"}

    second = publisher().cycle()
    assert outside_writes == [outsider]
    assert second["status"] == "published", second
    assert second["write_race_refusals"] == [
        {"attempt": 1, "stage": "before_view_writes"},
    ]
    assert second["derived_fact_summary"]["view_applied_count"] >= 1, second
    assert second["timings_ms"].get("chronos_dirty_scope.view_keys_merged") == 1.0
    assert "chronos_dirty_scope.view_keys_mismatch" not in second["timings_ms"]

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation_id = str(second["generation_id"])
        metadata = dict(
            getattr(store.get_generation(generation_id), "metadata", None) or {}
        )
        session = store.checkout_generation(generation_id)
        visible = {
            row["node_id"]
            for row in session.query("SELECT node_id FROM okg.graph_nodes")
        }
        live_edges = int(
            session.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    assert outsider in visible
    assert (int(metadata["node_count"]), int(metadata["edge_count"])) == (
        len(visible),
        live_edges,
    )


@pytest.mark.parametrize("with_gate", [True, False], ids=["gate", "no_gate"])
def test_write_after_the_view_writes_is_refused_before_the_protected_writes(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
    with_gate: bool,
) -> None:
    """An outside write after the view-write lock releases, before the re-read.

    The next read of the changed-row scope sees the row, but the counts do
    not include it, so that re-read must not move the counted fingerprint.
    The incremental cycle then takes the branch write lock before its first
    protected attribute write and refuses there (`before_effective_view`):
    nothing protected was written, so the retry stays incremental (the
    count-stamp cadence advances; no full re-anchor) and stamps a node count
    equal to the visible count. Before the fix the refusal came at the
    checkpoint cut, after the materialize, and the retry was a full rebuild.
    """
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    sfx = uuid.uuid4().hex[:8]
    case1, case2 = f"case:1:{sfx}", f"case:2:{sfx}"
    person1, person2 = f"person:1:{sfx}", f"person:2:{sfx}"
    outsider = f"person:outside:{sfx}"
    emit: list[dict[str, str] | None] = [None]
    view = _edge_emitting_view(emit)
    gates = [TransformConsistencyGate(dangling_policy="prune")] if with_gate else []
    outside_writes: list[str] = []
    real_unlock = ChronosGraphStore._unlock_branch_write_session

    def unlock_then_outside_write(self, branch: str) -> None:
        real_unlock(self, branch)
        if emit[0] is not None and not outside_writes:
            # The first release on the raced cycle is the view-write lock.
            with psycopg.connect(dsn, autocommit=True) as other:
                ChronosGraphStore.from_connection(other).apply_facts(
                    [NodeFact(outsider, "person", {}, {"id": outsider}, {"v": 1})],
                    source="default",
                    strict_endpoints=True,
                )
            outside_writes.append(outsider)

    monkeypatch.setattr(
        ChronosGraphStore,
        "_unlock_branch_write_session",
        unlock_then_outside_write,
    )

    def publisher() -> Publisher:
        return Publisher(
            dsn,
            "default",
            consistency_gates=gates,
            materializers=[view],
            catalog_identity=bind_test_catalog_writer(dsn, "default"),
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person", "view"],
            narrowings=[("case", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(case1, "case", {}, {"id": case1}, {"v": 1}),
                NodeFact(person1, "person", {}, {"id": person1}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    first = publisher().cycle()
    assert first["status"] == "published", first

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(case2, "case", {}, {"id": case2}, {"v": 1}),
                NodeFact(person2, "person", {}, {"id": person2}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    emit[0] = {"src": case2, "dst": person1, "edge_type": "reported_by"}

    second = publisher().cycle()
    assert outside_writes == [outsider]
    assert second["status"] == "published", second
    assert second["write_race_refusals"] == [
        {"attempt": 1, "stage": "before_effective_view"},
    ]

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        first_metadata = dict(
            getattr(store.get_generation(str(first["generation_id"])), "metadata", None)
            or {}
        )
        generation_id = str(second["generation_id"])
        metadata = dict(
            getattr(store.get_generation(generation_id), "metadata", None) or {}
        )
        session = store.checkout_generation(generation_id)
        visible = {
            row["node_id"]
            for row in session.query("SELECT node_id FROM okg.graph_nodes")
        }
        live_edges = int(
            session.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    # The retry was incremental: a full-proof cycle resets the cadence to 0.
    assert int(metadata["count_stamp_cadence"]) == (
        int(first_metadata["count_stamp_cadence"]) + 1
    ), metadata
    assert outsider in visible
    assert (int(metadata["node_count"]), int(metadata["edge_count"])) == (
        len(visible),
        live_edges,
    )


def test_write_after_an_incremental_materialize_waits_for_the_commit(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
) -> None:
    """A write after the protected writes began waits; nothing is refused.

    The incremental cycle holds the branch write lock from before the
    materialize through the commit, so a writer that arrives in that span
    blocks, lands after the commit outside this generation, and the next
    cycle counts it. Before the fix it landed at once and the cut refused
    the attempt, making the retry a full rebuild.
    """
    import time

    import okg.substrate.attributes as attributes

    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    sfx = uuid.uuid4().hex[:8]
    late = f"person:late:{sfx}"
    writer: dict[str, Any] = {"error": None, "blocked": False}
    threads: list[threading.Thread] = []
    armed = [False]

    def apply(facts: list[NodeFact]) -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                facts, source="default", strict_endpoints=True,
            )

    def outside_write() -> None:
        try:
            apply([NodeFact(late, "person", {}, {"id": late}, {"v": 1})])
        except BaseException as exc:  # noqa: BLE001 - asserted below
            writer["error"] = exc

    real_materialize = attributes.materialize_effective_attribute_view_for_generation

    def materialize_then_write(*args, **kwargs):
        result = real_materialize(*args, **kwargs)
        if armed[0] and not threads:
            thread = threading.Thread(target=outside_write, daemon=True)
            threads.append(thread)
            thread.start()
            deadline = time.monotonic() + 30.0
            with psycopg.connect(dsn, autocommit=True) as probe:
                while time.monotonic() < deadline and thread.is_alive():
                    (waiting,) = probe.execute(
                        "SELECT count(*) FROM pg_locks"
                        " WHERE locktype = 'advisory' AND NOT granted"
                    ).fetchone()
                    if waiting:
                        writer["blocked"] = True
                        break
                    time.sleep(0.05)
        return result

    monkeypatch.setattr(
        attributes,
        "materialize_effective_attribute_view_for_generation",
        materialize_then_write,
    )

    def publisher() -> Publisher:
        return Publisher(
            dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
        )

    def metadata_and_visible(generation_id: str) -> tuple[dict, set[str]]:
        with psycopg.connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            metadata = dict(
                getattr(store.get_generation(generation_id), "metadata", None) or {}
            )
            visible = {
                row["node_id"]
                for row in store.checkout_generation(generation_id).query(
                    "SELECT node_id FROM okg.graph_nodes"
                )
            }
        return metadata, visible

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"])
    apply([NodeFact(f"person:1:{sfx}", "person", {}, {"id": 1}, {"v": 1})])
    first = publisher().cycle()
    assert first["status"] == "published", first
    apply([NodeFact(f"person:2:{sfx}", "person", {}, {"id": 2}, {"v": 1})])
    armed[0] = True

    second = publisher().cycle()
    assert len(threads) == 1
    threads[0].join(60)
    assert not threads[0].is_alive()
    assert writer["error"] is None, writer["error"]
    assert writer["blocked"] is True
    assert second["status"] == "published", second
    assert second["write_race_refusals"] == []
    first_metadata, _ = metadata_and_visible(str(first["generation_id"]))
    metadata, visible = metadata_and_visible(str(second["generation_id"]))
    assert late not in visible
    assert int(metadata["node_count"]) == len(visible)
    assert int(metadata["count_stamp_cadence"]) == (
        int(first_metadata["count_stamp_cadence"]) + 1
    ), metadata

    third = publisher().cycle()
    assert third["status"] == "published", third
    assert third["write_race_refusals"] == []
    metadata, visible = metadata_and_visible(str(third["generation_id"]))
    assert late in visible
    assert int(metadata["node_count"]) == len(visible)


_BRANCH_LOCK_SQL = (
    "SELECT {fn}(hashtext('okg.chronos_graph'), hashtext('default'))"
)


def _branch_write_lock_free(dsn: str, *, wait_s: float = 0.0) -> bool:
    """Whether another session can take the default branch's write lock.

    Polls with a try-lock until ``wait_s`` elapses, so a leaked lock fails
    the assertion instead of hanging the test.
    """
    import time

    deadline = time.monotonic() + wait_s
    with psycopg.connect(dsn, autocommit=True) as other:
        while True:
            (taken,) = other.execute(
                _BRANCH_LOCK_SQL.format(fn="pg_try_advisory_lock")
            ).fetchone()
            if taken:
                other.execute(_BRANCH_LOCK_SQL.format(fn="pg_advisory_unlock"))
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)


def _branch_lock_holders(dsn: str) -> list[tuple]:
    with psycopg.connect(dsn, autocommit=True) as other:
        return other.execute(
            "SELECT pid, granted, mode FROM pg_locks"
            " WHERE locktype = 'advisory' AND objsubid = 2"
            "   AND classid::bigint"
            "       = (hashtext('okg.chronos_graph')::bigint & 4294967295)"
        ).fetchall()


@pytest.fixture
def branch_lock_watchdog(clean_runtime: str):
    """Cancel any session stuck over 30 s on the branch write lock.

    A lock leaked by the publisher makes its own next attempt, or a later
    cycle on another pooled connection, wait forever. The watchdog cancels
    that wait so the test fails with a named cause instead of hanging.
    """
    stop = threading.Event()
    cancelled: list[int] = []

    def watch() -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            while not stop.wait(0.5):
                rows = conn.execute(
                    "SELECT l.pid FROM pg_locks AS l"
                    "  JOIN pg_stat_activity AS a USING (pid)"
                    " WHERE l.locktype = 'advisory' AND NOT l.granted"
                    "   AND l.objsubid = 2"
                    "   AND l.classid::bigint"
                    "       = (hashtext('okg.chronos_graph')::bigint & 4294967295)"
                    "   AND a.state_change < now() - interval '30 seconds'"
                ).fetchall()
                for (pid,) in rows:
                    conn.execute("SELECT pg_cancel_backend(%s)", (pid,))
                    cancelled.append(int(pid))

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    yield cancelled
    stop.set()
    thread.join(10)
    assert not cancelled, (
        "a session waited over 30 s for the branch write lock (a leaked "
        f"lock); cancelled backends {cancelled}"
    )


def _write_nodes_bounded(dsn: str, node_ids: list[str]) -> None:
    """Write person nodes from another session, waiting at most 20 s.

    A branch write lock leaked by the publisher would otherwise block this
    writer forever; the bounded wait turns that into an assertion.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET lock_timeout = '20s'")
        try:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [
                    NodeFact(nid, "person", {}, {"id": nid}, {"v": 1})
                    for nid in node_ids
                ],
                source="default",
                strict_endpoints=True,
            )
        except psycopg.errors.LockNotAvailable as exc:
            raise AssertionError(
                "the branch write lock is still held: "
                f"{_branch_lock_holders(dsn)}"
            ) from exc


def _publisher_for(dsn: str) -> Publisher:
    return Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    )


def _published_baseline_then_one_write(dsn: str) -> str:
    """Publish one node, then write another; the next cycle is incremental."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"])
    _write_nodes_bounded(dsn, [f"person:1:{sfx}"])
    first = _publisher_for(dsn).cycle()
    assert first["status"] == "published", first
    _write_nodes_bounded(dsn, [f"person:2:{sfx}"])
    return sfx


def _assert_stamped_count_is_visible(dsn: str, generation_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        metadata = dict(
            getattr(store.get_generation(generation_id), "metadata", None) or {}
        )
        visible = store.checkout_generation(generation_id).query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
        )[0]["n"]
    assert int(metadata["node_count"]) == int(visible), metadata


def test_protected_write_lock_is_released_before_the_post_commit_steps(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
) -> None:
    """The lock taken before the protected writes ends at the commit.

    The post-commit steps (integrity receipt, retry and queue
    acknowledgements, source-retirement binding) need no graph fence, so
    writers must not wait for them.
    """
    import okg.substrate.attributes as attributes
    import okg.substrate.source_retirement as source_retirement

    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    _published_baseline_then_one_write(dsn)
    seen: dict[str, bool] = {}
    real_materialize = attributes.materialize_effective_attribute_view_for_generation
    real_bind = source_retirement.bind_source_retirements_to_published_generation

    def materialize_seeing_lock(*args, **kwargs):
        seen["held_at_materialize"] = not _branch_write_lock_free(dsn)
        return real_materialize(*args, **kwargs)

    def bind_seeing_lock(*args, **kwargs):
        seen["free_after_commit"] = _branch_write_lock_free(dsn)
        return real_bind(*args, **kwargs)

    monkeypatch.setattr(
        attributes,
        "materialize_effective_attribute_view_for_generation",
        materialize_seeing_lock,
    )
    monkeypatch.setattr(
        source_retirement,
        "bind_source_retirements_to_published_generation",
        bind_seeing_lock,
    )
    out = _publisher_for(dsn).cycle()
    assert out["status"] == "published", out
    assert seen == {"held_at_materialize": True, "free_after_commit": True}, (
        seen, _branch_lock_holders(dsn),
    )


@pytest.mark.parametrize("kind", ["python_error", "sql_error"])
def test_failure_after_the_lock_point_releases_the_branch_lock(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
    kind: str,
) -> None:
    import okg.substrate.attributes as attributes

    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    _published_baseline_then_one_write(dsn)
    seen: dict[str, bool] = {}

    def fail(conn, **_kwargs):
        seen["held"] = not _branch_write_lock_free(dsn)
        if kind == "sql_error":
            conn.execute("SELECT 1/0")
        raise RuntimeError("failure after the lock point")

    monkeypatch.setattr(
        attributes, "materialize_effective_attribute_view_for_generation", fail,
    )
    with pytest.raises(Exception):
        _publisher_for(dsn).cycle()
    assert seen == {"held": True}
    assert _branch_write_lock_free(dsn, wait_s=2.0), _branch_lock_holders(dsn)


def test_failure_inside_the_publication_transaction_releases_the_branch_lock(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
) -> None:
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    _published_baseline_then_one_write(dsn)
    real_checkpoint = ChronosGraphStore.checkpoint_generation

    def checkpoint_then_fail(self, *args, **kwargs):
        out = real_checkpoint(self, *args, **kwargs)
        self.context.db.execute("SELECT 1/0", [])
        return out

    monkeypatch.setattr(
        ChronosGraphStore, "checkpoint_generation", checkpoint_then_fail,
    )
    with pytest.raises(Exception):
        _publisher_for(dsn).cycle()
    assert _branch_write_lock_free(dsn, wait_s=2.0), _branch_lock_holders(dsn)


def test_publication_commit_failure_releases_the_branch_lock(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
) -> None:
    """A deferred constraint trigger fails the publication COMMIT itself."""
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    _published_baseline_then_one_write(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "CREATE OR REPLACE FUNCTION public.fail_publication_commit()"
            " RETURNS trigger LANGUAGE plpgsql AS"
            " $$ BEGIN RAISE EXCEPTION 'publication commit refused'; END $$"
        )
        table = conn.execute(
            "SELECT to_regclass('_chronos_branch_interval_checkpoints')::text"
        ).fetchone()[0]
        assert table
        conn.execute(
            f"CREATE CONSTRAINT TRIGGER fail_publication_commit AFTER INSERT"
            f" ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW"
            " EXECUTE FUNCTION public.fail_publication_commit()"
        )
    try:
        with pytest.raises(Exception) as raised:
            _publisher_for(dsn).cycle()
        chain = f"{raised.value!r} {raised.value.__cause__!r} {raised.value.__context__!r}"
        assert "publication commit refused" in chain
        assert _branch_write_lock_free(dsn, wait_s=2.0), _branch_lock_holders(dsn)
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f"DROP TRIGGER IF EXISTS fail_publication_commit ON {table}")
    again = _publisher_for(dsn).cycle()
    assert again["status"] == "published", again


def test_races_at_the_lock_point_exhaust_the_retries_and_release_the_lock(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
) -> None:
    """A writer racing every incremental attempt: 3 refusals, then a clean run."""
    import okg.substrate.publisher.publisher as publisher_module

    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    monkeypatch.setattr(
        publisher_module, "_write_race_backoff_seconds", lambda _attempt: 0.0,
    )
    sfx = _published_baseline_then_one_write(dsn)
    real_fence = Publisher._chronos_fence_protected_writes
    races = [0]

    def race_then_fence(self, store, conn, signature_counted):
        races[0] += 1
        _write_nodes_bounded(dsn, [f"person:race{races[0]}:{sfx}"])
        return real_fence(self, store, conn, signature_counted)

    monkeypatch.setattr(
        Publisher, "_chronos_fence_protected_writes", race_then_fence,
    )
    with pytest.raises(SubstrateError) as raised:
        _publisher_for(dsn).cycle()
    assert raised.value.code == "graph_written_during_publish"
    assert [r["stage"] for r in raised.value.data["write_race_refusals"]] == [
        "before_effective_view",
    ] * 3
    assert races[0] == 3
    assert _branch_write_lock_free(dsn, wait_s=2.0), _branch_lock_holders(dsn)

    monkeypatch.setattr(Publisher, "_chronos_fence_protected_writes", real_fence)
    out = _publisher_for(dsn).cycle()
    assert out["status"] == "published", out
    assert out["write_race_refusals"] == []
    _assert_stamped_count_is_visible(dsn, str(out["generation_id"]))


@pytest.mark.parametrize("fail", [False, True], ids=["published", "failed"])
def test_caller_held_branch_lock_is_reentered_not_released(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
    fail: bool,
) -> None:
    """The inline publish holds the lock on the connection it passes in."""
    import okg.substrate.attributes as attributes

    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    _published_baseline_then_one_write(dsn)
    if fail:
        def fail_materialize(conn, **_kwargs):
            conn.execute("SELECT 1/0")

        monkeypatch.setattr(
            attributes,
            "materialize_effective_attribute_view_for_generation",
            fail_materialize,
        )
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(_BRANCH_LOCK_SQL.format(fn="pg_advisory_lock"))
        if fail:
            with pytest.raises(Exception):
                _publisher_for(dsn).cycle(conn=conn)
        else:
            out = _publisher_for(dsn).cycle(conn=conn)
            assert out["status"] == "published", out
        # The caller's level survives the cycle ...
        assert not _branch_write_lock_free(dsn)
        (released,) = conn.execute(
            _BRANCH_LOCK_SQL.format(fn="pg_advisory_unlock")
        ).fetchone()
        assert released is True
        # ... and the cycle left no level of its own behind.
        assert _branch_write_lock_free(dsn), _branch_lock_holders(dsn)


def test_writer_waiting_on_the_lock_proceeds_when_the_cycle_fails(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
) -> None:
    import time

    import okg.substrate.attributes as attributes

    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    sfx = _published_baseline_then_one_write(dsn)
    real_materialize = attributes.materialize_effective_attribute_view_for_generation
    writer: dict[str, Any] = {}

    def write() -> None:
        try:
            _write_nodes_bounded(dsn, [f"person:waiting:{sfx}"])
            writer["done"] = True
        except BaseException as exc:  # noqa: BLE001 - asserted below
            writer["error"] = exc

    thread = threading.Thread(target=write, daemon=True)

    def materialize_then_fail(conn, **kwargs):
        real_materialize(conn, **kwargs)
        thread.start()
        time.sleep(1.0)
        writer["waited"] = "done" not in writer
        conn.execute("SELECT 1/0")

    monkeypatch.setattr(
        attributes,
        "materialize_effective_attribute_view_for_generation",
        materialize_then_fail,
    )
    with pytest.raises(Exception):
        _publisher_for(dsn).cycle()
    thread.join(30)
    assert writer.get("error") is None, writer.get("error")
    assert writer.get("waited") is True
    assert writer.get("done") is True, _branch_lock_holders(dsn)


@pytest.mark.parametrize("sql_error", [False, True], ids=["python", "sql"])
def test_caller_verified_failure_releases_the_branch_lock(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    branch_lock_watchdog: list[int],
    sql_error: bool,
) -> None:
    """A caller-owned transaction whose verifier refuses an incremental cycle."""
    dsn = clean_runtime
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    _published_baseline_then_one_write(dsn)
    seen: dict[str, bool] = {}

    def verifier(conn, generation_id):
        seen["held"] = not _branch_write_lock_free(dsn)
        if sql_error:
            conn.execute("SELECT 1/0")
        raise RuntimeError("verifier refuses")

    with psycopg.connect(dsn, autocommit=False) as conn:
        with pytest.raises(Exception):
            _publisher_for(dsn).cycle(conn=conn, atomic_precommit_verifier=verifier)
        try:
            conn.rollback()
        except psycopg.Error:
            pass
        assert seen == {"held": True}
        assert _branch_write_lock_free(dsn, wait_s=2.0), _branch_lock_holders(dsn)


def test_gate_mutation_counts_add_the_superseded_pre_view_pass() -> None:
    """The final receipt counts removals from both consistency passes (#2305).

    A post-view summary replaces its pre-view summary and nests it under
    ``details["pre_view_pass"]``. Read without ``include_pre_view_pass`` (as
    each pass reads its own summaries), only the top-level pass counts; with
    it, both passes add up.
    """
    merged = [{
        "gate_name": "transform_consistency",
        "details": {
            "pruned_dangling_edge_count": 2,
            "orphan_retracted_derived_edge_count": 0,
            "pre_view_pass": {
                "gate_name": "transform_consistency",
                "details": {
                    "pruned_dangling_edge_count": 3,
                    "derived_node_cleanup": {
                        "retracted_node_count": 1,
                        "retracted_incident_edge_count": 1,
                    },
                },
            },
        },
    }]

    top_level_only = Publisher._consistency_gate_edge_mutation_counts(merged)
    assert top_level_only["dangling_edges_pruned"] == 2
    assert top_level_only["edges_removed"] == 2
    assert top_level_only["nodes_removed"] == 0

    both = Publisher._consistency_gate_edge_mutation_counts(
        merged, include_pre_view_pass=True,
    )
    assert both["dangling_edges_pruned"] == 5
    assert both["unsupported_derived_incident_edges_retracted"] == 1
    assert both["edges_removed"] == 6
    assert both["nodes_removed"] == 1

    # A pass that removed nothing and nests no earlier pass counts nothing.
    assert Publisher._consistency_gate_edge_mutation_counts(
        [{"gate_name": "g", "details": {}}], include_pre_view_pass=True,
    )["edges_removed"] == 0


def test_first_publish_prune_counts_the_pruned_branch_once(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2305 review: a first publish that prunes counts the post-gate branch once.

    The pre-view recount and the final count stamp read the same checkout, so
    the second must reuse the first. A full count can take an hour at
    okg-workspace scale; the spy counts how many times one runs.
    """
    sfx = uuid.uuid4().hex[:8]
    ticket, person, ghost = f"t:{sfx}", f"p:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(ticket, "ticket", {}, {"id": ticket}, {"v": 1}),
                NodeFact(person, "person", {}, {"id": person}, {"v": 1}),
                EdgeFact(
                    ticket,
                    person,
                    "reported_by",
                    source_record_id={"id": "valid"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    ticket,
                    ghost,
                    "reported_by",
                    source_record_id={"id": "dangling"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=False,
        )

    scans: list[str] = []
    original = Publisher._chronos_current_graph_counts

    def _recording(branch: object) -> tuple[int, int]:
        scans.append("scan")
        return original(branch)

    monkeypatch.setattr(
        Publisher, "_chronos_current_graph_counts", staticmethod(_recording),
    )
    pub = Publisher(
        clean_runtime,
        "default",
        consistency_gates=[TransformConsistencyGate(dangling_policy="prune")],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    out = pub.cycle()
    assert out["status"] == "published", out
    assert out["consistency_gates"][0]["details"][
        "pruned_dangling_edge_count"
    ] == 1, out["consistency_gates"]
    assert "delta_counts_after_pre_view_gate" in out["timings_ms"]
    assert out["edge_count"] == 1, out
    # Base 9c9de0a37 took 3 full counts here; before this fix #2305 took 4.
    assert len(scans) == 3, scans


# ---------------------------------------------------------------------------
# Issue #3003: a command-line source run holds the deployment's source-sync
# signal (the admission-intent advisory lock) from before its first chunk
# write until after its own publish. A second run waits a bounded time, then
# stops with source_sync_already_running; a publish from inside a sync waits
# for another sync's signal but never for its own; a killed holder releases
# the signal with its session. No-database cases live in
# tests/substrate/runtime/test_source_sync_run_guard.py.
# ---------------------------------------------------------------------------

import contextlib  # noqa: E402
import contextvars  # noqa: E402
import os  # noqa: E402
import signal  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import textwrap  # noqa: E402
import types  # noqa: E402
import time  # noqa: E402

from okg.substrate.errors import (  # noqa: E402
    PublisherLeaseHeld,
    SourceSyncAlreadyRunning,
)
from okg.substrate.ingest import service  # noqa: E402
from okg.substrate.runtime import source_sync_priority as priority  # noqa: E402
from okg.substrate.runtime.source_sync_priority import (  # noqa: E402
    source_sync_admission_intent_workflow_id_on_connection,
    source_sync_run_guard,
)


def _run_guard_deployment() -> str:
    return f"test-run-guard-{uuid.uuid4().hex[:8]}"


def _run_guard_deployment_dir(tmp_path: Path, name: str, *, runtime: bool = False) -> Path:
    directory = tmp_path / name
    directory.mkdir()
    runtime_block = "runtime:\n  enabled: true\n" if runtime else ""
    (directory / "deployment.yaml").write_text(
        f"name: {name}\n{runtime_block}", encoding="utf-8",
    )
    return directory


class _Holder:
    """Hold the run guard on a background thread until released."""

    def __init__(self, dsn: str, deployment: str) -> None:
        self.held = threading.Event()
        self.release = threading.Event()
        self.pid: int | None = None
        self.error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run, args=(dsn, deployment), daemon=True,
        )

    def _run(self, dsn: str, deployment: str) -> None:
        try:
            with source_sync_run_guard(
                dsn=dsn, deployment=deployment, wait_seconds=0,
            ) as pid:
                self.pid = pid
                self.held.set()
                self.release.wait(timeout=30)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the test
            self.error = exc
            self.held.set()

    def __enter__(self) -> "_Holder":
        self._thread.start()
        assert self.held.wait(timeout=10)
        assert self.error is None, self.error
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release.set()
        self._thread.join(timeout=10)


def _signal_free(dsn: str, deployment: str) -> bool:
    classid, objid = priority.source_sync_admission_intent_advisory_keys(
        deployment,
    )
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (classid, objid))
        acquired = bool(cur.fetchone()[0])
        if acquired:
            cur.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
    return acquired


# --- two concurrent ingests serialize -------------------------------------


@pytest.mark.postgres
def test_two_concurrent_ingests_run_one_after_the_other(
    okg_migrated: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two real ``run_ingest`` calls on one deployment never overlap: the
    second starts its source work only after the first has finished,
    including its publish."""
    deployment = _run_guard_deployment()
    dep_dir = _run_guard_deployment_dir(tmp_path, deployment)
    spans: dict[str, tuple[float, float]] = {}
    first_inside = threading.Event()

    def fake_inner(options: service.IngestOptions) -> service.IngestRunResult:
        label = str(options.owner_id)
        start = time.monotonic()
        first_inside.set()
        # Stands in for chunk writes plus the run's own publish.
        time.sleep(1.5)
        spans[label] = (start, time.monotonic())
        return service.IngestRunResult(report={}, any_failed=False)

    monkeypatch.setattr(service, "_run_ingest_inner", fake_inner)
    errors: list[BaseException] = []

    def run(label: str) -> None:
        try:
            service.run_ingest(service.IngestOptions(
                dsn=okg_migrated,
                deployment=deployment,
                deployment_dir=dep_dir,
                owner_id=label,
                sync_wait_seconds=30,
            ))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    first = threading.Thread(target=run, args=("A",))
    first.start()
    assert first_inside.wait(timeout=10)
    second_called = time.monotonic()
    second = threading.Thread(target=run, args=("B",))
    second.start()
    first.join(timeout=30)
    second.join(timeout=30)

    assert not errors, errors
    a_start, a_end = spans["A"]
    b_start, _b_end = spans["B"]
    assert b_start >= a_end, (spans, "second ingest overlapped the first")
    assert b_start - second_called >= 1.0
    assert _signal_free(okg_migrated, deployment)


@pytest.mark.postgres
def test_run_ingest_times_out_with_named_error_before_any_source_work(
    okg_migrated: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _run_guard_deployment()
    dep_dir = _run_guard_deployment_dir(tmp_path, deployment)
    calls: list[str] = []
    monkeypatch.setattr(
        service, "_run_ingest_inner", lambda options: calls.append("ran"),
    )
    with _Holder(okg_migrated, deployment):
        with pytest.raises(SourceSyncAlreadyRunning) as raised:
            service.run_ingest(service.IngestOptions(
                dsn=okg_migrated,
                deployment=deployment,
                deployment_dir=dep_dir,
                sync_wait_seconds=0.5,
            ))
    assert calls == []
    assert raised.value.code == "source_sync_already_running"
    assert "another source sync is running" in str(raised.value)


# --- the bounded wait -----------------------------------------------------


@pytest.mark.postgres
def test_bounded_wait_times_out_with_named_error(okg_migrated: str) -> None:
    deployment = _run_guard_deployment()
    with _Holder(okg_migrated, deployment) as holder:
        started = time.monotonic()
        with pytest.raises(SourceSyncAlreadyRunning) as raised:
            with source_sync_run_guard(
                dsn=okg_migrated, deployment=deployment, wait_seconds=1.0,
            ):
                pytest.fail("entered the guard while another run held it")
        elapsed = time.monotonic() - started
    assert 0.9 <= elapsed < 5.0, elapsed
    error = raised.value
    assert error.code == "source_sync_already_running"
    assert error.data["deployment"] == deployment
    assert error.data["wait_seconds"] == 1.0
    assert error.data["blocking_id"] == SOURCE_SYNC_ADMISSION_INTENT_ID
    assert error.data["holder_pids"] == [holder.pid]
    assert "--sync-wait-seconds" in (error.fix_hint or "")


@pytest.mark.postgres
def test_waiter_proceeds_once_the_holder_finishes(okg_migrated: str) -> None:
    deployment = _run_guard_deployment()
    holder = _Holder(okg_migrated, deployment)
    with holder:
        threading.Timer(1.0, holder.release.set).start()
        started = time.monotonic()
        with source_sync_run_guard(
            dsn=okg_migrated, deployment=deployment, wait_seconds=10,
        ) as pid:
            waited = time.monotonic() - started
            assert pid != holder.pid
    assert 0.8 <= waited < 5.0, waited
    assert _signal_free(okg_migrated, deployment)


# --- the runtime worker's queue -------------------------------------------


@pytest.mark.postgres
def test_run_waits_for_a_queued_or_running_worker_sync(
    okg_migrated: str,
) -> None:
    """A worker sync that is queued or running blocks a command-line run
    even though the worker holds no lock across its run; the run releases
    the signal while it waits, so the worker's own enqueues are not stuck."""
    deployment = _run_guard_deployment()
    answers = iter(["wf-running", "wf-running", None])
    observed_free: list[bool] = []

    def probe() -> str | None:
        answer = next(answers)
        return answer

    real_sleep = time.sleep

    def sleeping(seconds: float) -> None:
        observed_free.append(_signal_free(okg_migrated, deployment))
        real_sleep(seconds)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(priority.time, "sleep", sleeping)
        with source_sync_run_guard(
            dsn=okg_migrated,
            deployment=deployment,
            wait_seconds=10,
            runtime_probe=probe,
            poll_seconds=0.05,
        ):
            assert not _signal_free(okg_migrated, deployment)
    assert observed_free == [True, True]

    with pytest.raises(SourceSyncAlreadyRunning) as raised:
        with source_sync_run_guard(
            dsn=okg_migrated,
            deployment=deployment,
            wait_seconds=0.3,
            runtime_probe=lambda: "wf-stuck",
            poll_seconds=0.05,
        ):
            pytest.fail("entered while the worker queue was busy")
    assert raised.value.data["blocking_id"] == "wf-stuck"
    assert raised.value.data["holder_pids"] == []
    # A stranded worker row names itself and the two ways out; it does not
    # tell the operator to wait for a run that will never finish.
    assert "wf-stuck" in str(raised.value)
    assert "okg runtime worker" in raised.value.fix_hint
    assert f"okg runtime cancel-workflow wf-stuck --deployment {deployment}" in (
        raised.value.fix_hint
    )
    assert _signal_free(okg_migrated, deployment)


# --- operator enqueues and the worker queue (review round 2) ---------------


def _stub_dbos_enqueue(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Stand in for DBOS in client_enqueue_source_sync; record enqueue times."""
    from okg.substrate.runtime import admin

    enqueued: list[float] = []

    class _Client:
        def enqueue(self, *_args: object, **_kwargs: object) -> object:
            enqueued.append(time.monotonic())
            return types.SimpleNamespace(workflow_id="wf-queued")

    monkeypatch.setattr(admin, "_build_client", lambda **_kwargs: _Client())
    monkeypatch.setattr(admin, "latest_registered_app_version", lambda _dsn: None)
    monkeypatch.setattr(
        admin, "promotion_enqueue_permit", lambda **_kwargs: contextlib.nullcontext(),
    )
    return enqueued


@pytest.mark.postgres
def test_operator_enqueue_waits_for_a_command_line_run(
    okg_migrated: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plain `okg ingest` (the worker path) cannot start a sync beside a
    command-line run: its enqueue waits for the run's signal, bounded."""
    from okg.substrate.runtime.admin import client_enqueue_source_sync

    enqueued = _stub_dbos_enqueue(monkeypatch)
    deployment = _run_guard_deployment()
    holder = _Holder(okg_migrated, deployment)
    with holder:
        started = time.monotonic()
        with pytest.raises(SourceSyncAlreadyRunning) as raised:
            client_enqueue_source_sync(
                deployment=deployment, dsn=okg_migrated,
                wait_for_result=False, sync_wait_seconds=0.5,
            )
        refused_after = time.monotonic() - started
        assert enqueued == []
        assert raised.value.data["holder_pids"] == [holder.pid]
        assert "could not enqueue" in str(raised.value)

        released_at: list[float] = []

        def release() -> None:
            released_at.append(time.monotonic())
            holder.release.set()

        threading.Timer(1.0, release).start()
        result = client_enqueue_source_sync(
            deployment=deployment, dsn=okg_migrated,
            wait_for_result=False, sync_wait_seconds=10,
        )
    assert 0.4 <= refused_after < 3.0, refused_after
    assert result.workflow_id == "wf-queued"
    assert len(enqueued) == 1 and enqueued[0] >= released_at[0]
    assert _signal_free(okg_migrated, deployment)


@pytest.mark.postgres
def test_run_publish_waits_for_a_worker_sync_queued_beside_it(
    okg_migrated: str,
) -> None:
    """A command-line run's own publish checks the worker queue: a sync
    queued or running beside the run blocks the publish, then refuses."""
    deployment = _run_guard_deployment()
    answers = iter([None])

    def probe() -> str | None:
        return next(answers, "wf-beside")

    with source_sync_run_guard(
        dsn=okg_migrated, deployment=deployment, wait_seconds=0,
        runtime_probe=probe,
    ):
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            own = Publisher(okg_migrated, deployment)
            assert own._blocking_source_sync_workflow_id(conn=conn) == "wf-beside"
            started = time.monotonic()
            with pytest.raises(PublisherLeaseHeld) as raised:
                own._acquire_dbos_publish_lock(conn, wait_seconds=1.0)
            waited = time.monotonic() - started
    assert raised.value.data["blocking_workflow_id"] == "wf-beside"
    assert 0.9 <= waited < 5.0, waited


@pytest.mark.postgres
def test_scheduled_publish_rechecks_source_sync_after_taking_the_mutex(
    okg_migrated: str,
) -> None:
    """A sync that appears between the first check and taking the publish
    mutex still wins: the publisher lets go of the mutex and yields."""
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    deployment = _run_guard_deployment()
    answers = iter([None, "wf-late"])
    scheduled = Publisher(
        okg_migrated, deployment,
        source_sync_priority_probe=lambda: next(answers),
    )
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        with pytest.raises(PublisherLeaseHeld) as raised:
            scheduled._acquire_dbos_publish_lock(conn, wait_seconds=0)
    assert raised.value.data["blocking_workflow_id"] == "wf-late"
    classid, objid = publisher_publish_advisory_keys(deployment)
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        assert conn.execute(
            "SELECT pg_try_advisory_lock(%s, %s)", (classid, objid),
        ).fetchone()[0]


# --- a lost signal (review round 2) ---------------------------------------


@pytest.mark.postgres
def test_run_guard_session_survives_an_idle_session_timeout(
    okg_migrated: str,
) -> None:
    deployment = _run_guard_deployment()
    with psycopg.connect(okg_migrated, autocommit=True) as admin_conn:
        dbname = admin_conn.info.dbname
        admin_conn.execute(
            f'ALTER DATABASE "{dbname}" SET idle_session_timeout = 500',
        )
        try:
            with source_sync_run_guard(
                dsn=okg_migrated, deployment=deployment, wait_seconds=0,
            ) as pid:
                time.sleep(1.5)
                holders = [
                    row[0] for row in admin_conn.execute(
                        "SELECT pid FROM pg_locks WHERE locktype = 'advisory'"
                        " AND granted AND pid = %s", (pid,),
                    )
                ]
                assert holders == [pid]
        finally:
            admin_conn.execute(
                f'ALTER DATABASE "{dbname}" RESET idle_session_timeout',
            )


@pytest.mark.postgres
def test_run_publish_refuses_when_the_signal_session_died(
    okg_migrated: str, caplog: pytest.LogCaptureFixture,
) -> None:
    """If the guard's session dies mid-run, the run's publish refuses with
    source_sync_signal_lost (freed or taken by another run), and the
    guard's release logs the dead session without replacing the result."""
    from okg.substrate.errors import SourceSyncSignalLost

    deployment = _run_guard_deployment()
    with psycopg.connect(okg_migrated, autocommit=True) as admin_conn:
        with source_sync_run_guard(
            dsn=okg_migrated, deployment=deployment, wait_seconds=0,
        ) as pid:
            admin_conn.execute("SELECT pg_terminate_backend(%s)", (pid,))
            for _ in range(50):
                if _signal_free(okg_migrated, deployment):
                    break
                time.sleep(0.1)
            with psycopg.connect(okg_migrated, autocommit=True) as conn:
                own = Publisher(okg_migrated, deployment)
                with pytest.raises(SourceSyncSignalLost) as freed:
                    own._acquire_dbos_publish_lock(conn, wait_seconds=0)
                with _Holder(okg_migrated, deployment) as other:
                    with pytest.raises(SourceSyncSignalLost) as taken:
                        own._blocking_source_sync_workflow_id(conn=conn)
            outcome = "run finished"
    assert outcome == "run finished"
    assert freed.value.code == "source_sync_signal_lost"
    assert freed.value.data == {
        "deployment": deployment, "own_pid": pid, "holder_pids": [],
    }
    assert taken.value.data["holder_pids"] == [other.pid]
    assert "source-sync signal release failed" in caplog.text


@pytest.mark.postgres
def test_run_stops_at_the_next_chunk_once_its_signal_session_dies(
    clean_runtime: str,
) -> None:
    """Review round 3: other publishers may publish a lost run's writes, so
    the run checks its lock before every chunk and stops at the first one
    after the lock is gone. Only chunks written while it held the lock (plus
    at most the one in flight) reach the branch."""
    from okg.substrate.errors import SourceSyncSignalLost
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.base import ProgressMarker, SourceRun

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[])
    deployment = _run_guard_deployment()
    batch = 100  # the runner commits a chunk every 100 progress markers
    killed: list[int] = []

    class _KillsItsGuardMidRun:
        name = "guard-kill"
        profile = "discovery_crawl"

        def __init__(self, guard_pid: int) -> None:
            self.guard_pid = guard_pid

        def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
            del run_id

            def stream():
                for i in range(3 * batch):
                    if i == batch + 10:
                        with psycopg.connect(clean_runtime, autocommit=True) as c:
                            c.execute(
                                "SELECT pg_terminate_backend(%s)",
                                (self.guard_pid,),
                            )
                        killed.append(i)
                    yield NodeFact(
                        node_id=f"n:guard-kill:{i:04d}",
                        subtype="concept",
                        attrs={"text": f"record {i}"},
                        source_record_id={"path": f"r{i:04d}"},
                        source_revision={"v": 1},
                    )
                    yield ProgressMarker(record_id=f"r{i:04d}", fingerprint=str(i))

            return SourceRun(facts=stream(), run_mode=mode)

    with source_sync_run_guard(
        dsn=clean_runtime, deployment=deployment, wait_seconds=0,
    ) as pid:
        with pytest.raises(SourceSyncSignalLost):
            SourceRunner(
                clean_runtime,
                source_name="test.guard-kill",
                deployment_name=deployment,
            ).run(_KillsItsGuardMidRun(pid), mode="cursor")
    assert killed == [batch + 10]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        head = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        written = head.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            "WHERE source = 'test.guard-kill'",
        )[0]["n"]
    # Chunk 1 (100 records) committed under the lock; chunk 2 is the first
    # write after the kill and is refused, so nothing past chunk 1 lands
    # (a failed run's compensation may also have removed chunk 1).
    assert written in (0, batch), written


@pytest.mark.postgres
def test_source_child_chunk_check_follows_the_parent_hold(
    okg_migrated: str,
) -> None:
    """A source child checks the parent's hold by looking the parent's
    session up in pg_stat_activity by pid AND backend_start: it passes while
    that session lives, and refuses once it is gone (lock then free or taken
    by another run) or when the pid now names a different session."""
    from okg.substrate.errors import SourceSyncSignalLost
    from okg.substrate.runtime.source_sync_priority import (
        adopt_run_guard,
        ensure_run_guard_held,
    )

    deployment = _run_guard_deployment()
    outcomes: dict[str, object] = {}

    def child(handoff: dict[str, object], label: str) -> None:
        adopt_run_guard(handoff, dsn=okg_migrated)
        try:
            ensure_run_guard_held(deployment)
            outcomes[label] = "held"
        except SourceSyncSignalLost as exc:
            outcomes[label] = exc.data["holder_pids"]

    def in_child(handoff: dict[str, object], label: str) -> None:
        contextvars.copy_context().run(child, handoff, label)

    with psycopg.connect(okg_migrated, autocommit=True) as admin_conn:
        parent = _Holder(okg_migrated, deployment)
        with parent:
            started = admin_conn.execute(
                "SELECT backend_start::text FROM pg_stat_activity WHERE pid = %s",
                (parent.pid,),
            ).fetchone()[0]
            handoff = {
                "deployment": deployment,
                "pid": parent.pid,
                "backend_start": started,
            }
            in_child(handoff, "alive")
            # The same pid with another start time is another session (a
            # reused pid), not the parent.
            in_child({**handoff, "backend_start": "2001-01-01 00:00:00+00"}, "reused")
            admin_conn.execute("SELECT pg_terminate_backend(%s)", (parent.pid,))
            for _ in range(50):
                if _signal_free(okg_migrated, deployment):
                    break
                time.sleep(0.1)
            in_child(handoff, "freed")
            with _Holder(okg_migrated, deployment) as other:
                in_child(handoff, "taken")
    assert other.pid != parent.pid
    assert outcomes == {"alive": "held", "reused": [], "freed": [], "taken": []}


@pytest.mark.postgres
def test_run_error_survives_a_dead_signal_session(okg_migrated: str) -> None:
    deployment = _run_guard_deployment()
    with psycopg.connect(okg_migrated, autocommit=True) as admin_conn:
        with pytest.raises(ValueError, match="the run's own error"):
            with source_sync_run_guard(
                dsn=okg_migrated, deployment=deployment, wait_seconds=0,
            ) as pid:
                admin_conn.execute("SELECT pg_terminate_backend(%s)", (pid,))
                time.sleep(0.3)
                raise ValueError("the run's own error")


@pytest.mark.postgres
def test_run_guard_logs_progress_while_it_waits(
    okg_migrated: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(priority, "_WAIT_PROGRESS_LOG_SECONDS", 0.2)
    deployment = _run_guard_deployment()
    with _Holder(okg_migrated, deployment):
        with caplog.at_level("WARNING"):
            with pytest.raises(SourceSyncAlreadyRunning):
                with source_sync_run_guard(
                    dsn=okg_migrated, deployment=deployment,
                    wait_seconds=1.0, poll_seconds=0.05,
                ):
                    pass
    waits = [r for r in caplog.records if "waiting for another sync" in r.message]
    assert len(waits) >= 3, [r.message for r in waits]


# --- a publish from inside a sync -----------------------------------------


@pytest.mark.postgres
def test_publish_inside_a_sync_waits_then_refuses_while_another_sync_holds(
    okg_migrated: str,
) -> None:
    deployment = _run_guard_deployment()
    with _Holder(okg_migrated, deployment):
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            in_sync = Publisher(
                okg_migrated, deployment, source_sync_publish_context=True,
            )
            started = time.monotonic()
            with pytest.raises(PublisherLeaseHeld) as raised:
                in_sync._acquire_dbos_publish_lock(conn, wait_seconds=1.0)
            waited = time.monotonic() - started

            scheduled = Publisher(okg_migrated, deployment)
            started = time.monotonic()
            with pytest.raises(PublisherLeaseHeld) as yielded:
                scheduled._acquire_dbos_publish_lock(conn, wait_seconds=5.0)
            scheduled_wait = time.monotonic() - started
    assert raised.value.data["reason"] == "source_sync_active"
    assert raised.value.data["blocking_workflow_id"] == (
        SOURCE_SYNC_ADMISSION_INTENT_ID
    )
    assert 0.9 <= waited < 5.0, waited
    # A scheduled publish still yields at once, as before.
    assert yielded.value.data["reason"] == "source_sync_active"
    assert scheduled_wait < 0.5, scheduled_wait


@pytest.mark.postgres
def test_publish_inside_a_sync_proceeds_when_the_other_sync_finishes(
    okg_migrated: str,
) -> None:
    deployment = _run_guard_deployment()
    holder = _Holder(okg_migrated, deployment)
    with holder:
        threading.Timer(0.8, holder.release.set).start()
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            in_sync = Publisher(
                okg_migrated, deployment, source_sync_publish_context=True,
            )
            started = time.monotonic()
            key = in_sync._acquire_dbos_publish_lock(conn, wait_seconds=10)
            waited = time.monotonic() - started
            in_sync._release_dbos_publish_lock(conn, key)
    assert 0.6 <= waited < 5.0, waited


@pytest.mark.postgres
def test_nested_run_reuses_its_own_signal(okg_migrated: str) -> None:
    """A run nested inside another on the same deployment and context (e.g.
    provision calling run_ingest under its own guard) reuses the held lock
    instead of waiting on itself; the lock is released only by the outer."""
    deployment = _run_guard_deployment()
    with source_sync_run_guard(
        dsn=okg_migrated, deployment=deployment, wait_seconds=0,
    ) as outer:
        with source_sync_run_guard(
            dsn=okg_migrated, deployment=deployment, wait_seconds=0,
        ) as inner:
            assert inner == outer
        assert not _signal_free(okg_migrated, deployment)
    assert _signal_free(okg_migrated, deployment)


@pytest.mark.postgres
def test_run_publish_never_blocks_on_its_own_signal(okg_migrated: str) -> None:
    """The run's own publish (same context, or a copied context on a worker
    thread) sees no blocker; any other context still sees the run."""
    deployment = _run_guard_deployment()
    with source_sync_run_guard(
        dsn=okg_migrated, deployment=deployment, wait_seconds=0,
    ):
        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            own = Publisher(okg_migrated, deployment)
            assert own._blocking_source_sync_workflow_id(conn=conn) is None
            key = own._acquire_dbos_publish_lock(conn, wait_seconds=0)
            own._release_dbos_publish_lock(conn, key)

        seen: dict[str, str | None] = {}

        def probe(label: str) -> None:
            with psycopg.connect(okg_migrated, autocommit=True) as conn:
                seen[label] = (
                    source_sync_admission_intent_workflow_id_on_connection(
                        conn=conn, deployment=deployment,
                    )
                )

        copied = threading.Thread(
            target=contextvars.copy_context().run, args=(probe, "copied"),
        )
        other = threading.Thread(target=probe, args=("other",))
        copied.start()
        other.start()
        copied.join(timeout=10)
        other.join(timeout=10)
    assert seen == {
        "copied": None,
        "other": SOURCE_SYNC_ADMISSION_INTENT_ID,
    }


# --- crash recovery -------------------------------------------------------


@pytest.mark.postgres
def test_killed_holder_releases_the_signal(okg_migrated: str) -> None:
    deployment = _run_guard_deployment()
    script = textwrap.dedent(
        f"""
        import sys, time
        from okg.substrate.runtime.source_sync_priority import (
            source_sync_run_guard,
        )
        with source_sync_run_guard(
            dsn={okg_migrated!r}, deployment={deployment!r}, wait_seconds=0,
        ):
            print("held", flush=True)
            time.sleep(120)
        """
    )
    # #703: its own session, and no stdio shared with the xdist worker.
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env={**os.environ},
        start_new_session=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "held"
        with pytest.raises(SourceSyncAlreadyRunning):
            with source_sync_run_guard(
                dsn=okg_migrated, deployment=deployment, wait_seconds=0,
            ):
                pass
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        started = time.monotonic()
        with source_sync_run_guard(
            dsn=okg_migrated, deployment=deployment, wait_seconds=15,
        ):
            reacquired = time.monotonic() - started
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    assert reacquired < 15
    assert _signal_free(okg_migrated, deployment)
