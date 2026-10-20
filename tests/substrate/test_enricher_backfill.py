"""Targeted enricher backfill tests."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4

import psycopg
import pytest

import okg.substrate.enrichers.backfill as backfill
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.errors import (
    EnricherBackfillAdmissionRejected,
    EnricherBackfillBranchUnsupported,
    EnricherBackfillChunkFailed,
    EnricherBackfillSelectionError,
    EnricherBackfillUnsupportedOutput,
    EnricherBackfillUnsafeEnricher,
    SubstrateError,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import service as ingest_service
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.producer_authority import materialize_producer_profile
from okg.substrate.publisher import Publisher
from okg.substrate.publisher import deployment_stack as _stack_mod
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_factory_catalog_writer,
    bind_test_catalog_writer,
)


pytestmark = pytest.mark.postgres

run_enricher_backfill = backfill.run_enricher_backfill

# The product authority resolver, captured before any fixture patches it, so
# the refusal regression below exercises the real code path.
_REAL_RUNTIME_CATALOG_IDENTITY = _stack_mod.build_runtime_catalog_identity

# A deployment name with no directory under deployments/ and no sealed
# environment, so catalog writer authority cannot resolve for it.
_UNOWNED_DEPLOYMENT = "backfill-unowned-deployment"


@pytest.fixture(autouse=True)
def _explicit_backfill_catalog_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Give every backfill in this module explicit catalog writer authority.

    ``run_enricher_backfill`` builds its Publisher through
    ``build_deployment_publisher``, which refuses when the deployment has no
    resolvable writer authority. These tests drive a synthetic ``test``
    deployment with no directory on disk, so they bind the explicit pytest
    writer instead. ``test_backfill_refuses_without_catalog_writer_authority``
    restores the real resolver and proves the refusal still fires.
    """
    bind_factory_catalog_writer(monkeypatch)
    deployment_dir = tmp_path / "backfill-deployment"
    deployment_dir.mkdir()
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: []\n",
        encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: test\n"
        "description: backfill fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "OKG fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
    }
    subprocess.run(
        ["git", "init", "-q", "-b", "main"],
        cwd=deployment_dir,
        env=git_env,
        check=True,
    )
    subprocess.run(
        ["git", "add", "-A"], cwd=deployment_dir, env=git_env, check=True,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "fixture"],
        cwd=deployment_dir,
        env=git_env,
        check=True,
    )
    import okg.substrate.deployments as deployments

    real_find = deployments.find_deployment_dir
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda name, **kwargs: (
            deployment_dir if name == "test" else real_find(name, **kwargs)
        ),
    )


class _BackfillEnricher:
    name = "backfill_links"
    reads_subtypes = ("bf_src",)
    reads_edge_types = ()
    emits_edge_types = ("bf_links",)
    requires_narrowings = (("bf_src", "bf_links", "bf_dst"),)
    publisher_skip_when_clean = True
    targeted_backfill_chunk_safe = True

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_on_call = False

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id
        dirty = tuple(sorted(incremental.dirty_node_ids if incremental else ()))
        self.calls.append(dirty)
        if self.fail_on_call:
            raise AssertionError("enricher should have been skipped")
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src=node_id,
                        edge_type="bf_links",
                        dst="bf:dst",
                    )
                    for node_id in dirty
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _RejectedBackfillEnricher(_BackfillEnricher):
    name = "backfill_rejected"

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id
        dirty = tuple(sorted(incremental.dirty_node_ids if incremental else ()))
        self.calls.append(dirty)
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src=node_id,
                        edge_type="bf_denied",
                        dst="bf:dst",
                    )
                    for node_id in dirty
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _NodeOutputEnricher(_BackfillEnricher):
    name = "backfill_nodes"
    emits_node_types = ("bf_derived",)
    producer_policy = {
        "producer_policy": {
            "allowed_record_kinds": ["edge"],
            "output_signature": {"nodes": [{"subtype": "bf_derived"}]},
        },
    }


class _SneakyNodeFactEnricher(_BackfillEnricher):
    name = "backfill_sneaky_nodes"

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id
        dirty = tuple(sorted(incremental.dirty_node_ids if incremental else ()))
        self.calls.append(dirty)
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'bf_src', %s::jsonb, %s,
                        '{}'::jsonb, '{}'::jsonb, now(), %s, %s, 'I')
                """,
                (
                    "bf:sneaky",
                    json.dumps({"text": "sneaky"}),
                    f"_enricher:{self.name}",
                    f"{self.name}:{run_id}",
                    run_id,
                ),
            )
        return EnrichResult(self.name, n_edges_emitted=0)


class _MixedSourceEnricher(_BackfillEnricher):
    name = "backfill_mixed_source"

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id
        dirty = tuple(sorted(incremental.dirty_node_ids if incremental else ()))
        self.calls.append(dirty)
        with conn.cursor() as cur:
            selected = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src=node_id,
                        edge_type="bf_links",
                        dst="bf:dst",
                    )
                    for node_id in dirty
                ],
            )
            insert_deterministic_edges(
                cur,
                source="_enricher:other",
                candidates=[
                    DerivedEdgeCandidate(
                        src=node_id,
                        edge_type="bf_links",
                        dst="bf:other-dst",
                    )
                    for node_id in dirty
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=selected.inserted,
            n_edges_skipped=selected.skipped,
        )


class _UnsafeBackfillEnricher(_BackfillEnricher):
    name = "backfill_unsafe"
    targeted_backfill_chunk_safe = False


class _NoReadSubtypeEnricher(_BackfillEnricher):
    name = "backfill_no_read_subtypes"
    reads_subtypes = ()


class _OutOfChunkSameSourceEnricher(_BackfillEnricher):
    name = "backfill_out_of_chunk"

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id
        dirty = tuple(sorted(incremental.dirty_node_ids if incremental else ()))
        self.calls.append(dirty)
        sources = list(dirty)
        sources.append("bf:src:1")
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src=node_id,
                        edge_type="bf_links",
                        dst="bf:dst",
                    )
                    for node_id in sources
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_backfill_processes_limit_in_resumable_chunks(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=5)

    enricher = _BackfillEnricher()
    first = run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=3,
        chunk_size=2,
    )

    assert first.status == "partial"
    assert first.processed_inputs == 3
    assert first.progress_advanced is False
    assert enricher.calls == [
        ("bf:src:0", "bf:src:1"),
        ("bf:src:2",),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 3
        assert _progress_status(conn, enricher.name) is None

    second = run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=2,
    )

    assert second.status == "completed"
    assert second.processed_inputs == 2
    assert second.progress_advanced is True
    assert enricher.calls[-1] == ("bf:src:3", "bf:src:4")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 5
        assert _progress_status(conn, enricher.name) == "success"
        assert _ledger_edge_counts(conn, enricher.name) == {
            "bf:src:0": 1,
            "bf:src:1": 1,
            "bf:src:2": 1,
            "bf:src:3": 1,
            "bf:src:4": 1,
        }


def test_backfill_applies_selected_enricher_outputs_with_provenance(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _BackfillEnricher()
    result = run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=5,
    )

    assert result.status == "completed"
    assert result.chunks[0].edges_applied == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT src, dst, edge_type, source
              FROM okg.graph_edges
             WHERE edge_type = :edge_type
            """,
            {"edge_type": "bf_links"},
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT intent, status,
                       check_summary->'producer_policy'->>'status',
                       check_summary->'producer_policy'->>'profile_origin'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:backfill_links'
                   AND intent IN (
                       'derived_edge_emission',
                       'targeted_enricher_backfill'
                   )
                 ORDER BY intent
                """
            )
            admissions = cur.fetchall()
            cur.execute(
                """
                SELECT status
                  FROM okg.producer_profiles
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:backfill_links'
                   AND source_ref->>'profile_origin' IS NULL
                """
            )
            declared_profiles = cur.fetchall()

    assert rows == [{
        "src": "bf:src:0",
        "dst": "bf:dst",
        "edge_type": "bf_links",
        "source": "_enricher:backfill_links",
    }]
    # One backfill records BOTH envelopes under this producer id, and they
    # are not interchangeable, so assert each by intent rather than letting
    # a scan pick one (issue #1556). The emission decision carries the
    # enricher's declared, active authority. The fact-application pass cites
    # the bounded-fact-batch bookkeeping row instead: no output signature,
    # and marked machine-minted so it cannot outrank what the deployment
    # declared (issue #1545).
    assert admissions == [
        ("derived_edge_emission", "accepted", "active", None),
        (
            "targeted_enricher_backfill",
            "accepted",
            "compat",
            "bounded_fact_batch",
        ),
    ]
    # The enricher's own declared profile still records active authority,
    # and there is exactly one of them.
    assert declared_profiles == [("active",)]


def test_backfill_rejects_non_default_branch_before_progress(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _BackfillEnricher()
    with pytest.raises(EnricherBackfillBranchUnsupported) as excinfo:
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
            branch_name="experiment:probe",
        )

    assert excinfo.value.code == "enricher_backfill_branch_unsupported"
    assert excinfo.value.data["branch_name"] == "experiment:probe"
    assert enricher.calls == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None


def test_backfill_rejects_unmarked_enricher_before_writes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _UnsafeBackfillEnricher()
    with pytest.raises(EnricherBackfillUnsafeEnricher) as excinfo:
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    assert excinfo.value.code == "enricher_backfill_unsafe_enricher"
    assert excinfo.value.data["required_capability"] == (
        "targeted_backfill_chunk_safe"
    )
    assert enricher.calls == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None


def test_backfill_rejects_missing_read_subtypes_before_writes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _NoReadSubtypeEnricher()
    with pytest.raises(EnricherBackfillSelectionError) as excinfo:
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    assert excinfo.value.data["missing_contract"] == "reads_subtypes"
    assert enricher.calls == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                """
            )
            assert int(cur.fetchone()[0]) == 0


def test_backfill_watermarks_and_progress_run_under_publisher_lock(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    lock_depth = 0
    lock_entries: list[tuple[str, str, bool]] = []
    original_locked_connection = Publisher._locked_connection
    original_current_fact_watermarks = Publisher._current_fact_watermarks
    original_advance_enricher_progress = Publisher._advance_enricher_progress

    @contextmanager
    def observed_locked_connection(self, conn=None, **kwargs):
        nonlocal lock_depth
        lock_entries.append((
            self.deployment_name,
            self.branch_name,
            conn is not None,
        ))
        with original_locked_connection(self, conn, **kwargs) as locked:
            lock_depth += 1
            try:
                yield locked
            finally:
                lock_depth -= 1

    def observed_current_fact_watermarks(self, conn):
        del self
        assert lock_depth == 1
        return original_current_fact_watermarks(conn)

    def observed_advance_enricher_progress(self, *args, **kwargs):
        assert lock_depth == 1
        return original_advance_enricher_progress(self, *args, **kwargs)

    monkeypatch.setattr(
        Publisher,
        "_locked_connection",
        observed_locked_connection,
    )
    monkeypatch.setattr(
        Publisher,
        "_current_fact_watermarks",
        observed_current_fact_watermarks,
    )
    monkeypatch.setattr(
        Publisher,
        "_advance_enricher_progress",
        observed_advance_enricher_progress,
    )

    enricher = _BackfillEnricher()
    result = run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=5,
    )

    assert result.status == "completed"
    assert result.progress_advanced is True
    assert lock_entries == [("test", "default", True)]


def test_backfill_rolls_back_graph_apply_when_ledger_mark_fails(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    def fail_mark_inputs_complete(*args, **kwargs):
        raise RuntimeError("injected ledger failure")

    monkeypatch.setattr(
        backfill,
        "_mark_inputs_complete",
        fail_mark_inputs_complete,
    )

    enricher = _BackfillEnricher()
    with pytest.raises(EnricherBackfillChunkFailed):
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT src, dst, edge_type, source
              FROM okg.graph_edges
             WHERE edge_type = :edge_type
            """,
            {"edge_type": "bf_links"},
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.edge_facts
                 WHERE source = '_enricher:backfill_links'
                """
            )
            edge_facts = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:backfill_links'
                """
            )
            admission_rows = int(cur.fetchone()[0])

        assert rows == []
        assert edge_facts == 0
        assert admission_rows == 0
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None


def test_backfill_rejects_unrelated_same_chunk_derived_facts(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _MixedSourceEnricher()
    with pytest.raises(EnricherBackfillChunkFailed):
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT dst, source
              FROM okg.graph_edges
             WHERE edge_type = :edge_type
             ORDER BY dst
            """,
            {"edge_type": "bf_links"},
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.edge_facts
                 WHERE source = '_enricher:other'
                """
            )
            other_audit_facts = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.enricher_backfill_progress
                 WHERE deployment_name = 'test'
                   AND enricher_name = %s
                """,
                (enricher.name,),
            )
            progress_rows = int(cur.fetchone()[0])

    assert rows == []
    assert other_audit_facts == 0
    assert progress_rows == 0


def test_backfill_rejected_admission_does_not_advance_progress(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _RejectedBackfillEnricher()
    with pytest.raises(EnricherBackfillAdmissionRejected) as exc:
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )
    # Pin the INTENDED rejection reason (out-of-signature bf_denied
    # edge). Pre-migration this test passed for the wrong reason —
    # every chunk was rejected as derived_output_trust_exceeds_input
    # because the fixture input source had no trust profile.
    assert [
        verdict.get("reason") for verdict in exc.value.data["verdicts"]
    ] == ["producer_edge_triple_not_allowed"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT src
              FROM okg.graph_edges
             WHERE edge_type = :edge_type
            """,
            {"edge_type": "bf_denied"},
        )
    assert rows == []


def test_backfill_rejects_same_source_out_of_chunk_edges(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=2)

    enricher = _OutOfChunkSameSourceEnricher()
    with pytest.raises(EnricherBackfillChunkFailed) as excinfo:
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=1,
            chunk_size=1,
        )

    assert "out-of-chunk" in str(excinfo.value)
    assert excinfo.value.data["out_of_scope_edge_count"] == 1
    assert enricher.calls == [("bf:src:0",)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT src
              FROM okg.graph_edges
             WHERE edge_type = :edge_type
            """,
            {"edge_type": "bf_links"},
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.edge_facts
                 WHERE source = '_enricher:backfill_out_of_chunk'
                """
            )
            edge_facts = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:backfill_out_of_chunk'
                """
            )
            admission_rows = int(cur.fetchone()[0])

        assert rows == []
        assert edge_facts == 0
        assert admission_rows == 0
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None


def test_backfill_rejects_node_output_enricher_before_writes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _NodeOutputEnricher()
    with pytest.raises(EnricherBackfillUnsupportedOutput):
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                """
            )
            assert int(cur.fetchone()[0]) == 0


def test_backfill_rolls_back_undeclared_node_facts(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _SneakyNodeFactEnricher()
    with pytest.raises(EnricherBackfillUnsupportedOutput):
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment="test",
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(conn, enricher.name) == 0
        assert _progress_status(conn, enricher.name) is None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE source = '_enricher:backfill_sneaky_nodes'
                """
            )
            assert int(cur.fetchone()[0]) == 0


def test_backfill_progress_uses_pinned_start_frontier(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    inserted = False
    original_count = backfill._missing_input_count

    def racing_missing_count(*args, **kwargs):
        nonlocal inserted
        count = original_count(*args, **kwargs)
        if count == 0 and not inserted:
            inserted = True
            _insert_late_node_fact(args[0], "bf:src:late")
        return count

    monkeypatch.setattr(
        backfill,
        "_missing_input_count",
        racing_missing_count,
    )

    enricher = _BackfillEnricher()
    result = run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=5,
    )

    assert result.status == "completed"
    assert result.progress_advanced is True
    assert inserted is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        progress_fact_watermark, _progress_edge_watermark = (
            _progress_watermarks(conn, enricher.name)
        )
        late_fact_id = _node_fact_id(conn, "bf:src:late")
        assert progress_fact_watermark == result.input_fact_watermark
        assert progress_fact_watermark < late_fact_id


def test_backfill_satisfies_missing_enricher_progress_for_publish_skip(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    enricher = _BackfillEnricher()
    run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=5,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        assert ingest_service._stack_enrichers_have_pending_work(
            cur,
            deployment="test",
            enricher_names=[enricher.name],
        ) is False


def test_normal_publish_skips_completed_targeted_backfill_first_run(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=1)

    baseline = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert baseline["status"] in {"published", "no_op"}

    enricher = _BackfillEnricher()
    run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=5,
    )
    calls_after_backfill = len(enricher.calls)
    enricher.fail_on_call = True

    out = Publisher(clean_runtime, "test", enrichers=[enricher], catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()

    assert out["status"] in {"published", "no_op"}
    assert len(enricher.calls) == calls_after_backfill
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _progress_status(conn, enricher.name) == "success"


def test_backfill_accumulates_lifetime_edges_emitted(
    clean_runtime: str,
) -> None:
    """A completed backfill must credit the enricher's lifetime emission
    counter with the edges it produced. Otherwise `okg enrichers backfill`
    (the canonical way to populate a newly-wired enricher over an existing
    corpus) leaves the row at last_status='success', lifetime=0 and the
    #707 liveness signal falsely flags it wired-but-silent for a static
    corpus that never re-dirties."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_backfill_catalog_and_graph(conn, src_count=3)

    enricher = _BackfillEnricher()
    result = run_enricher_backfill(
        dsn=clean_runtime,
        deployment="test",
        enricher_name=enricher.name,
        enrichers=[enricher],
        limit=10,
        chunk_size=2,
    )

    assert result.status == "completed"
    assert result.progress_advanced is True
    # _BackfillEnricher emits one bf_links edge per source node.
    emitted = sum(int(c.n_edges_emitted) for c in result.chunks)
    assert emitted == 3
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _progress_status(conn, enricher.name) == "success"
        assert _progress_lifetime(conn, enricher.name) == 3


def test_backfill_refuses_without_catalog_writer_authority(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backfill that cannot resolve catalog writer authority must refuse.

    Regression for issue #1563. The autouse fixture in this module binds an
    explicit test writer; this test puts the real resolver back so the
    product refusal is proven rather than assumed. A deployment with no
    directory on disk and no sealed environment has no writer scope, so
    `okg enricher backfill` must fail loudly before it touches the ledger,
    the graph, or enricher progress.

    A decoy row under a DIFFERENT deployment makes the post-refusal
    assertions load-bearing: a ledger or progress read that is not scoped to
    the refused deployment reads that row instead and fails here.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_decoy_enricher_rows(conn, deployment="test")

    monkeypatch.setattr(
        _stack_mod,
        "build_runtime_catalog_identity",
        _REAL_RUNTIME_CATALOG_IDENTITY,
    )

    enricher = _BackfillEnricher()
    with pytest.raises(SubstrateError) as excinfo:
        run_enricher_backfill(
            dsn=clean_runtime,
            deployment=_UNOWNED_DEPLOYMENT,
            enricher_name=enricher.name,
            enrichers=[enricher],
            limit=10,
            chunk_size=5,
        )

    assert "catalog writer authority" in str(excinfo.value)
    assert excinfo.value.code == "catalog_writer_scope_required"
    assert enricher.calls == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _ledger_count(
            conn,
            enricher.name,
            deployment=_UNOWNED_DEPLOYMENT,
        ) == 0
        assert _progress_status(
            conn,
            enricher.name,
            deployment=_UNOWNED_DEPLOYMENT,
        ) is None
        # The decoy is still there, so the two reads above were scoped and
        # not merely looking at an empty database.
        assert _ledger_count(conn, enricher.name, deployment="test") == 1
        assert _progress_status(
            conn,
            enricher.name,
            deployment="test",
        ) == "success"


def _seed_decoy_enricher_rows(
    conn: psycopg.Connection,
    *,
    deployment: str,
) -> None:
    """Write one backfill-ledger row and one progress row for `deployment`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.enricher_backfill_progress
              (deployment_name, enricher_name, input_node_id,
               profile_fingerprint, transform_checksum)
            VALUES (%s, %s, 'bf:decoy', 'sha256:decoy', 'sha256:decoy')
            """,
            (deployment, _BackfillEnricher.name),
        )
        cur.execute(
            """
            INSERT INTO okg.enricher_progress
              (deployment_name, enricher_name, last_status)
            VALUES (%s, %s, 'success')
            """,
            (deployment, _BackfillEnricher.name),
        )


def _seed_backfill_catalog_and_graph(
    conn: psycopg.Connection,
    *,
    src_count: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            VALUES
              ('bf_src', '{}'::jsonb, %s),
              ('bf_dst', '{}'::jsonb, %s)
            """,
            (catalog_version_id, catalog_version_id),
        )
        cur.executemany(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, %s, %s, 0)
            ON CONFLICT DO NOTHING
            """,
            [
                (catalog_version_id, "bf_src", "bf_src"),
                (catalog_version_id, "bf_dst", "bf_dst"),
            ],
        )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES ('bf_src', 'bf_links', 'bf_dst', %s)
            """,
            (catalog_version_id,),
        )
    facts = [
        NodeFact(
            f"bf:src:{idx}",
            "bf_src",
            {"text": f"source {idx}"},
            {},
            {},
        )
        for idx in range(src_count)
    ]
    facts.append(NodeFact("bf:dst", "bf_dst", {"text": "target"}, {}, {}))
    ChronosGraphStore.from_connection(conn).apply_facts(
        facts,
        source="backfill_fixture",
        run_id=str(uuid4()),
    )
    # Trust-lattice migration (13faaf7a4 / f2daeacb1): derived output
    # must not exceed its INPUT trust, and an input source without a
    # materialized profile counts as untrusted — which fails the
    # derived-deterministic-trusted enricher closed
    # (derived_output_trust_exceeds_input) before any backfill
    # behavior under test can run. Give both fixture input sources a
    # rank-3 profile, mirroring test_derived_producer_authority.
    for input_source in ("backfill_fixture", "backfill_race_fixture"):
        materialize_producer_profile(
            conn,
            deployment_name="test",
            producer_id=input_source,
            producer_kind="source",
            trust_label="repo-local-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": input_source},
            allowed_record_kinds=(),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={"summary": "input trust fixture"},
            source_ref={"fixture": "test_enricher_backfill"},
            catalog_version_id=catalog_version_id,
        )
    return catalog_version_id


def _insert_late_node_fact(conn: psycopg.Connection, node_id: str) -> None:
    run_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'bf_src', %s::jsonb, 'backfill_race_fixture',
                    '{}'::jsonb, '{}'::jsonb, now(), %s, %s, 'I')
            """,
            (
                node_id,
                json.dumps({"text": node_id}),
                f"backfill_race_fixture:{node_id}:{run_id}",
                run_id,
            ),
        )


def _ledger_count(
    conn: psycopg.Connection,
    enricher_name: str,
    *,
    deployment: str = "test",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.enricher_backfill_progress
             WHERE deployment_name = %s
               AND enricher_name = %s
            """,
            (deployment, enricher_name),
        )
        return int(cur.fetchone()[0])


def _ledger_edge_counts(
    conn: psycopg.Connection,
    enricher_name: str,
) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_node_id, emitted_edge_count
              FROM okg.enricher_backfill_progress
             WHERE deployment_name = 'test'
               AND enricher_name = %s
             ORDER BY input_node_id
            """,
            (enricher_name,),
        )
        return {str(node_id): int(count) for node_id, count in cur.fetchall()}


def _node_fact_id(conn: psycopg.Connection, node_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(fact_id)
              FROM okg.node_facts
             WHERE node_id = %s
            """,
            (node_id,),
        )
        return int(cur.fetchone()[0])


def _progress_watermarks(
    conn: psycopg.Connection,
    enricher_name: str,
) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_watermark, edge_watermark
              FROM okg.enricher_progress
             WHERE deployment_name = 'test'
               AND enricher_name = %s
            """,
            (enricher_name,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def _progress_lifetime(
    conn: psycopg.Connection,
    enricher_name: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lifetime_edges_emitted
              FROM okg.enricher_progress
             WHERE deployment_name = 'test'
               AND enricher_name = %s
            """,
            (enricher_name,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _progress_status(
    conn: psycopg.Connection,
    enricher_name: str,
    *,
    deployment: str = "test",
) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_status
              FROM okg.enricher_progress
             WHERE deployment_name = %s
               AND enricher_name = %s
            """,
            (deployment, enricher_name),
        )
        row = cur.fetchone()
    return str(row[0]) if row else None
