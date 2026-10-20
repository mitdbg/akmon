"""Regression for #681 and #720 typed partial-failure visibility.

The embed batch workflow used to swallow every per-node embed exception
into an ``errors`` list and return ``{processed: 0, errors: N}`` with no
``any_failed`` key. DBOS then marked the row SUCCESS, and the
publisher-health silent-failure net had no queryable signal — DBOS output is
opaque to SQL readers — so a partial embedding outage stayed green.

These tests mirror ``source_sync_workflow.py``'s fail-loud contract:

  * all-fail (``processed==0, errors>0``) RAISES so DBOS marks ERROR —
    this is the actual #681 fix and does not depend on the net;
  * partial failure returns SUCCESS and persists typed evidence through the
    operational-outcome transaction;
  * a healthy run is unchanged (``any_failed: false``);
  * the former serializer coupling regression checks the typed outcome mapper;
    the live DBOS/Postgres proof is exercised by the test below.

The typed mapper test uses stubs for deployment/DSN/Chronos-checkout. The
live DBOS/Postgres proof is separate and uses a harness-created disposable
database to assert terminal SUCCESS, typed SQL rows, and a non-green doctor.
"""
from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import pytest


_ROWS = [
    ("n-1", "text one", "hash1"),
    ("n-2", "text two", "hash2"),
    ("n-3", "text three", "hash3"),
]


def _verified_hash_plan():
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    return build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )


class _FakeConnCM:
    def __enter__(self):
        return object()

    def __exit__(self, *_a):
        return False


class _FakeGraph:
    def __init__(self, rows):
        self._rows = [
            {"node_id": nid, "text": txt, "text_hash": th}
            for (nid, txt, th) in rows
        ]

    def query(self, _sql, _params):
        return self._rows


class _FakeStore:
    def __init__(self, rows):
        self._rows = rows

    def checkout(self):
        return _FakeGraph(self._rows)


@pytest.fixture
def stub_batch_env(monkeypatch: pytest.MonkeyPatch):
    """Stub manifest resolution, DSN, psycopg, the Chronos checkout, and
    the profiling context so ``_run_one_embed_batch`` runs its claim +
    per-node loop entirely in-process against ``_ROWS``."""
    import psycopg

    import okg.substrate.deployments as deployments
    import okg.substrate.runtime.profiling_bridge as profiling_bridge
    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda _dep: SimpleNamespace(
            embedding_config=_verified_hash_plan().config,
        ),
    )
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, deployment=None: "postgres://stub/db",
    )
    monkeypatch.setattr(
        psycopg, "connect", lambda *_a, **_k: _FakeConnCM(),
    )
    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        staticmethod(lambda _conn, ensure_schema=False: _FakeStore(_ROWS)),
    )

    @contextmanager
    def _null_profiling(**_kwargs):
        yield

    monkeypatch.setattr(
        profiling_bridge, "workflow_profiling_context", _null_profiling,
    )


def test_a_node_write_refuses_a_plan_that_does_not_fit_the_node_column(
    monkeypatch: pytest.MonkeyPatch,
):
    """Must-fail: a search set 768 wide has a plan with ``node_column``
    false; writing one node with it must refuse before it opens the graph,
    not write a 768-wide vector into the 2560-wide node column."""
    from okg.substrate.embeddings.config import (
        build_embedding_execution_plan,
        parse_embedding_config,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime.embed_workflow import _embed_and_write_one

    config = parse_embedding_config(plural={
        "enabled": True,
        "search_set": "main",
        "sets": {"main": {
            "backend": "hash@768",
            "subtypes": ["note"],
            "max_chars": 24_000,
            "document": {"default": {"header": ["subtype", "title"], "body": "text"}},
            "index": {"kind": "hnsw"},
        }},
    })
    plan = build_embedding_execution_plan(config)
    assert plan.enabled and plan.embedder is not None
    assert plan.node_column is False

    def _no_graph(*_a, **_k):
        raise AssertionError("the node write opened the graph")

    monkeypatch.setattr(ChronosGraphStore, "connect", staticmethod(_no_graph))
    with pytest.raises(RuntimeError, match="fit the node column"):
        _embed_and_write_one(
            dsn="postgres://stub/db",
            node_id="n-1",
            text="text one",
            text_hash="hash1",
            embedding_plan=plan,
        )


def test_all_embeds_fail_raises(stub_batch_env):
    """Every node embed raising ends the workflow as a failure (RAISE),
    not a SUCCESS dict — so DBOS marks the row ERROR."""
    from okg.substrate.runtime.embed_workflow import _run_one_embed_batch

    def _always_fail(node_id, _text, _hash, _spec):
        raise RuntimeError(f"embed backend down for {node_id}")

    with pytest.raises(RuntimeError, match="all 3 node embeds failed"):
        _run_one_embed_batch(
            deployment="dep",
            batch_size=10,
            embed_one=_always_fail,
            embedding_plan=_verified_hash_plan(),
        )


def test_partial_failure_returns_typed_source_summary(stub_batch_env):
    """A partial failure stays SUCCESS and stamps ``any_failed: true``."""
    from okg.substrate.runtime.embed_workflow import _run_one_embed_batch

    def _fail_last(node_id, _text, _hash, _spec):
        if node_id == "n-3":
            raise RuntimeError("one node down")
        return {"node_id": node_id, "cache_hit": False}

    out = _run_one_embed_batch(
        deployment="dep",
        batch_size=10,
        embed_one=_fail_last,
        embedding_plan=_verified_hash_plan(),
    )
    assert out["any_failed"] is True
    assert out["processed"] == 2
    assert out["errors"] == 1


def test_healthy_run_reports_any_failed_false(stub_batch_env):
    """A run with no embed errors is unchanged: SUCCESS, any_failed=false."""
    from okg.substrate.runtime.embed_workflow import _run_one_embed_batch

    def _all_ok(node_id, _text, _hash, _spec):
        return {"node_id": node_id, "cache_hit": False}

    out = _run_one_embed_batch(
        deployment="dep",
        batch_size=10,
        embed_one=_all_ok,
        embedding_plan=_verified_hash_plan(),
    )
    assert out["any_failed"] is False
    assert out["processed"] == 3
    assert out["errors"] == 0


def test_partial_mapper_emits_typed_outcome(stub_batch_env):
    """The former serializer-coupling xfail now asserts typed evidence."""
    from okg.substrate.runtime.embed_workflow import (
        _embed_operational_outcome,
        _run_one_embed_batch,
    )

    def _fail_last(node_id, _text, _hash, _spec):
        if node_id == "n-3":
            raise RuntimeError("one node down")
        return {"node_id": node_id}

    out = _run_one_embed_batch(
        deployment="dep",
        batch_size=10,
        embed_one=_fail_last,
        embedding_plan=_verified_hash_plan(),
    )
    typed = _embed_operational_outcome(out)
    assert typed["outcome"] == "partial_failure"
    assert typed["succeeded_count"] == 2
    assert typed["failed_count"] == 1
    assert typed["semantic_units"]["failed_units"] == ["n-3|embed_failure"]


@pytest.mark.dbos_integration
@pytest.mark.postgres
def test_real_dbos_partial_embed_writes_typed_outcome_and_doctor_is_non_green(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real DBOS SUCCESS row is degraded through the typed side channel.

    The DBOS workflow and SQL/doctor path are live; the embed batch body is a
    deterministic partial-result fixture so this regression isolates durable
    operational evidence from provider and graph-store variability.
    """
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSConfig

    from okg.substrate.runtime import embed_workflow, partition_queue
    from okg.substrate.runtime.partition_queue import (
        partition_queue_name,
        register_deployment_partition_queue,
    )
    from okg.substrate.runtime.workflow_operational_outcomes import (
        check_workflow_operational_outcomes,
    )

    deployment = "okg-workspace"
    dsn = clean_runtime
    with contextlib.suppress(Exception):
        DBOS.destroy()

    monkeypatch.setattr(
        embed_workflow,
        "_run_one_embed_batch",
        lambda **_kwargs: {
            "skipped": False,
            "any_failed": True,
            "processed": 2,
            "errors": 1,
            "embedder": "hash",
            "processed_node_ids": ["n-1", "n-2"],
            "failed_units": ["n-3|embed_failure"],
            "error_details": [{"node_id": "n-3", "error": "fixture failure"}],
        },
    )
    partition_queue._REGISTERED.clear()
    DBOS(config=DBOSConfig(
        name="okg-typed-outcome-test",
        system_database_url=dsn,
        database_url=dsn,
        run_admin_server=False,
        application_version="typed-outcome-test-v1",
    ))
    register_deployment_partition_queue(deployment=deployment)
    embed_workflow.register_embed_workflow(
        deployment=deployment,
        embedding_plan=_verified_hash_plan(),
    )
    DBOS.launch()
    try:
        workflow = embed_workflow._DECORATED[embed_workflow.EMBED_WORKFLOW_NAME]
        handle = DBOS.enqueue_workflow(partition_queue_name(deployment), workflow)
        result = handle.get_result()
        assert result["any_failed"] is True
        import psycopg

        with psycopg.connect(dsn) as conn:
            status = conn.execute(
                "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                (handle.workflow_id,),
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT deployment_name, workflow_name, queue_name, outcome, "
                "succeeded_count, failed_count FROM okg.workflow_operational_registrations "
                "JOIN okg.workflow_operational_outcomes USING (workflow_uuid) "
                "WHERE workflow_uuid = %s",
                (handle.workflow_id,),
            ).fetchall()
        assert status == "SUCCESS"
        assert rows == [
            (
                deployment,
                "okg_embed_batch",
                partition_queue_name(deployment),
                "partial_failure",
                2,
                1,
            ),
        ]
        verdict = check_workflow_operational_outcomes(
            deployment=deployment, dsn=dsn,
        )
        assert verdict.ok is False
        assert verdict.code == "workflow_outcome_degraded"
        assert verdict.partial_count == 1

        env = os.environ.copy()
        env.update({"OKG_DSN": dsn, "OKG_DEPLOYMENT": deployment})
        doctor = subprocess.run(
            [sys.executable, "-m", "okg.substrate.cli.main", "doctor",
             "--deployment", deployment, "--json"],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        payload = json.loads(doctor.stdout)
        assert doctor.returncode == 1
        assert payload["ok"] is False
        assert payload["workflow_operational_outcomes"]["ok"] is False
        assert payload["workflow_operational_outcomes"]["code"] == (
            "workflow_outcome_degraded"
        )
    finally:
        DBOS.destroy()


@pytest.mark.dbos_integration
@pytest.mark.postgres
def test_must_fail_a_vector_set_fill_failure_does_not_fail_the_embed_workflow(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review P7 S3 (PACT openrouter-embeddings-hnsw): the vector-set fill
    runs in the embed workflow after the node-column batch. A fill failure
    is recorded in the summary (`vector_sets_error`) and logged; the
    node-column batch's typed outcome is still recorded and the workflow
    ends SUCCESS. Review P8 S3: the outcome row's details carry the
    failure and `okg doctor` reports it (`vector_set_fill_failed`).
    Must-fail: the fill's exception escaped the workflow before the outcome
    was recorded, so the workflow ended ERROR with no outcome row; and
    (P8) the outcome was recorded before the fill, with no fill state."""
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSConfig

    from okg.substrate.runtime import embed_workflow, partition_queue
    from okg.substrate.runtime.partition_queue import (
        partition_queue_name,
        register_deployment_partition_queue,
    )

    # Its own deployment, so its partition queue is not one an earlier
    # DBOS test in this process already declared.
    deployment = "okg-fill-isolation"
    dsn = clean_runtime
    with contextlib.suppress(Exception):
        DBOS.destroy()

    monkeypatch.setattr(
        embed_workflow,
        "_run_one_embed_batch",
        lambda **_kwargs: {
            "skipped": False,
            "any_failed": False,
            "processed": 2,
            "errors": 0,
            "embedder": "hash",
            "processed_node_ids": ["n-1", "n-2"],
            "failed_units": [],
            "error_details": [],
        },
    )

    def broken_fill(**_kwargs):
        raise RuntimeError("fixture vector-set fill failure")

    monkeypatch.setattr(embed_workflow, "fill_vector_sets", broken_fill)
    partition_queue._REGISTERED.clear()
    DBOS(config=DBOSConfig(
        name="okg-fill-isolation-test",
        system_database_url=dsn,
        database_url=dsn,
        run_admin_server=False,
        application_version="fill-isolation-test-v1",
    ))
    register_deployment_partition_queue(deployment=deployment)
    embed_workflow.register_embed_workflow(
        deployment=deployment,
        embedding_plan=_verified_hash_plan(),
    )
    DBOS.launch()
    try:
        workflow = embed_workflow._DECORATED[embed_workflow.EMBED_WORKFLOW_NAME]
        handle = DBOS.enqueue_workflow(partition_queue_name(deployment), workflow)
        result = handle.get_result()
        assert result["processed"] == 2
        assert "vector_sets" not in result
        assert result["vector_sets_error"] == {
            "error": "RuntimeError",
            "code": None,
            "reason": "fixture vector-set fill failure",
        }
        import psycopg

        with psycopg.connect(dsn) as conn:
            status = conn.execute(
                "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                (handle.workflow_id,),
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT outcome, succeeded_count, failed_count "
                "FROM okg.workflow_operational_outcomes WHERE workflow_uuid = %s",
                (handle.workflow_id,),
            ).fetchall()
            details = conn.execute(
                "SELECT details FROM okg.workflow_operational_outcomes "
                "WHERE workflow_uuid = %s",
                (handle.workflow_id,),
            ).fetchone()[0]
        assert status == "SUCCESS"
        assert rows == [("clean", 2, 0)]
        # Review P8 S3: the failure persists beyond the log, in the outcome
        # row's details, and doctor flags it from there.
        assert details["vector_sets_error"] == {
            "error": "RuntimeError",
            "code": None,
            "reason": "fixture vector-set fill failure",
        }
        from okg.substrate.embeddings.doctor import vector_set_report

        with psycopg.connect(dsn, autocommit=True) as conn:
            report = vector_set_report(conn, deployment=deployment, config=None)
        (finding,) = [
            f for f in report["findings"] if f["code"] == "vector_set_fill_failed"
        ]
        assert finding["workflow"] == handle.workflow_id
        assert "fixture vector-set fill failure" in finding["message"]
        assert report["ok"] is False
    finally:
        DBOS.destroy()
