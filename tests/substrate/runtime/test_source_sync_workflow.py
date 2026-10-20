"""Tests for the source-sync DBOS workflow (Phase 2 §2.1).

Three layers:

  1. Registration: register_source_sync_workflow installs into the
     mock DBOS singleton.
  2. Accessor: source_sync_workflow() returns the decorated function;
     raises before registration.
  3. Re-registration: a second register call (worker restart in the
     same process) clears the stale cache.

Live integration (the workflow actually running against a real
deployment's source registry) is covered by the per-deployment
cutover gate (§5).
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest


@pytest.fixture(autouse=True)
def _dummy_dsn_takes_no_source_sync_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3003: an operator enqueue takes the source-sync signal on the
    database; the dummy DSN has none. The gating is proven in
    tests/substrate/test_publisher.py."""
    from contextlib import nullcontext

    from okg.substrate.runtime import source_sync_priority

    real_lock = source_sync_priority.source_sync_admission_intent_lock

    def lock(**kwargs: object) -> object:
        if kwargs["dsn"] == "postgres://unused":
            return nullcontext()
        return real_lock(**kwargs)

    monkeypatch.setattr(
        source_sync_priority, "source_sync_admission_intent_lock", lock,
    )


def test_register_source_sync_workflow_caches_handle():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.source_sync_workflow import (
            SOURCE_SYNC_WORKFLOW_NAME,
            _DECORATED,
            register_source_sync_workflow,
            source_sync_workflow,
        )

        register_source_sync_workflow(deployment="test")

        assert SOURCE_SYNC_WORKFLOW_NAME in _DECORATED
        assert callable(source_sync_workflow())


def test_source_sync_workflow_accessor_raises_when_not_registered():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.source_sync_workflow import (
            _DECORATED,
            source_sync_workflow,
        )
        _DECORATED.clear()

        with pytest.raises(RuntimeError, match="not registered"):
            source_sync_workflow()


def test_register_clears_stale_cache():
    """A second register call replaces the prior closure."""
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.source_sync_workflow import (
            _DECORATED,
            register_source_sync_workflow,
        )

        register_source_sync_workflow(deployment="alpha")
        first = _DECORATED[next(iter(_DECORATED))]

        register_source_sync_workflow(deployment="beta")
        second = _DECORATED[next(iter(_DECORATED))]

        assert first is not second


def test_workflow_name_is_stable():
    """The workflow name is used by `okg runtime debug-workflow` filters
    and by operator dashboards; pin it to catch accidental renames."""
    from okg.substrate.runtime.source_sync_workflow import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )
    assert SOURCE_SYNC_WORKFLOW_NAME == "okg_source_sync"


def test_admin_enqueue_helper_exists():
    """The CLI cutover (`okg ingest` enqueuing via DBOSClient) depends
    on `client_enqueue_source_sync` being exported from admin."""
    from okg.substrate.runtime import admin

    assert hasattr(admin, "client_enqueue_source_sync")
    assert "client_enqueue_source_sync" in admin.__all__


def test_source_sync_start_usefulness_requires_all_sources_superseded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import source_sync_workflow as workflow

    def fake_active(**_kwargs):
        return {
            "docs": {
                "workflow_id": "wf-newer",
                "candidate_workflow_ids": ["wf-current", "wf-newer"],
                "selection_reason": "preferred_fresh_completed_scope",
            },
            "notes": {
                "workflow_id": "wf-current",
                "candidate_workflow_ids": ["wf-current", "wf-newer"],
                "selection_reason": "preferred_fresh_completed_scope",
            },
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflows_by_source",
        fake_active,
    )

    out = workflow._source_sync_start_usefulness(
        dsn="postgres://unused",
        deployment="test",
        workflow_id="wf-current",
        sources=["docs", "notes"],
    )

    assert out["useful"] is True
    assert out["reason"] == "at_least_one_source_still_selects_workflow"
    decisions = {
        decision["source_name"]: decision
        for decision in out["decisions"]
    }
    assert decisions["docs"]["reason"] == "superseded_by_selected_frontier"
    assert decisions["notes"]["reason"] == "selected_frontier"


def test_source_sync_start_usefulness_marks_all_sources_superseded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import source_sync_workflow as workflow

    def fake_active(**_kwargs):
        return {
            "docs": {
                "workflow_id": "wf-newer",
                "candidate_workflow_ids": ["wf-current", "wf-newer"],
                "selection_reason": "preferred_fresh_completed_scope",
            },
            "notes": {
                "workflow_id": "wf-newer",
                "candidate_workflow_ids": ["wf-current", "wf-newer"],
                "selection_reason": "preferred_fresh_completed_scope",
            },
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflows_by_source",
        fake_active,
    )

    out = workflow._source_sync_start_usefulness(
        dsn="postgres://unused",
        deployment="test",
        workflow_id="wf-current",
        sources=["docs", "notes"],
    )

    assert out["useful"] is False
    assert out["reason"] == "all_sources_superseded_by_selected_frontiers"
    assert {
        decision["reason"] for decision in out["decisions"]
    } == {"superseded_by_selected_frontier"}


def test_source_sync_superseded_start_exits_before_ingest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from okg.substrate.runtime import source_sync_workflow as workflow

    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            SimpleNamespace(profiling=None, sync_root=None),
            "postgres://unused",
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_current_dbos_workflow_id",
        lambda: "wf-current",
    )
    monkeypatch.setattr(
        workflow,
        "_source_sync_start_usefulness",
        lambda **_kwargs: {
            "useful": False,
            "reason": "all_sources_superseded_by_selected_frontiers",
            "workflow_id": "wf-current",
        },
    )

    def fail_if_run_ingest(_options):
        raise AssertionError("superseded workflow should not scan sources")

    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        fail_if_run_ingest,
    )

    out = workflow._run_one_ingest(
        deployment="okg-workspace",
        sources=["pact"],
        run_mode="scope_complete",
        include_dependents=True,
        dependent_fanout=None,
        events=[],
        publish=True,
    )

    assert out["superseded"] is True
    assert out["any_failed"] is False
    assert out["report"]["status"] == "superseded"
    assert out["report"]["source_sync_usefulness"]["workflow_id"] == (
        "wf-current"
    )


@pytest.mark.parametrize("explicit", [None, False, True])
def test_source_sync_body_passes_whether_the_mode_was_typed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    explicit,
) -> None:
    """A typed `okg ingest --mode` reaches IngestOptions as typed (#2486);
    a workflow enqueued without the flag (including one in flight before it
    existed) runs as before, with the mode treated as not typed."""
    from okg.substrate.ingest.service import IngestRunResult
    from okg.substrate.runtime import source_sync_workflow as workflow

    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            SimpleNamespace(profiling=None, sync_root=None),
            "postgres://unused",
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_source_sync_start_usefulness",
        lambda **_kwargs: {"useful": True, "reason": "test", "workflow_id": None},
    )
    seen = []

    def fake_run_ingest(options):
        seen.append(options)
        return IngestRunResult(
            any_failed=False,
            report={"results": [{"source": "pact", "status": "OK"}], "publishes": []},
        )

    monkeypatch.setattr("okg.substrate.ingest.service.run_ingest", fake_run_ingest)
    extra = {} if explicit is None else {"run_mode_explicit": explicit}

    workflow._run_one_ingest(
        deployment="okg-workspace",
        sources=["pact"],
        run_mode="scope_complete",
        include_dependents=True,
        dependent_fanout=None,
        events=[],
        publish=True,
        **extra,
    )

    assert [(o.run_mode, o.run_mode_explicit) for o in seen] == [
        ("scope_complete", bool(explicit)),
    ]


def test_source_sync_enqueue_sends_the_typed_flag_only_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag travels as a keyword and only when True, so an ordinary
    enqueue carries exactly the arguments it carried before (#2486)."""
    import contextlib

    from okg.substrate.runtime import admin

    enqueued: list[tuple[tuple, dict]] = []

    class FakeClient:
        def enqueue(self, options, *args, **kwargs):
            enqueued.append((args, kwargs))
            return SimpleNamespace(workflow_id=f"wf-{len(enqueued)}")

    monkeypatch.setattr(admin, "_build_client", lambda dsn: FakeClient())
    monkeypatch.setattr(admin, "latest_registered_app_version", lambda dsn: None)
    monkeypatch.setattr(
        admin,
        "promotion_enqueue_permit",
        lambda **_kwargs: contextlib.nullcontext(),
    )

    for flag in (False, True):
        admin.client_enqueue_source_sync(
            deployment="d",
            dsn="postgres://unused",
            sources=["pact"],
            run_mode="reconcile",
            run_mode_explicit=flag,
            wait_for_result=False,
        )

    (plain_args, plain_kwargs), (typed_args, typed_kwargs) = enqueued
    assert plain_kwargs == {}
    assert typed_kwargs == {"run_mode_explicit": True}
    assert plain_args == typed_args
    assert plain_args[:2] == (["pact"], "reconcile")


def test_source_sync_publish_failure_is_workflow_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.ingest.service import IngestRunResult
    from okg.substrate.runtime import source_sync_workflow as workflow

    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            SimpleNamespace(profiling=None, sync_root=None),
            "postgres://unused",
            tmp_path,
        ),
    )
    def fake_run_ingest(_options):
        return IngestRunResult(
            any_failed=True,
            report={
                "results": [{"source": "pact", "status": "OK"}],
                "publishes": [{
                    "label": "after-layer-1",
                    "status": "fail",
                    "error": "gate timeout",
                }],
            },
        )

    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        fake_run_ingest,
    )
    caplog.set_level(logging.ERROR, logger=workflow.__name__)

    with pytest.raises(RuntimeError, match="publish failed"):
        workflow._run_one_ingest(
            deployment="okg-workspace",
            sources=["pact"],
            run_mode="scope_complete",
            include_dependents=True,
            dependent_fanout=None,
            events=[],
            publish=True,
        )

    assert "failed_sources=0/1 failed_publishes=1/1" in caplog.text
    assert "publish after-layer-1 status=fail: gate timeout" in caplog.text


def test_source_sync_runtime_payload_declares_dbos_authority() -> None:
    from okg.substrate.runtime import source_sync_workflow as workflow

    out = workflow._source_sync_runtime_payload(
        deployment="okg-workspace",
        workflow_id="wf-1",
        source_count=2,
        report={
            "results": [
                {"source": "docs", "status": "OK"},
                {"source": "pact", "status": "OK"},
            ],
            "publishes": [{"status": "published"}],
        },
    )

    assert out == {
        "schema": "okg.source_sync.runtime.v1",
        "status": "completed",
        "deployment": "okg-workspace",
        "dbos_workflow_id": "wf-1",
        "authority": "dbos.workflow_status",
        "source_progress_authority": (
            "okg.source_sync_progress_and_watermarks"
        ),
        "source_count": 2,
        "result_count": 2,
        "publish_count": 1,
    }


def test_source_sync_workflow_success_does_not_write_job_runtime_ledger(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from okg.substrate.ingest.service import IngestRunResult
    from okg.substrate.runtime import source_sync_workflow as workflow

    sync_root = tmp_path / "registered-sync-root"
    sync_root.mkdir()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            SimpleNamespace(profiling=None, sync_root=sync_root),
            "postgres://unused",
            tmp_path,
        ),
    )

    def fake_run_ingest(options):
        captured["options"] = options
        return IngestRunResult(
            any_failed=False,
            report={
                "results": [{"source": "pact", "status": "OK"}],
                "publishes": [{"status": "published"}],
            },
        )

    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        fake_run_ingest,
    )

    def fail_if_ledger_connects(*_args, **_kwargs):
        raise AssertionError("source-sync must not write okg.job_* rows")

    monkeypatch.setattr(psycopg, "connect", fail_if_ledger_connects)

    out = workflow._run_one_ingest(
        deployment="okg-workspace",
        sources=["pact"],
        run_mode="scope_complete",
        include_dependents=True,
        dependent_fanout=None,
        events=[],
        publish=True,
    )

    assert out["source_sync_runtime"]["authority"] == "dbos.workflow_status"
    assert "okg_job_runtime_ledger" not in out["source_sync_runtime"]
    assert "job_run_id" not in out["source_sync_runtime"]
    assert (
        captured["options"].source_registry_repo_root_override
        == sync_root
    )
    assert captured["options"].include == ("pact",)
    assert captured["options"].require_declared_sources is True


@pytest.mark.parametrize("context_state", ["cold", "cache_hit"])
def test_source_sync_refuses_missing_root_before_worker_registry_or_ingest(
    context_state: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from okg.substrate.errors import SourceConfigurationAuthorityInvalid
    from okg.substrate.runtime import source_sync_workflow as workflow

    deployment = f"missing-root-{context_state}"
    deployment_dir = tmp_path / deployment
    deployment_dir.mkdir()
    manifest_path = deployment_dir / "deployment.yaml"
    manifest_path.write_text(f"name: {deployment}\n", encoding="utf-8")
    manifest = SimpleNamespace(
        profiling=None,
        sync_root=tmp_path / "foreign-host-root",
        sync_root_unresolved=(),
    )
    workflow._CTX_CACHE.clear()
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )
    if context_state == "cold":
        monkeypatch.setattr(
            "okg.substrate.deployments.resolve_deployment",
            lambda _deployment: manifest,
        )
        monkeypatch.setattr(
            "okg.substrate.deployments.require_postgres_dsn",
            lambda _manifest, *, deployment: "postgres://unused",
        )
        monkeypatch.setattr(
            "okg.substrate.deployments.find_deployment_dir",
            lambda _deployment: deployment_dir,
        )
    else:
        workflow._CTX_CACHE[deployment] = (
            manifest_path.stat().st_mtime_ns,
            manifest,
            "postgres://unused",
            deployment_dir,
        )
    monkeypatch.setattr(
        workflow,
        "_source_sync_start_usefulness",
        lambda **_kwargs: pytest.fail("workflow read source state before guard"),
    )
    monkeypatch.setattr(
        workflow,
        "_source_sync_all_registry_sources",
        lambda *_args, **_kwargs: pytest.fail(
            "worker admitted registry before source-root authority",
        ),
    )

    with pytest.raises(SourceConfigurationAuthorityInvalid):
        workflow._run_one_ingest(
            deployment=deployment,
            sources=None,
            run_mode="scope_complete",
            include_dependents=False,
            dependent_fanout=None,
            events=[],
            publish=True,
        )

    workflow._CTX_CACHE.clear()


def test_source_sync_silent_failed_result_is_workflow_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from okg.substrate.ingest.service import IngestRunResult
    from okg.substrate.runtime import source_sync_workflow as workflow

    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            SimpleNamespace(profiling=None, sync_root=None),
            "postgres://unused",
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        lambda _options: IngestRunResult(
            any_failed=True,
            report={
                "results": [{"source": "pact", "status": "OK"}],
                "publishes": [{
                    "label": "after-layer-1",
                    "status": "skipped",
                    "publish_skipped": True,
                    "reason": "no_source_work",
                }],
            },
        ),
    )

    with pytest.raises(RuntimeError, match="no failed source or publish"):
        workflow._run_one_ingest(
            deployment="okg-workspace",
            sources=["pact"],
            run_mode="scope_complete",
            include_dependents=True,
            dependent_fanout=None,
            events=[],
            publish=True,
        )


@pytest.mark.dbos_integration
@pytest.mark.postgres
def test_source_sync_dbos_client_dispatches_durable_registered_workflow(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.source-sync test.runtime-authority.source-sync",
    )
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSConfig

    from okg.substrate.runtime import partition_queue
    from okg.substrate.runtime import source_sync_workflow as workflow
    from okg.substrate.runtime.engine import defensive_destroy
    from okg.substrate.runtime.partition_queue import (
        register_source_sync_partition_queue,
    )
    from okg.substrate.runtime.services import source_sync_replay_boundary
    from okg.substrate.runtime.source_sync_constants import (
        SOURCE_SYNC_APPLICATION_VERSION,
    )
    from okg.substrate.source_sync import SourceSyncJob
    from okg.substrate.source_sync_enqueue import enqueue_source_sync_via_dbos

    defensive_destroy()
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})
    workflow._DECORATED.clear()

    calls: list[dict] = []

    def fake_run_one_ingest(**kwargs):
        calls.append(kwargs)
        return {
            "all_failed": False,
            "any_failed": False,
            "superseded": False,
            "malformed_event_units": [],
            "report": {
                "status": "stubbed",
                "sources": kwargs.get("sources"),
                "results": [
                    {"name": source, "status": "OK"}
                    for source in (kwargs.get("sources") or ())
                ],
                "publishes": [],
            },
            "source_sync_runtime": {"status": "completed"},
        }

    monkeypatch.setattr(workflow, "_run_one_ingest", fake_run_one_ingest)

    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        def dbos_config():
            return DBOSConfig(
                name="okg-source-sync-dispatch-test",
                system_database_url=clean_runtime,
                database_url=clean_runtime,
                run_admin_server=False,
                application_version=SOURCE_SYNC_APPLICATION_VERSION,
            )

        def launch_with_replay_boundary():
            with source_sync_replay_boundary(
                dsn=clean_runtime,
                deployment="test",
                live_version=SOURCE_SYNC_APPLICATION_VERSION,
            ) as replay_boundary:
                DBOS.launch()
                return replay_boundary

        DBOS(config=dbos_config())
        register_source_sync_partition_queue(deployment="test")
        workflow.register_source_sync_workflow(deployment="test")
        first_launch_boundary = launch_with_replay_boundary()
        assert first_launch_boundary["schema_initialized"] is False
        assert first_launch_boundary["admission_lock_held"] is False

        # Execute the actual pre-v2 two-step topology in DBOS: the old worker
        # captured observed_at and recorded setup completion, then returned its
        # workflow result without a durable ingest-cycle step. Retain those
        # real operation outputs while making the row look like a crash before
        # terminal persistence.
        from dbos import DBOS

        @DBOS.step(name="okg_source_sync_observed_at")
        def legacy_capture_observed_at() -> str:
            return "legacy-observed-at"

        @DBOS.step(name="okg_source_sync_setup_done")
        def legacy_record_setup_done() -> str:
            return "legacy-setup-done"

        @DBOS.workflow(
            name="okg_source_sync",
            max_recovery_attempts=10,
        )
        def legacy_source_sync_workflow(*_args, **_kwargs) -> dict[str, str]:
            return {
                "observed_at": legacy_capture_observed_at(),
                "setup": legacy_record_setup_done(),
            }

        legacy_workflow_id = enqueue_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=SourceSyncJob(
                deployment="test",
                sources=("docs",),
                run_mode="scope_complete",
            ),
            deduplication_id=(
                "sync:test:dispatch:scope_complete:docs:none:legacy"
            ),
        )

        status = _wait_for_dbos_terminal_status(
            clean_runtime,
            legacy_workflow_id,
        )

        assert status == "SUCCESS"
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT function_name
                  FROM dbos.operation_outputs
                 WHERE workflow_uuid = %s
                 ORDER BY function_id
                """,
                (legacy_workflow_id,),
            )
            assert [row[0] for row in cur.fetchall()] == [
                "okg_source_sync_observed_at",
                "okg_source_sync_setup_done",
            ]

        # Simulate a retained old-shape row before the replacement worker
        # launches. The old row has no ingest output and must be cancelled by
        # the replay boundary, never replayed under the v2 topology.
        DBOS.destroy()
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dbos.workflow_status
                   SET status = 'PENDING',
                       application_version = 'okg-runtime-source-sync-v1',
                       output = NULL,
                       error = NULL,
                       executor_id = 'local',
                       recovery_attempts = 0,
                       completed_at = NULL
                 WHERE workflow_uuid = %s
                """,
                (legacy_workflow_id,),
            )
            conn.commit()

        DBOS(config=dbos_config())
        register_source_sync_partition_queue(deployment="test")
        workflow.register_source_sync_workflow(deployment="test")
        old_shape_boundary = launch_with_replay_boundary()
        assert old_shape_boundary["schema_initialized"] is True
        assert old_shape_boundary["admission_lock_held"] is True
        old_shape_status = _wait_for_dbos_terminal_status(
            clean_runtime,
            legacy_workflow_id,
        )

        assert old_shape_status == "CANCELLED"
        assert len(calls) == 0

        # A new v2 workflow still runs after the old-shape row is cancelled.
        current_workflow_id = enqueue_source_sync_via_dbos(
            deployment="test",
            dsn=clean_runtime,
            job=SourceSyncJob(
                deployment="test",
                sources=("docs",),
                run_mode="scope_complete",
            ),
            deduplication_id=(
                "sync:test:dispatch:scope_complete:docs:none:current"
            ),
        )
        current_status = _wait_for_dbos_terminal_status(
            clean_runtime,
            current_workflow_id,
        )
        assert current_status == "SUCCESS"
        assert len(calls) == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT application_version
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = %s
                """,
                (current_workflow_id,),
            )
            assert cur.fetchone() == (SOURCE_SYNC_APPLICATION_VERSION,)
            cur.execute(
                """
                SELECT count(*)::int
                  FROM dbos.operation_outputs
                 WHERE workflow_uuid = %s
                   AND function_name = 'okg_source_sync_ingest_cycle'
                """,
                (current_workflow_id,),
            )
            assert cur.fetchone() == (1,)
            cur.execute(
                """
                SELECT r.deployment_name, r.workflow_name, r.queue_name,
                       o.outcome, o.succeeded_count, o.failed_count
                  FROM okg.workflow_operational_registrations r
                  JOIN okg.workflow_operational_outcomes o USING (workflow_uuid)
                 WHERE r.workflow_uuid = %s
                """,
                (current_workflow_id,),
            )
            assert cur.fetchone() == (
                "test",
                "okg_source_sync",
                "okg-test-source-sync",
                "clean",
                1,
                0,
            )

        # Simulate the exact crash boundary this PACT protects: the durable
        # ingest step committed, but the process died before DBOS persisted the
        # workflow's terminal result. A fresh DBOS singleton must recover the
        # current-version workflow and reuse operation_outputs instead of
        # calling the heavy ingest body a second time.
        DBOS.destroy()
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dbos.workflow_status
                   SET status = 'PENDING',
                       output = NULL,
                       error = NULL,
                       executor_id = 'local',
                       recovery_attempts = 0,
                       completed_at = NULL
                 WHERE workflow_uuid = %s
                """,
                (current_workflow_id,),
            )
            conn.commit()

        DBOS(config=dbos_config())
        register_source_sync_partition_queue(deployment="test")
        workflow.register_source_sync_workflow(deployment="test")
        launch_with_replay_boundary()
        recovered_status = _wait_for_dbos_terminal_status(
            clean_runtime,
            current_workflow_id,
        )

        assert recovered_status == "SUCCESS"
        assert len(calls) == 1
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)::int
                  FROM dbos.operation_outputs
                 WHERE workflow_uuid = %s
                   AND function_name = 'okg_source_sync_ingest_cycle'
                """,
                (current_workflow_id,),
            )
            assert cur.fetchone() == (1,)
    finally:
        try:
            DBOS.destroy()
        except Exception:  # noqa: BLE001
            pass
        workflow._DECORATED.clear()


def _wait_for_dbos_terminal_status(
    dsn: str,
    workflow_id: str,
    *,
    timeout_seconds: float = 20.0,
) -> str | None:
    deadline = time.monotonic() + timeout_seconds
    terminal = {
        "SUCCESS",
        "ERROR",
        "CANCELLED",
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    }
    status: str | None = None
    while time.monotonic() < deadline:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                  FROM dbos.workflow_status
                 WHERE workflow_uuid = %s
                """,
                (workflow_id,),
            )
            row = cur.fetchone()
        status = str(row[0]) if row else None
        if status in terminal:
            return status
        time.sleep(0.1)
    return status


@pytest.mark.parametrize("explicit", [None, True])
def test_the_registered_workflow_hands_the_typed_flag_to_the_ingest(
    monkeypatch: pytest.MonkeyPatch,
    explicit,
) -> None:
    """The DBOS workflow carries `run_mode_explicit` through its durable
    ingest step (#2486); a call without it (a workflow enqueued before the
    argument existed) runs with the mode treated as not typed."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime import workflow_operational_outcomes

    monkeypatch.setattr(
        workflow_operational_outcomes,
        "define_operational_transactions",
        lambda **_kwargs: SimpleNamespace(
            register_execution_tx=lambda: None,
            record_outcome_tx=lambda _outcome: None,
        ),
    )
    with mock_dbos_context():
        from okg.substrate.runtime import source_sync_workflow as workflow

        seen: list[dict] = []

        def fake_run_one_ingest(**kwargs):
            seen.append(kwargs)
            return {"all_failed": False, "any_failed": False, "report": {}}

        monkeypatch.setattr(workflow, "_run_one_ingest", fake_run_one_ingest)
        monkeypatch.setattr(
            workflow, "_source_sync_operational_outcome", lambda result: result,
        )
        workflow.register_source_sync_workflow(deployment="test")
        extra = {} if explicit is None else {"run_mode_explicit": explicit}
        workflow.source_sync_workflow()(["pact"], "scope_complete", **extra)

    assert [(c["run_mode"], c["run_mode_explicit"]) for c in seen] == [
        ("scope_complete", bool(explicit)),
    ]
