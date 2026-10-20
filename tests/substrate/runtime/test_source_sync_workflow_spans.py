"""Runtime proof for source-sync workflow phase spans."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import time
from types import SimpleNamespace

import psycopg
import pytest
import yaml


def _trace_handoff(*, duration_ms: int = 250) -> dict[str, str]:
    started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    ended_at = started_at + timedelta(milliseconds=duration_ms)
    return {
        "schema": "okg.source_sync.trace.v1",
        "trace_id": "1" * 32,
        "root_span_id": "2" * 16,
        "wait_span_id": "3" * 16,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
    }


@pytest.mark.postgres
def test_source_sync_admission_wait_is_recovery_idempotent(
    clean_runtime: str,
) -> None:
    from okg.substrate.runtime.workflow_telemetry import dbos_workflow_trace

    workflow_id = "wf-source-sync-recovery-idempotence"
    handoff = _trace_handoff()
    for _attempt in range(2):
        with dbos_workflow_trace(
            dsn=clean_runtime,
            deployment_name="svc",
            workflow_name="okg_source_sync",
            workflow_id=workflow_id,
            source_sync_trace_handoff=handoff,
        ):
            pass

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT span_name, trace_id, span_id, parent_span_id,
                   started_at, ended_at
              FROM okg.telemetry_spans
             WHERE deployment_name = 'svc'
               AND workflow_id = %s
               AND span_name IN (
                 'dbos.workflow.okg_source_sync',
                 'source_sync.admission_lock.wait'
               )
             ORDER BY span_name
            """,
            (workflow_id,),
        )
        rows = cur.fetchall()

    assert len(rows) == 2
    by_name = {row[0]: row for row in rows}
    root = by_name["dbos.workflow.okg_source_sync"]
    wait = by_name["source_sync.admission_lock.wait"]
    assert root[1:3] == (handoff["trace_id"], handoff["root_span_id"])
    assert wait[1:4] == (
        handoff["trace_id"],
        handoff["wait_span_id"],
        handoff["root_span_id"],
    )
    assert root[4] <= wait[4] <= wait[5]


@pytest.mark.postgres
def test_legacy_source_sync_trace_marks_admission_wait_unavailable(
    clean_runtime: str,
) -> None:
    from okg.substrate.runtime.workflow_telemetry import dbos_workflow_trace

    workflow_id = "wf-source-sync-legacy-input"
    with dbos_workflow_trace(
        dsn=clean_runtime,
        deployment_name="svc",
        workflow_name="okg_source_sync",
        workflow_id=workflow_id,
    ):
        pass

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT attributes, unavailable_reason
              FROM okg.telemetry_spans
             WHERE deployment_name = 'svc'
               AND workflow_id = %s
               AND span_name = 'dbos.workflow.okg_source_sync'
            """,
            (workflow_id,),
        )
        row = cur.fetchone()
        cur.execute(
            """
            SELECT count(*)
              FROM okg.telemetry_spans
             WHERE deployment_name = 'svc'
               AND workflow_id = %s
               AND span_name = 'source_sync.admission_lock.wait'
            """,
            (workflow_id,),
        )
        wait_count = cur.fetchone()[0]

    assert row is not None
    assert row[0]["unavailable"]["source_sync.admission_lock.wait"] == (
        "source_sync_trace_handoff_not_available_legacy_input"
    )
    assert wait_count == 0


@pytest.mark.postgres
def test_superseded_source_sync_still_records_admission_wait(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import source_sync_workflow as workflow

    workflow_id = "wf-source-sync-superseded"
    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda _deployment: (
            SimpleNamespace(profiling=None, sync_root=None),
            clean_runtime,
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_current_dbos_workflow_id",
        lambda: workflow_id,
    )
    monkeypatch.setattr(
        workflow,
        "_source_sync_start_usefulness",
        lambda **_kwargs: {
            "useful": False,
            "reason": "all_sources_superseded_by_selected_frontiers",
        },
    )

    result = workflow._run_one_ingest(
        deployment="svc",
        sources=["docs"],
        run_mode="scope_complete",
        include_dependents=False,
        dependent_fanout="none",
        events=[],
        publish=True,
        workflow_telemetry=_trace_handoff(),
    )

    assert result["superseded"] is True
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT span_name, count(*)
              FROM okg.telemetry_spans
             WHERE deployment_name = 'svc'
               AND workflow_id = %s
             GROUP BY span_name
            """,
            (workflow_id,),
        )
        counts = dict(cur.fetchall())
    assert counts["dbos.workflow.okg_source_sync"] == 1
    assert counts["source_sync.admission_lock.wait"] == 1


@pytest.mark.postgres
def test_source_sync_phase_write_failure_warns_without_changing_result(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.runtime import workflow_telemetry
    from okg.substrate.telemetry import use_trace

    def fail_record(*_args, **_kwargs):
        raise RuntimeError("telemetry unavailable")

    monkeypatch.setattr(workflow_telemetry, "record_span", fail_record)
    caplog.set_level(logging.WARNING)

    with use_trace("1" * 32, span_id="2" * 16):
        with workflow_telemetry.source_sync_publish_span(
            dsn=clean_runtime,
            deployment_name="svc",
            workflow_id="wf-warning",
        ) as phase:
            result = {"status": "published", "generation_id": "1"}
            phase.observe_result(result)

    assert result["status"] == "published"
    assert "failed to record source-sync publish phase telemetry" in caplog.text


@pytest.mark.dbos_integration
@pytest.mark.postgres
def test_source_sync_workflow_phase_spans(
    okg_workspace_isolated_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    """A real DBOS source sync records contended wait and publish attempts."""
    record_property(
        "requirements",
        "req.issue-877.source-sync-phase-spans test.issue-877.real-source-sync-trace",
    )
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSConfig

    clean_runtime = okg_workspace_isolated_runtime

    from okg.substrate.runtime import partition_queue
    from okg.substrate.runtime import source_sync_workflow as workflow
    from okg.substrate.runtime.engine import defensive_destroy
    from okg.substrate.runtime.partition_queue import (
        register_source_sync_partition_queue,
    )
    from okg.substrate.runtime.source_sync_constants import (
        SOURCE_SYNC_APPLICATION_VERSION,
    )
    from okg.substrate.runtime.source_sync_priority import (
        source_sync_admission_intent_advisory_keys,
    )
    from okg.substrate.source_sync import SourceSyncJob
    from okg.substrate.source_sync_enqueue import enqueue_source_sync_via_dbos

    deployment_dir = _write_fixture_deployment(
        tmp_path,
        dsn=clean_runtime,
        blocked=True,
    )
    _claim_fixture_catalog(clean_runtime, deployment_dir)
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR",
        str(deployment_dir.parent),
    )
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})
    workflow._CTX_CACHE.clear()
    workflow._DECORATED.clear()
    defensive_destroy()

    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        DBOS(
            config=DBOSConfig(
                name="issue-877-source-sync-spans",
                system_database_url=clean_runtime,
                database_url=clean_runtime,
                run_admin_server=False,
                application_version=SOURCE_SYNC_APPLICATION_VERSION,
            )
        )
        register_source_sync_partition_queue(deployment="svc")
        workflow.register_source_sync_workflow(deployment="svc")
        DBOS.launch()

        classid, objid = source_sync_admission_intent_advisory_keys("svc")
        lock_conn = psycopg.connect(clean_runtime, autocommit=True)
        try:
            lock_conn.execute(
                "SELECT pg_advisory_lock(%s, %s)",
                (classid, objid),
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                enqueue_future = executor.submit(
                    enqueue_source_sync_via_dbos,
                    deployment="svc",
                    dsn=clean_runtime,
                    job=SourceSyncJob(
                        deployment="svc",
                        sources=("fixture",),
                        run_mode="scope_complete",
                        include_dependents=False,
                    ),
                    deduplication_id="issue-877-blocked",
                )
                # Time the hold from the moment the enqueue is seen waiting
                # on the lock, not from submit: on a loaded runner thread
                # start-up and the publisher check before the wait took
                # over 100 ms, and the measured wait came out at 242 ms.
                _wait_until_advisory_lock_is_awaited(
                    lock_conn, classid, objid, enqueue_future,
                )
                time.sleep(0.35)
                lock_released_at = datetime.now(timezone.utc)
                lock_conn.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (classid, objid),
                )
                blocked_workflow_id = enqueue_future.result(timeout=10)
        finally:
            lock_conn.close()

        assert (
            _wait_for_terminal_status(
                clean_runtime,
                blocked_workflow_id,
            )
            == "ERROR"
        )

        _set_fixture_publish_blocked(deployment_dir, blocked=False)
        _claim_fixture_catalog(clean_runtime, deployment_dir)
        workflow._CTX_CACHE.clear()
        successful_workflow_id = enqueue_source_sync_via_dbos(
            deployment="svc",
            dsn=clean_runtime,
            job=SourceSyncJob(
                deployment="svc",
                sources=("fixture",),
                run_mode="scope_complete",
                include_dependents=False,
            ),
            deduplication_id="issue-877-success",
        )
        assert (
            _wait_for_terminal_status(
                clean_runtime,
                successful_workflow_id,
            )
            == "SUCCESS"
        )

        trace_rows = _source_sync_trace_rows(
            clean_runtime,
            [blocked_workflow_id, successful_workflow_id],
        )
        by_workflow: dict[str, list[dict[str, object]]] = {}
        for row in trace_rows:
            by_workflow.setdefault(str(row["workflow_id"]), []).append(row)

        blocked = by_workflow[blocked_workflow_id]
        successful = by_workflow[successful_workflow_id]
        _assert_complete_phase_trace(blocked, publish_status="error")
        _assert_complete_phase_trace(successful, publish_status="ok")
        blocked_wait = _one_span(
            blocked,
            "source_sync.admission_lock.wait",
        )
        assert blocked_wait["duration_ms"] >= 250
        assert blocked_wait["started_at"] <= lock_released_at
        assert blocked_wait["ended_at"] >= lock_released_at

        blocked_publish = _one_span(blocked, "source_sync.publish.commit")
        successful_publish = _one_span(
            successful,
            "source_sync.publish.commit",
        )
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT created_at
                  FROM okg.consistency_gate_results
                 WHERE gate_name = 'fixture_blocking_gate'
                   AND blocks_publish
                 ORDER BY created_at DESC
                 LIMIT 1
                """
            )
            blocked_gate = cur.fetchone()
            assert blocked_gate is not None
            assert blocked_publish["started_at"] <= blocked_gate[0]
            assert blocked_publish["ended_at"] >= blocked_gate[0]

            from okg.substrate.graph_store import ChronosGraphStore

            generations = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).list_generations(deployment_name="svc")
        published = [
            generation
            for generation in generations
            if generation.metadata.get("status") == "published"
        ]
        assert len(published) == 1
        published_at = datetime.fromisoformat(published[0].created_at)
        assert successful_publish["started_at"] <= published_at
        assert successful_publish["ended_at"] >= published_at

        _write_trace_graph_report(
            blocked_workflow_id=blocked_workflow_id,
            successful_workflow_id=successful_workflow_id,
            trace_rows=trace_rows,
        )
    finally:
        try:
            DBOS.destroy()
        except Exception:  # noqa: BLE001
            pass
        workflow._CTX_CACHE.clear()
        workflow._DECORATED.clear()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


def _write_fixture_deployment(
    tmp_path: Path,
    *,
    dsn: str,
    blocked: bool,
) -> Path:
    from tests.pytest.catalog_fixture_builders import seed_catalog

    deployment_dir = tmp_path / "deployments" / "svc"
    deployment_dir.mkdir(parents=True)
    schemas_dir = deployment_dir / "schemas"
    schemas_dir.mkdir()
    (schemas_dir / "fixture.yaml").write_text(
        "id: https://example.invalid/okg/issue-877\n"
        "name: issue_877_fixture\n"
        "classes:\n"
        "  Document:\n"
        "    attributes:\n"
        "      text: {}\n"
        "    annotations:\n"
        "      archetype: artifact\n"
        "      transient: 'false'\n"
    )
    fixture_path = deployment_dir / "fixture.json"
    fixture_path.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "svc:issue-877",
                        "subtype": "document",
                        "attrs": {"text": "source-sync span proof"},
                        "source_record_id": {"path": "fixture.json"},
                    }
                ],
                "edges": [],
            }
        )
    )
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "fixture": {
                        "ownership_id": "svc.fixture",
                        "source_class": "discovery_crawl",
                        "record_identity_kind": "scoped_locator",
                        "record_identity_fields": ["id"],
                        "source_revision_kind": "content_hash",
                        "deletion_semantics": "missing_from_completed_scope",
                        "publication_mode": "published_generation",
                        "admission_policy": {
                            "producer_id": "svc.fixture",
                            "producer_kind": "source",
                            "trust_label": "fixture-trusted",
                            "admission_mode": "fast_track",
                            "output_scope_summary": {
                                "summary": "issue 877 fixture facts",
                            },
                        },
                        "module": "okg.substrate.library.sources.bootstrap_fixture",
                        "class": "BootstrapFixtureSource",
                        "params": {"fixture_path": str(fixture_path)},
                        "source_policy": {
                            "sensitivity": "local_private",
                            "data_classification": "private",
                            "credential_ref_policy": "none",
                            "live_call_allowed": False,
                            "exportability": "no_export",
                            "retention": "normal",
                            "provenance": "local_filesystem",
                            "privacy_obligations": ["audit"],
                            "store_raw": True,
                        },
                        "sync": {
                            "triggers": ["manual", "reconcile"],
                            "default_event_mode": "scope_complete",
                            "reconcile_mode": "scope_complete",
                        },
                    },
                },
            }
        )
    )
    _write_deployment_yaml(deployment_dir, dsn=dsn, blocked=blocked)
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    return deployment_dir


def _write_deployment_yaml(
    deployment_dir: Path,
    *,
    dsn: str,
    blocked: bool,
) -> None:
    manifest: dict[str, object] = {
        "name": "svc",
        "description": "source-sync phase span fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "sync_root": str(deployment_dir),
        "postgres": {"dsn": dsn},
        "modules": [],
        # The fixture lives in a temporary directory outside any Git
        # repository, so it declares itself standalone; without this the
        # publish refuses because release authority cannot be resolved
        # (issue #2127).
        "release": {"channel": "dev", "kind": "standalone"},
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["no_export"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }
    if blocked:
        manifest["consistency_gates"] = [_blocking_consistency_gate()]
    path = deployment_dir / "deployment.yaml"
    path.write_text(yaml.safe_dump(manifest))
    os.utime(path, None)


def _set_fixture_publish_blocked(
    deployment_dir: Path,
    *,
    blocked: bool,
) -> None:
    manifest_path = deployment_dir / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    dsn = str(manifest["postgres"]["dsn"])
    time.sleep(0.01)
    _write_deployment_yaml(deployment_dir, dsn=dsn, blocked=blocked)


def _blocking_consistency_gate() -> dict[str, object]:
    return {
        "name": "fixture_blocking_gate",
        "type": "deterministic_sql",
        "check_id": "test.issue-877.fixture-blocking-gate",
        "predicate_family": "fixture_block",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": """
            SELECT
              'scope:fixture'::text AS input_scope,
              'block:fixture'::text AS blocking_rule_hash,
              jsonb_build_object('source', 'svc.fixture')
                AS participant_roles,
              'fixture_publish_block'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              'fixture_publish_block'::text AS normalized_predicate,
              jsonb_build_object('blocked', true) AS normalized_values,
              'high'::text AS severity,
              1.0::numeric AS confidence,
              '{}'::jsonb AS evidence
        """,
        "coverage_sql": """
            SELECT
              'scope:fixture'::text AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              1::bigint AS eligible_input_count,
              1::bigint AS evaluator_invocation_count,
              1::bigint AS all_pairs_upper_bound
        """,
        "timeout_ms": 3000,
    }


def _claim_fixture_catalog(dsn: str, deployment_dir: Path) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )

    composed = compose_catalog(deployment_dir)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=deployment_dir,
        command="test issue 877 source-sync spans",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        apply_composed_catalog(conn, composed, identity=identity)


def _wait_until_advisory_lock_is_awaited(
    conn: psycopg.Connection,
    classid: int,
    objid: int,
    future,
    *,
    timeout: float = 30.0,
) -> None:
    """Return once another session waits on the two-key advisory lock
    ``(classid, objid)`` that ``conn`` holds (a not-granted row in
    ``pg_locks``). Fails if ``future`` ends first or the wait never shows."""
    # pg_locks shows each signed int4 key as its unsigned oid.
    keys = (classid & 0xFFFFFFFF, objid & 0xFFFFFFFF)
    deadline = time.monotonic() + timeout
    while True:
        row = conn.execute(
            """
            SELECT count(*)
              FROM pg_locks
             WHERE locktype = 'advisory'
               AND objsubid = 2
               AND classid::text::bigint = %s
               AND objid::text::bigint = %s
               AND NOT granted
               AND pid <> pg_backend_pid()
            """,
            keys,
        ).fetchone()
        if row[0]:
            return
        if future.done():
            future.result()
            raise AssertionError("the enqueue ended without waiting on the admission lock")
        assert time.monotonic() < deadline, "the enqueue never waited on the admission lock"
        time.sleep(0.01)


def _wait_for_terminal_status(
    dsn: str,
    workflow_id: str,
    *,
    timeout_seconds: float = 30.0,
) -> str | None:
    terminal = {
        "SUCCESS",
        "ERROR",
        "CANCELLED",
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    }
    deadline = time.monotonic() + timeout_seconds
    status: str | None = None
    while time.monotonic() < deadline:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                (workflow_id,),
            )
            row = cur.fetchone()
        status = str(row[0]) if row else None
        if status in terminal:
            return status
        time.sleep(0.1)
    return status


def _source_sync_trace_rows(
    dsn: str,
    workflow_ids: list[str],
) -> list[dict[str, object]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_id, span_name, trace_id, span_id, parent_span_id,
                   status_code, status_message, started_at, ended_at,
                   duration_ms
              FROM okg.telemetry_spans
             WHERE deployment_name = 'svc'
               AND workflow_id = ANY(%s)
               AND span_name IN (
                 'dbos.workflow.okg_source_sync',
                 'source_sync.admission_lock.wait',
                 'source_sync.publish.commit'
               )
             ORDER BY workflow_id, started_at, span_name
            """,
            (workflow_ids,),
        )
        columns = [column.name for column in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def _assert_complete_phase_trace(
    rows: list[dict[str, object]],
    *,
    publish_status: str,
) -> None:
    names = [str(row["span_name"]) for row in rows]
    assert names.count("dbos.workflow.okg_source_sync") == 1
    assert names.count("source_sync.admission_lock.wait") == 1
    assert names.count("source_sync.publish.commit") == 1
    root = _one_span(rows, "dbos.workflow.okg_source_sync")
    wait = _one_span(rows, "source_sync.admission_lock.wait")
    publish = _one_span(rows, "source_sync.publish.commit")
    assert wait["trace_id"] == root["trace_id"] == publish["trace_id"]
    assert wait["parent_span_id"] == root["span_id"]
    assert publish["parent_span_id"] == root["span_id"]
    assert root["started_at"] <= wait["started_at"]
    assert publish["status_code"] == publish_status
    if publish_status == "error":
        assert publish["status_message"]
        assert len(str(publish["status_message"])) <= 512


def _one_span(
    rows: list[dict[str, object]],
    span_name: str,
) -> dict[str, object]:
    matches = [row for row in rows if row["span_name"] == span_name]
    assert len(matches) == 1
    return matches[0]


def _write_trace_graph_report(
    *,
    blocked_workflow_id: str,
    successful_workflow_id: str,
    trace_rows: list[dict[str, object]],
) -> None:
    report_path = Path(".scratch/issue-877-trace-report.yaml")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    serializable_rows = [
        {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in row.items()
        }
        for row in trace_rows
    ]
    report_path.write_text(
        yaml.safe_dump(
            {
                "evidence": [
                    {
                        "kind": "graph_report",
                        "status": "pass",
                        "refs": [
                            "req.issue-877.source-sync-phase-spans",
                            "task.issue-877.emit-and-prove-phase-spans",
                            "test.issue-877.real-source-sync-trace",
                        ],
                        "summary": (
                            "Two real DBOS source-sync workflows emitted one admission "
                            "wait and one publish span each; blocked then published"
                        ),
                        "command": (
                            "SELECT workflow_id, span_name, trace_id, span_id, "
                            "parent_span_id, status_code, started_at, ended_at, "
                            "duration_ms FROM okg.telemetry_spans WHERE "
                            "workflow_id = ANY(<verified-workflow-ids>)"
                        ),
                        "data": {
                            "blocked_workflow_id": blocked_workflow_id,
                            "successful_workflow_id": successful_workflow_id,
                            "span_count": len(serializable_rows),
                            "spans": serializable_rows,
                        },
                    }
                ],
            },
            sort_keys=False,
        )
    )
