from __future__ import annotations

import copy
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from dbos._serialization import DBOSDefaultSerializer
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError
from okg.substrate.cli.main import _build_parser
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.runtime_dlq import (
    _digest,
    _responsibility,
    _workflow_row,
    current_runtime_dlq,
)
from okg.substrate.runtime_dlq_acknowledgement import (
    ACKNOWLEDGEMENT_REQUEST_SCHEMA,
    _current_application_version,
    apply_runtime_dlq_acknowledgements,
    load_acknowledgement_request,
    plan_runtime_dlq_acknowledgements,
)
from okg.substrate.source_sync import SourceSyncJob
from okg.substrate.source_sync_enqueue import (
    _serialize_source_sync_dbos_args,
    _source_sync_workflow_args,
)
from tests.pytest.dbos_system_schema import create_dbos_system_schema
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def test_acknowledge_runtime_dlq_cli_surface_is_registered() -> None:
    args = _build_parser().parse_args(
        [
            "db",
            "acknowledge-runtime-dlq",
            "--deployment",
            "okg-ci",
            "--request",
            "request.json",
        ]
    )

    assert args.db_subcmd == "acknowledge-runtime-dlq"
    assert args.apply is False


def test_fenced_recovery_cli_surface_requires_owner() -> None:
    args = _build_parser().parse_args(
        [
            "runtime",
            "enqueue-fenced-recovery",
            "--deployment",
            "okg-ci",
            "doctor_workflow",
            "--promotion-id",
            "promotion-1",
            "--deduplication-id",
            "doctor-promotion-1",
        ]
    )

    assert args.rt_subcmd == "enqueue-fenced-recovery"
    assert args.workflow_name == "doctor_workflow"
    assert args.promotion_id == "promotion-1"
    assert args.deduplication_id == "doctor-promotion-1"


def test_fenced_recovery_reuses_existing_deduplicated_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admin

    monkeypatch.setattr(
        admin,
        "promotion_fence_status",
        lambda **kwargs: {
            "status": "fenced",
            "promotion_id": "promotion-1",
        },
    )
    monkeypatch.setattr(
        admin,
        "workflow_id_for_deduplication",
        lambda **kwargs: "wf-existing",
    )
    monkeypatch.setattr(
        admin,
        "enqueue_deployment_workflow",
        lambda **kwargs: pytest.fail("existing workflow must not be re-enqueued"),
    )

    workflow_id = admin.client_enqueue_fenced_recovery(
        deployment="okg-ci",
        dsn="postgres://unused",
        workflow_name="doctor_workflow",
        promotion_id="promotion-1",
        deduplication_id="doctor-promotion-1",
    )

    assert workflow_id == "wf-existing"


def test_acknowledgement_request_load_errors_are_structured(tmp_path) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{", encoding="utf-8")

    with pytest.raises(SubstrateError, match="could not be loaded"):
        load_acknowledgement_request(invalid)


def test_acknowledgement_schema_coexists_with_real_published_generation(
    clean_runtime: str,
) -> None:
    deployment = "runtime-dlq-ack-publish-proof"
    node_id = "document:runtime-dlq-ack:publish-proof"
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(node_id, "document", {}, {"id": node_id}, {"version": 1})],
            source="runtime-dlq-ack-test",
        )
        conn.commit()

    published = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()

    assert published["status"] == "published"
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.latest_generation(
            deployment_name=deployment,
            status="published",
        )
        assert str(generation.checkpoint_id) == published["generation_id"]
        live = store.checkout_generation(published["generation_id"]).query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
        assert live == [{"node_id": node_id}]
    finally:
        store.close()


def _source_inputs(deployment: str) -> tuple[str, str]:
    job = SourceSyncJob(
        deployment=deployment,
        sources=("git-history",),
        run_mode="scope_complete",
    )
    return _serialize_source_sync_dbos_args(
        args=_source_sync_workflow_args(job),
        serialization=None,
    )


def _seed_source_recovery(
    conn: psycopg.Connection,
    dsn: str,
) -> dict[str, object]:
    suffix = uuid.uuid4().hex
    deployment = f"runtime-dlq-ack-{suffix}"
    candidate = f"candidate-{suffix}"
    promotion_id = f"promotion-{suffix}"
    failure_id = f"failure-{suffix}"
    success_id = f"success-{suffix}"
    now = datetime.now(timezone.utc)
    failure_at = int((now - timedelta(minutes=3)).timestamp() * 1000)
    success_at = int(now.timestamp() * 1000)
    published_at = now - timedelta(minutes=1)
    inputs, serialization = _source_inputs(deployment)

    # Build the DBOS schema from DBOS's own migrations rather than hand-writing
    # the columns this fixture happens to read. The acknowledgement path calls
    # into the real SDK (`client_enqueue_fenced_recovery` filters on
    # `deduplication_id`), so a hand-written subset silently drifts behind every
    # `dbos` version bump and fails as UndefinedColumn.
    create_dbos_system_schema(dsn)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dbos.application_versions "
            "(version_id, version_name, version_timestamp, created_at) "
            "VALUES (%s, 'app-v2', %s, %s)",
            (str(uuid.uuid4()), success_at, success_at),
        )
        cur.executemany(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, name, status, application_version, queue_name, "
            "inputs, serialization, error, output, created_at, updated_at) "
            "VALUES (%s, 'okg_source_sync', %s, 'app-v2', %s, %s, %s, "
            "%s, %s, %s, %s)",
            [
                (
                    failure_id,
                    "ERROR",
                    f"okg-{deployment}-source-sync",
                    inputs,
                    serialization,
                    "publish failed",
                    None,
                    failure_at,
                    failure_at,
                ),
                (
                    success_id,
                    "SUCCESS",
                    f"okg-{deployment}-source-sync",
                    inputs,
                    serialization,
                    None,
                    '{"status":"published"}',
                    success_at,
                    success_at,
                ),
            ],
        )
        cur.execute(
            "INSERT INTO okg.deployment_promotion_fences "
            "(deployment_name, promotion_id, status, metadata) "
            "VALUES (%s, %s, 'fenced', %s) "
            "ON CONFLICT (deployment_name) DO UPDATE SET "
            "promotion_id = EXCLUDED.promotion_id, status = EXCLUDED.status, "
            "metadata = EXCLUDED.metadata",
            (deployment, promotion_id, Jsonb({"candidate_revision": candidate})),
        )
        cur.execute(
            "INSERT INTO okg.source_watermarks (source, last_reconcile_at) "
            "VALUES ('git-history', %s) "
            "ON CONFLICT (source) DO UPDATE SET "
            "last_reconcile_at = EXCLUDED.last_reconcile_at",
            (now - timedelta(seconds=30),),
        )
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        )
        catalog_version = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "(deployment_name, status, catalog_version_id, published_at, "
            "correlation_id) VALUES (%s, 'published', %s, %s, %s) "
            "RETURNING generation_id",
            (
                deployment,
                catalog_version,
                published_at,
                f"runtime-dlq-ack:{suffix}",
            ),
        )
        generation_id = str(cur.fetchone()[0])
        recovery_output, recovery_serialization = _serialized({
            "any_failed": False,
            "report": {
                "results": [{"source": "git-history", "status": "OK"}],
                "publishes": [
                    {
                        "status": "published",
                        "generation_id": generation_id,
                    }
                ],
            },
        })
        cur.execute(
            "UPDATE dbos.workflow_status SET output = %s, serialization = %s "
            "WHERE workflow_uuid = %s",
            (recovery_output, recovery_serialization, success_id),
        )
        cur.execute(
            "SELECT workflow_uuid, name, status, application_version, "
            "queue_name, inputs, serialization, error, output, updated_at "
            "FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (failure_id,),
        )
        responsibility = _responsibility(_workflow_row(cur.fetchone()))

    request = {
        "schema": ACKNOWLEDGEMENT_REQUEST_SCHEMA,
        "actor": "operator@example.com",
        "reason": "replacement publish and source convergence verified",
        "candidate_revision": candidate,
        "operation_receipt": {
            "candidate_revision": candidate,
            "runtime_fence_owner": promotion_id,
            "phase": "source_convergence",
        },
        "readiness": {
            "source_freshness": {"ok": True},
            "source_sync_status": {
                "convergence": {"ok": True, "state": "ready"}
            },
        },
        "workflow_ids": [failure_id],
        "witnesses": [
            {
                "kind": "source_sync",
                "responsibility": responsibility,
                "replacement_workflow_id": success_id,
            }
        ],
    }
    return {
        "deployment": deployment,
        "failure_id": failure_id,
        "success_id": success_id,
        "generation_id": generation_id,
        "request": request,
    }


def _serialized(value: object) -> tuple[str, str]:
    return DBOSDefaultSerializer.serialize(value), DBOSDefaultSerializer.name()


def _insert_generic_recovery(
    conn: psycopg.Connection,
    *,
    workflow_name: str,
    output: object,
    event: tuple[str, object] | None = None,
) -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    failure_id = f"{workflow_name}-failure-{suffix}"
    success_id = f"{workflow_name}-success-{suffix}"
    now = datetime.now(timezone.utc)
    failure_at = int((now - timedelta(minutes=3)).timestamp() * 1000)
    success_at = int(now.timestamp() * 1000)
    serialized_output, serialization = _serialized(output)
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, name, status, application_version, queue_name, "
            "inputs, serialization, error, output, created_at, updated_at) "
            "VALUES (%s, %s, %s, 'app-v2', '_dbos_internal_queue', NULL, %s, "
            "%s, %s, %s, %s)",
            [
                (
                    failure_id,
                    workflow_name,
                    "ERROR",
                    serialization,
                    "workflow failed",
                    None,
                    failure_at,
                    failure_at,
                ),
                (
                    success_id,
                    workflow_name,
                    "SUCCESS",
                    serialization,
                    None,
                    serialized_output,
                    success_at,
                    success_at,
                ),
            ],
        )
    if event is not None:
        key, value = event
        serialized_event, event_serialization = _serialized(value)
        conn.execute(
            "INSERT INTO dbos.workflow_events "
            "(workflow_uuid, key, value, serialization) VALUES (%s, %s, %s, %s)",
            (success_id, key, serialized_event, event_serialization),
        )
    return failure_id, success_id


def _request_for(
    fixture: dict[str, object],
    *,
    failure_id: str,
    witness: dict[str, object],
) -> dict[str, object]:
    request = copy.deepcopy(fixture["request"])
    assert isinstance(request, dict)
    request["workflow_ids"] = [failure_id]
    request["witnesses"] = [witness]
    return request


@pytest.mark.parametrize(
    "workflow_name",
    [
        "doctor_workflow",
        "okg_ci_resource_monitor",
        "okg_ci_git_history_reconcile",
    ],
)
def test_active_fence_owner_can_enqueue_each_recovery_witness(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    workflow_name: str,
) -> None:
    from types import SimpleNamespace

    import dbos

    from okg.substrate.runtime.admin import client_enqueue_fenced_recovery

    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client"] = kwargs

        def enqueue(self, options: dict[str, object], *args: object):
            captured["options"] = options
            captured["args"] = args
            return SimpleNamespace(workflow_id=f"recovery-{workflow_name}")

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
    request = fixture["request"]
    assert isinstance(request, dict)
    receipt = request["operation_receipt"]
    assert isinstance(receipt, dict)
    promotion_id = str(receipt["runtime_fence_owner"])

    with pytest.raises(RuntimeError, match="does not own"):
        client_enqueue_fenced_recovery(
            deployment=str(fixture["deployment"]),
            dsn=clean_runtime,
            workflow_name=workflow_name,
            promotion_id="wrong-owner",
        )

    workflow_id = client_enqueue_fenced_recovery(
        deployment=str(fixture["deployment"]),
        dsn=clean_runtime,
        workflow_name=workflow_name,
        promotion_id=promotion_id,
        deduplication_id=f"recovery-{workflow_name}",
    )

    assert workflow_id == f"recovery-{workflow_name}"
    assert captured["options"]["workflow_name"] == workflow_name
    assert captured["options"]["deduplication_id"] == f"recovery-{workflow_name}"
    context = captured["args"][1]
    assert context == {
        "okg_recovery_kind": "runtime_dlq_witness",
        "okg_promotion_id": promotion_id,
    }


def test_exact_acknowledgement_is_append_only_and_preserves_dbos_history(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        request = fixture["request"]
        assert isinstance(request, dict)
        before = conn.execute(
            "SELECT * FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (fixture["failure_id"],),
        ).fetchone()

        plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=request,
        )
        assert plan["applied"] is False
        result = apply_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=request,
            confirm_plan_id=plan["plan_id"],
        )

        assert result["applied"] is True
        assert result["acknowledged_count"] == 1
        privileges = conn.execute(
            "SELECT has_table_privilege('app_rw', "
            "'okg.runtime_dlq_acknowledgement_events', 'INSERT'), "
            "has_table_privilege('catalog_admin', "
            "'okg.runtime_dlq_acknowledgement_events', 'INSERT')"
        ).fetchone()
        assert privileges == (False, False)
        after = conn.execute(
            "SELECT * FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (fixture["failure_id"],),
        ).fetchone()
        assert after == before
        with conn.cursor() as cur:
            projected = current_runtime_dlq(cur)
        assert projected["current_count"] == 0
        assert projected["acknowledged_count"] == 1

        new_failure_id, _ = _insert_generic_recovery(
            conn,
            workflow_name="new_runtime_failure",
            output={"ok": True},
        )
        with conn.cursor() as cur:
            regressed = current_runtime_dlq(cur)
        assert regressed["current_count"] == 1
        assert regressed["acknowledged_count"] == 1
        assert regressed["current_failures"][0]["workflow_id"] == new_failure_id

        with pytest.raises(psycopg.Error, match="append-only"):
            conn.execute(
                "UPDATE okg.runtime_dlq_acknowledgement_events SET reason = 'x' "
                "WHERE workflow_id = %s",
                (fixture["failure_id"],),
            )
        with pytest.raises(psycopg.Error, match="append-only"):
            conn.execute(
                "DELETE FROM okg.runtime_dlq_acknowledgement_events "
                "WHERE workflow_id = %s",
                (fixture["failure_id"],),
            )


@pytest.mark.parametrize(
    ("report", "error"),
    [
        (
            {
                "results": [{"source": "wrong-source", "status": "OK"}],
                "publishes": [
                    {"status": "published", "generation_id": "{generation_id}"}
                ],
            },
            "unsuccessful source results",
        ),
        (
            {
                "results": [{"source": "git-history", "status": "OK"}],
                "publishes": [
                    {"status": "failed", "generation_id": "{generation_id}"}
                ],
            },
            "no successful publish",
        ),
    ],
)
def test_source_acknowledgement_requires_exact_successful_publish_output(
    clean_runtime: str,
    report: dict[str, object],
    error: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        generation_id = str(fixture["generation_id"])
        rendered_report = copy.deepcopy(report)
        publishes = rendered_report["publishes"]
        assert isinstance(publishes, list)
        publish = publishes[0]
        assert isinstance(publish, dict)
        publish["generation_id"] = generation_id
        output, serialization = _serialized(
            {"any_failed": False, "report": rendered_report}
        )
        conn.execute(
            "UPDATE dbos.workflow_status SET output = %s, serialization = %s "
            "WHERE workflow_uuid = %s",
            (output, serialization, fixture["success_id"]),
        )
        request = fixture["request"]
        assert isinstance(request, dict)

        with pytest.raises(SubstrateError, match=error):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=str(fixture["deployment"]),
                request=request,
            )


def test_malformed_raw_acknowledgement_fails_closed(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        request = fixture["request"]
        assert isinstance(request, dict)
        plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=request,
        )
        plan_payload = {
            key: copy.deepcopy(value)
            for key, value in plan.items()
            if key not in {"plan_id", "applied"}
        }
        plan_payload["deployment"] = "wrong-deployment"
        plan_payload["candidate_revision"] = "bogus-candidate"
        plan_payload["runtime_fence"]["candidate_revision"] = "bogus-candidate"
        plan_payload["entries"][0]["witness"] = {}
        entry = plan_payload["entries"][0]
        forged_plan_id = _digest(plan_payload)
        conn.execute(
            "INSERT INTO okg.runtime_dlq_acknowledgement_events "
            "(deployment_name, workflow_id, workflow_name, application_version, "
            "failure_snapshot_digest, error_digest, responsibility, actor, reason, "
            "candidate_revision, operation_receipt_digest, witness, plan_payload, "
            "plan_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s)",
            (
                "wrong-deployment",
                entry["workflow_id"],
                entry["workflow_name"],
                entry["application_version"],
                entry["failure_snapshot_digest"],
                entry["error_digest"],
                Jsonb(entry["responsibility"]),
                plan_payload["actor"],
                plan_payload["reason"],
                "bogus-candidate",
                plan_payload["operation_receipt_digest"],
                Jsonb({}),
                Jsonb(plan_payload),
                forged_plan_id,
            ),
        )

        with conn.cursor() as cur:
            projected = current_runtime_dlq(cur)
        assert projected["current_count"] == 1
        assert projected["acknowledged_count"] == 0
        assert len(projected["invalid_acknowledgements"]) == 1


def test_stale_plan_is_rejected_atomically(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        request = fixture["request"]
        assert isinstance(request, dict)
        second_failure_id = f"second-failure-{uuid.uuid4().hex}"
        conn.execute(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, name, status, application_version, queue_name, "
            "inputs, serialization, error, output, created_at, updated_at) "
            "SELECT %s, name, status, application_version, queue_name, inputs, "
            "serialization, error, output, created_at, updated_at "
            "FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (second_failure_id, fixture["failure_id"]),
        )
        request["workflow_ids"] = [fixture["failure_id"], second_failure_id]
        plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=request,
        )
        assert len(plan["entries"]) == 2
        conn.execute(
            "UPDATE dbos.workflow_status SET error = 'changed after dry run' "
            "WHERE workflow_uuid = %s",
            (second_failure_id,),
        )

        with pytest.raises(SubstrateError, match="plan drifted"):
            apply_runtime_dlq_acknowledgements(
                conn,
                deployment=deployment,
                request=request,
                confirm_plan_id=plan["plan_id"],
            )
        count = conn.execute(
            "SELECT count(*) FROM okg.runtime_dlq_acknowledgement_events "
            "WHERE workflow_id = ANY(%s)",
            ([fixture["failure_id"], second_failure_id],),
        ).fetchone()[0]
        assert count == 0


def test_apply_version_read_blocks_concurrent_version_insert(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as setup:
        _seed_source_recovery(setup, clean_runtime)
    with psycopg.connect(clean_runtime) as locked:
        assert _current_application_version(locked, lock=True) == "app-v2"
        with psycopg.connect(clean_runtime, autocommit=True) as concurrent:
            concurrent.execute("SET lock_timeout = '250ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                concurrent.execute(
                    "INSERT INTO dbos.application_versions "
                    "(version_id, version_name, version_timestamp, created_at) "
                    "VALUES (%s, 'app-v3', %s, %s)",
                    (str(uuid.uuid4()), 3, 3),
                )


def test_acknowledgement_refuses_unknown_workflow_type(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        request = copy.deepcopy(fixture["request"])
        conn.execute(
            "UPDATE dbos.workflow_status SET name = 'unknown_runtime_workflow' "
            "WHERE workflow_uuid = %s",
            (fixture["failure_id"],),
        )

        with pytest.raises(SubstrateError, match="unknown workflow type"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=str(fixture["deployment"]),
                request=request,
            )


def test_workflow_specific_doctor_resource_and_git_witnesses(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])

        doctor_failure, doctor_success = _insert_generic_recovery(
            conn,
            workflow_name="doctor_workflow",
            output=None,
            event=(
                "doctor_verdict",
                {"deployment": deployment, "ok": True, "doctor": {"ok": True}},
            ),
        )
        doctor_request = _request_for(
            fixture,
            failure_id=doctor_failure,
            witness={
                "kind": "doctor",
                "replacement_workflow_id": doctor_success,
            },
        )
        doctor_request["readiness"]["doctor"] = {"ok": True}
        doctor_plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=doctor_request,
        )
        assert doctor_plan["entries"][0]["witness"]["kind"] == "doctor"

        resource_failure, resource_success = _insert_generic_recovery(
            conn,
            workflow_name="okg_ci_resource_monitor",
            output={
                "ok": True,
                "host_count": 2,
                "reachable": {"okg-ci": True, "runner-1": True},
                "node_ids": {
                    "okg-ci": "host:okg-ci",
                    "runner-1": "host:runner-1",
                },
            },
        )
        sample_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.node_series "
                "(deployment_name, node_id, metric, observed_at, value, status) "
                "VALUES (%s, %s, 'reachable', %s, 1.0, 'ok')",
                [
                    (deployment, "host:okg-ci", sample_at),
                    (deployment, "host:runner-1", sample_at),
                    (deployment, "host:retired-runner", sample_at),
                ],
            )
        resource_request = _request_for(
            fixture,
            failure_id=resource_failure,
            witness={
                "kind": "resource_monitor",
                "replacement_workflow_id": resource_success,
            },
        )
        resource_plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=resource_request,
        )
        assert resource_plan["entries"][0]["witness"]["kind"] == "resource_monitor"

        child_id = f"git-child-{uuid.uuid4().hex}"
        git_failure, git_success = _insert_generic_recovery(
            conn,
            workflow_name="okg_ci_git_history_reconcile",
            output={
                "ok": True,
                "source": "git-history",
                "run_mode": "scope_complete",
                "status": "ENQUEUED",
                "workflow_id": child_id,
            },
        )
        inputs, serialization = _source_inputs(deployment)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        conn.execute(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, name, status, application_version, queue_name, "
            "inputs, serialization, error, output, created_at, updated_at) "
            "VALUES (%s, 'okg_source_sync', 'SUCCESS', 'app-v2', %s, %s, %s, "
            "NULL, %s, %s, %s)",
            (
                child_id,
                f"okg-{deployment}-source-sync",
                inputs,
                serialization,
                _serialized({
                    "any_failed": False,
                    "report": {
                        "results": [
                            {"source": "git-history", "status": "OK"}
                        ],
                        "publishes": [
                            {
                                "status": "published",
                                "generation_id": fixture["generation_id"],
                            }
                        ],
                    },
                })[0],
                now_ms,
                now_ms,
            ),
        )
        git_request = _request_for(
            fixture,
            failure_id=git_failure,
            witness={
                "kind": "git_reconcile",
                "replacement_workflow_id": git_success,
                "child_workflow_id": child_id,
            },
        )
        git_plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=git_request,
        )
        assert git_plan["entries"][0]["witness"]["child_workflow_id"] == child_id


def test_caller_readiness_cannot_substitute_for_live_convergence(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        request = copy.deepcopy(fixture["request"])
        request["readiness"] = {
            "source_freshness": {"ok": False},
            "source_sync_status": {
                "convergence": {"ok": False, "state": "blocked"}
            },
        }

        plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=str(fixture["deployment"]),
            request=request,
        )

        witness = plan["entries"][0]["witness"]
        assert witness["live_convergence"]["state"] == "ready"
        assert witness["live_convergence"]["authority"] == [
            "dbos.workflow_status.output",
            "okg.source_progress_or_watermark",
            "okg.graph_generations",
        ]


@pytest.mark.parametrize(
    ("workflow_name", "kind", "output"),
    [
        (
            "okg_ci_resource_monitor",
            "resource_monitor",
            {"ok": False, "host_count": 0, "reachable": {}},
        ),
        (
            "okg_ci_git_history_reconcile",
            "git_reconcile",
            {
                "ok": False,
                "source": "git-history",
                "run_mode": "scope_complete",
            },
        ),
    ],
)
def test_unhealthy_resource_and_git_outputs_are_refused(
    clean_runtime: str,
    workflow_name: str,
    kind: str,
    output: dict[str, object],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        failure_id, success_id = _insert_generic_recovery(
            conn,
            workflow_name=workflow_name,
            output=output,
        )
        request = _request_for(
            fixture,
            failure_id=failure_id,
            witness={"kind": kind, "replacement_workflow_id": success_id},
        )

        with pytest.raises(SubstrateError, match="not healthy|no healthy"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=str(fixture["deployment"]),
                request=request,
            )


def test_unhealthy_doctor_event_is_refused(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        failure_id, success_id = _insert_generic_recovery(
            conn,
            workflow_name="doctor_workflow",
            output=None,
            event=("doctor_verdict", {"deployment": deployment, "ok": False}),
        )
        request = _request_for(
            fixture,
            failure_id=failure_id,
            witness={"kind": "doctor", "replacement_workflow_id": success_id},
        )
        request["readiness"]["doctor"] = {"ok": True}

        with pytest.raises(SubstrateError, match="doctor verdict is unhealthy"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=deployment,
                request=request,
            )


def test_generic_recovery_from_other_application_version_is_refused(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        failure_id, success_id = _insert_generic_recovery(
            conn,
            workflow_name="doctor_workflow",
            output=None,
            event=("doctor_verdict", {"deployment": deployment, "ok": True}),
        )
        conn.execute(
            "UPDATE dbos.workflow_status SET application_version = 'app-v1' "
            "WHERE workflow_uuid = %s",
            (success_id,),
        )
        request = _request_for(
            fixture,
            failure_id=failure_id,
            witness={"kind": "doctor", "replacement_workflow_id": success_id},
        )
        request["readiness"]["doctor"] = {"ok": True}

        with pytest.raises(SubstrateError, match="application version"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=deployment,
                request=request,
            )


def test_resource_recovery_without_durable_samples_is_refused(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        failure_id, success_id = _insert_generic_recovery(
            conn,
            workflow_name="okg_ci_resource_monitor",
            output={
                "ok": True,
                "host_count": 1,
                "reachable": {"okg-ci": True},
                "node_ids": {"okg-ci": "host:okg-ci"},
            },
        )
        request = _request_for(
            fixture,
            failure_id=failure_id,
            witness={
                "kind": "resource_monitor",
                "replacement_workflow_id": success_id,
            },
        )

        with pytest.raises(SubstrateError, match="durable|samples"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=str(fixture["deployment"]),
                request=request,
            )


def test_git_witness_without_child_publish_is_refused(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        child_id = f"git-child-unpublished-{uuid.uuid4().hex}"
        failure_id, success_id = _insert_generic_recovery(
            conn,
            workflow_name="okg_ci_git_history_reconcile",
            output={
                "ok": True,
                "source": "git-history",
                "run_mode": "scope_complete",
                "status": "ENQUEUED",
                "workflow_id": child_id,
            },
        )
        inputs, serialization = _source_inputs(deployment)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        conn.execute(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, name, status, application_version, queue_name, "
            "inputs, serialization, error, output, created_at, updated_at) "
            "VALUES (%s, 'okg_source_sync', 'SUCCESS', 'app-v2', %s, %s, %s, "
            "NULL, %s, %s, %s)",
            (
                child_id,
                f"okg-{deployment}-source-sync",
                inputs,
                serialization,
                _serialized({
                    "any_failed": False,
                    "report": {
                        "results": [
                            {"source": "git-history", "status": "OK"}
                        ],
                        "publishes": [
                            {
                                "status": "published",
                                "generation_id": fixture["generation_id"],
                            }
                        ],
                    },
                })[0],
                now_ms,
                now_ms,
            ),
        )
        conn.execute(
            "DELETE FROM okg.graph_generations WHERE deployment_name = %s",
            (deployment,),
        )
        request = _request_for(
            fixture,
            failure_id=failure_id,
            witness={
                "kind": "git_reconcile",
                "replacement_workflow_id": success_id,
                "child_workflow_id": child_id,
            },
        )

        with pytest.raises(SubstrateError, match="not a bound published generation"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=deployment,
                request=request,
            )


def test_source_witness_refuses_responsibility_and_watermark_drift(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fixture = _seed_source_recovery(conn, clean_runtime)
        deployment = str(fixture["deployment"])
        mismatched = copy.deepcopy(fixture["request"])
        mismatched["witnesses"][0]["responsibility"]["run_mode"] = "reconcile"
        with pytest.raises(SubstrateError, match="matching responsibility witness"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=deployment,
                request=mismatched,
            )

        conn.execute(
            "UPDATE okg.source_watermarks SET last_reconcile_at = %s "
            "WHERE source = 'git-history'",
            (datetime.now(timezone.utc) - timedelta(days=1),),
        )
        request = fixture["request"]
        assert isinstance(request, dict)
        with pytest.raises(SubstrateError, match="watermark did not move"):
            plan_runtime_dlq_acknowledgements(
                conn,
                deployment=deployment,
                request=request,
            )
