from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    classify_effective_attribute_recovery,
    prepare_corrupt_effective_attribute_recovery,
    repair_effective_attribute_interval_view,
)
from okg.substrate.cli import deployment as deployment_cli
from okg.substrate.db.migrate import migrate
from okg.substrate.db.management import migration_status
from okg.substrate.deployments import resolve_deployment
from okg.substrate.deployment_lifecycle_contract import CANONICAL_CONTRACT
from okg.substrate.deployment_live_proof import (
    collect_live_deployment_proof,
    query_probe_path,
)
from okg.substrate.deployment_upgrade import (
    CommandResult,
    UpgradeOptions,
    UpgradeContext,
    _collect_ci_deployment_final_proof,
    _phase_nomos_baseline,
    run_deployment_upgrade,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.deployment_stack import build_runtime_catalog_identity
from okg.substrate.runtime_dlq import (
    _responsibility,
    _workflow_row,
    current_runtime_dlq,
)
from okg.substrate.source_sync_convergence import deployment_convergence_inventory
from okg.substrate.source_sync_enqueue import _serialize_source_sync_dbos_args
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.dbos_system_schema import create_dbos_system_schema
from tests.substrate.deployment_catalog_claim import (
    load_and_claim_deployment_catalog,
    point_deployment_at_test_database,
)

pytestmark = pytest.mark.postgres


CI_SOURCES = (
    "git-history",
    "github-issues",
    "pact",
    "test-reports",
    "package-manifests",
)


def _ok(payload) -> CommandResult:
    return CommandResult(0, json.dumps(payload, default=str), "")


def _dbos_args(args: list[Any]) -> tuple[str, str]:
    """Encode workflow arguments the way DBOS persists them.

    `dbos.workflow_status.inputs` is TEXT holding a serialized
    ``{"args": ..., "kwargs": ...}`` payload plus the serializer name in
    `serialization` — the same pair `source_sync_enqueue` writes when it
    compacts a queued source-sync workflow, which is why this delegates to that
    module's own encoder rather than restating it. Diagnostics decode it with
    `deserialize_args`, so the fixture has to write the real encoding for the
    recovery/DLQ readers to see a payload at all.
    """
    return _serialize_source_sync_dbos_args(args=tuple(args), serialization=None)


def _dbos_output(value: Any) -> tuple[str, str]:
    """Encode a workflow result the way DBOS persists `output` (also TEXT).

    Returns the ``(output, serialization)`` pair: a row whose `output` is
    written without its serializer name is one a reader cannot decode.
    """
    from dbos._serialization import (  # type: ignore[import-untyped]
        DBOSDefaultSerializer,
        serialize_value,
    )

    return serialize_value(value, None, DBOSDefaultSerializer)


def test_one_upgrade_closes_observed_okg_ci_incident(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dsn = clean_runtime
    actual = resolve_deployment("okg-ci")
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    manifest = SimpleNamespace(
        deployment_dir=actual.deployment_dir,
        source_registry=actual.source_registry,
        runtime=actual.runtime,
        release={"channel": "prod", "pin": {"code_revision": revision}},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": dsn,
            "target": {"database": "okg_test", "identity": "isolated"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release.current_release_artifact",
        lambda manifest: SimpleNamespace(code_revision=revision),
    )

    def verified_backup(ctx) -> dict:
        result = {
            "ok": True,
            "verification_mode": "current_full_restore",
            "archive_revalidated": True,
        }
        ctx.data["backup_result"] = result
        return result

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_backup",
        verified_backup,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_backup_before_mutation",
        lambda ctx: {
            "ok": True,
            "verification_mode": "current_full_restore",
            "archive_revalidated": True,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._revalidate_effect_authority",
        lambda ctx, backup: {
            "ok": True,
            "target_revision_stable": True,
            "effect_digest_stable": True,
            "effect_inventory": {
                "smoke": {"selected_source_digest": "sha256:dirty-state"},
            },
            "smoke_source_digest": "sha256:dirty-state",
        },
    )

    # The shared PostgreSQL fixture resets public substrate state but DBOS owns
    # a separate schema. Rebuild it from DBOS's own migrations so this proof is
    # order-independent AND cannot drift behind a `dbos` version bump: the
    # runtime queue drain reads every column the installed SDK declares.
    create_dbos_system_schema(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person"],
            narrowings=[("case", "reported_by", "person")],
        )
        conn.execute(
            "INSERT INTO okg.database_owner "
            "(singleton, owner_deployment_name, stamped_by_command) "
            "VALUES (true, 'okg-ci', 'dirty-state-fixture') "
            "ON CONFLICT (singleton) DO UPDATE SET owner_deployment_name = EXCLUDED.owner_deployment_name"
        )
        # The promotion fence is not one of the tables `clean_runtime`
        # truncates, and an upgrade that fails mid-flight leaves its fence
        # behind. Clear it so a rerun after a failure is not refused by the
        # previous run's fence.
        conn.execute(
            "DELETE FROM okg.deployment_promotion_fences "
            "WHERE deployment_name = 'okg-ci'"
        )
        conn.execute(
            "INSERT INTO dbos.application_versions "
            "(version_id, version_name, version_timestamp, created_at) "
            "VALUES (%s, 'v-current', 999999999, 999999999) ON CONFLICT DO NOTHING",
            (str(uuid.uuid4()),),
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("case:incident:baseline", "case", {}, {"id": "baseline"}, {"v": 1})],
            source="default",
        )
    nomos_seed = _phase_nomos_baseline(SimpleNamespace(
        options=SimpleNamespace(deployment="okg-ci", plan_only=False),
        dsn=dsn,
        data={},
    ))
    assert nomos_seed["ok"] is True, nomos_seed
    # The incident's predecessor generation came from okg-ci itself, and the
    # upgrade re-claims the catalog as okg-ci's own writer. Claim and load the
    # catalog the way an operator does (`okg catalog ownership claim`, then
    # `okg catalog load --apply`), with okg-ci's DSN pointed at, and checked
    # against, the test database; then publish as that writer.
    load_and_claim_deployment_catalog(
        Path(actual.deployment_dir), dsn, monkeypatch, capsys,
    )
    deployment_identity = build_runtime_catalog_identity(
        dsn=dsn,
        deployment_name="okg-ci",
        deployment_dir=Path(actual.deployment_dir),
        command="pytest okg-ci publish",
    )
    predecessor_publish = Publisher(
        dsn, "okg-ci", catalog_identity=deployment_identity,
    ).cycle()
    assert predecessor_publish["status"] == "published", predecessor_publish
    predecessor = str(predecessor_publish["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        predecessor_metadata = dict(store.get_generation(predecessor).metadata or {})
        predecessor_metadata["node_count"] = int(predecessor_metadata["node_count"]) + 9
        conn.execute(
            "UPDATE public._chronos_branch_interval_checkpoints SET metadata = %s "
            "WHERE checkpoint_id = %s",
            (json.dumps(predecessor_metadata), predecessor),
        )
        conn.execute(
            f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} WHERE ctid IN ("
            f"SELECT ctid FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} LIMIT 1)"
        )
        # A current-version source-sync failure matching the first recovery action.
        failed_args = [["git-history"], "scope_complete", True, "closure", [], True, True, False, [], None]
        failed_inputs, failed_serialization = _dbos_args(failed_args)
        conn.execute(
            "INSERT INTO dbos.workflow_status "
            "(workflow_uuid, status, name, application_version, queue_name, "
            "inputs, serialization) "
            "VALUES ('failed-git', 'ERROR', 'okg_source_sync', 'v-current', "
            "'okg-okg-ci-source-sync', %s, %s)",
            (failed_inputs, failed_serialization),
        )
        # Pin that the DLQ reader actually DECODES the row we just wrote.
        # `source_sync_payload_from_dbos_inputs` returns None on any decode
        # failure and `_responsibility` then falls back to empty sources — which
        # would still compare equal to an equally-degraded successor, so the
        # supersession assertion at the end of this test would pass vacuously.
        # Assert the decoded responsibility here so a serialization drift fails
        # loudly at the seed instead of silently hollowing out the proof.
        seeded = conn.execute(
            "SELECT workflow_uuid, name, status, application_version, "
            "queue_name, inputs, serialization, error, output, updated_at "
            "FROM dbos.workflow_status WHERE workflow_uuid = 'failed-git'"
        ).fetchone()
        assert _responsibility(_workflow_row(seeded)) == {
            "deployment": "okg-ci",
            "sources": ["git-history"],
            "workflow_type": "okg_source_sync",
            "application_version": "v-current",
            "run_mode": "scope_complete",
            "scope": [],
        }
        assert classify_effective_attribute_recovery(
            conn, deployment_name="okg-ci"
        )["classification"] == "corrupt_state_recovery"

    ready_index = 0
    workflow_sources: dict[str, str] = {}
    workflow_published: set[str] = set()
    fenced_recovery_workflows: set[str] = set()
    smoke_counter = 0

    def latest_generation() -> str:
        with psycopg.connect(dsn, autocommit=True) as conn:
            generation = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).latest_generation(deployment_name="okg-ci", status="published")
        assert generation is not None
        return str(generation.checkpoint_id)

    # The sealed Postgres capacity authority describes the okg-ci production
    # PostgreSQL *service* (worker budget, TimescaleDB scheduler headroom), not
    # the database under test. Like the doctor/MCP boundaries below it is a host
    # process boundary this fixture replaces; leaving it live would make the
    # proof assert facts about whatever Postgres the suite happens to run on.
    #
    # Report it NOT APPLICABLE rather than passing. `applicable: False` is the
    # shape production emits for a deployment with no sealed environment, and
    # `_ready_postgres_capacity_check_level` maps it to "skipped" instead of
    # "pass". Claiming "capacity matches sealed authority" when nothing was
    # measured would put a fabricated pass into a payload this proof asserts on.
    monkeypatch.setattr(
        deployment_cli,
        "_ready_postgres_capacity_status",
        lambda *args, **kwargs: {
            "schema": "okg.postgres_capacity_readiness/v1",
            "ok": True,
            "applicable": False,
            "status": "not_applicable",
            "deployment": "okg-ci",
            "reason": "host PostgreSQL service boundary is replaced in this fixture",
        },
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_nomos_source_lineage_status",
        lambda *args, **kwargs: {"ok": True, "status": "ready"},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_nomos_read_probe_status",
        lambda *args, **kwargs: {"ok": True, "status": "ready"},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda *args, **kwargs: {
            "ok": True,
            "pinned_generation_id": latest_generation(),
        },
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_freshness_status",
        lambda *args, **kwargs: {
            "ok": len(workflow_published) == len(CI_SOURCES),
            "max_source_age_seconds": 21600,
            "missing": [
                source for source in CI_SOURCES
                if f"wf-{source}" not in workflow_published
            ],
            "stale": [],
            "groups": {
                "fixture": {
                    "ok": len(workflow_published) == len(CI_SOURCES),
                    "sources": [
                        {
                            "source": source,
                            "source_name": source,
                            "status": (
                                "fresh"
                                if f"wf-{source}" in workflow_published
                                else "missing"
                            ),
                            "last_reconcile_at": (
                                datetime.now(timezone.utc).isoformat()
                                if f"wf-{source}" in workflow_published
                                else None
                            ),
                        }
                        for source in CI_SOURCES
                    ]
                }
            },
        },
    )
    source_inventory = deployment_convergence_inventory(actual.deployment_dir)[
        "sources"
    ]
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_sync_liveness_status",
        lambda *args, **kwargs: {
            "deployment": "okg-ci",
            "ok": len(workflow_published) == len(CI_SOURCES),
            "missing_producers": [
                source for source in CI_SOURCES
                if f"wf-{source}" not in workflow_published
            ],
            "stale_producers": [],
            "expected_producers": [
                {"source_name": source} for source in CI_SOURCES
            ],
            "disabled_producers": [],
            "producers": [
                {"source_name": workflow_sources[row]}
                for row in workflow_published
            ],
            "dbos_queue_depth": {
                "available": True,
                "active_count": 0,
                "failed_terminal_count": 0,
                "terminal_history_count": 1,
            },
            "source_convergence_inventory": source_inventory,
            "active_source_sync_workflows": {},
        },
    )

    def aggregated_readiness() -> dict:
        payload = deployment_cli._deployment_ready_live_checks(
            deployment="okg-ci",
            deployment_dir=actual.deployment_dir,
            dsn=dsn,
            max_active_queue_depth=0,
            max_source_age_seconds=21600,
            readiness_profile="ci",
            skip_doctor=True,
        )
        payload["ok"] = not payload.get("blockers")
        payload["nomos_audit"] = {"ok": True}
        payload["nomos_runtime"] = {"ok": True}
        # CI readiness intentionally skips the workspace-specific doctor; the
        # collector still requires and records this external boundary.
        payload["doctor"] = {"ok": True, "status": "simulated_boundary"}
        return payload

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal ready_index, smoke_counter
        if argv[0] == "uv":
            return _ok({"ok": True})
        if "dogfood" in argv and ("down" in argv or "restart" in argv):
            return _ok({"ok": True, "worker": "stable"})
        if "migrations" in argv:
            return _ok(migration_status(dsn))
        if "migrate" in argv and "migrations" not in argv:
            return _ok(migrate(dsn))
        if "catalog" in argv and "load" in argv:
            return _ok({"ok": True, "catalog_aligned": True})
        if "inspect-effective-attribute-recovery" in argv:
            with psycopg.connect(dsn, autocommit=True) as conn:
                return _ok(classify_effective_attribute_recovery(conn, deployment_name="okg-ci"))
        if "prepare-effective-attribute-recovery" in argv:
            expected = json.loads(argv[argv.index("--expected-classification") + 1])
            with psycopg.connect(dsn, autocommit=True) as conn:
                return _ok(prepare_corrupt_effective_attribute_recovery(
                    conn,
                    deployment_name="okg-ci",
                    expected_binding_digest=expected["binding_digest"],
                    expected_classification=expected,
                ))
        if "repair-effective-attribute-intervals" in argv:
            with psycopg.connect(dsn, autocommit=True) as conn:
                return _ok(repair_effective_attribute_interval_view(conn, deployment_name="okg-ci"))
        if "generation" in argv and "describe" in argv:
            generation_id = argv[argv.index("describe") + 1]
            with psycopg.connect(dsn, autocommit=True) as conn:
                generation = ChronosGraphStore.from_connection(conn, ensure_schema=False).get_generation(generation_id)
            return _ok({"metadata": dict(generation.metadata or {})})
        if "current-runtime-dlq" in argv:
            with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
                return _ok(current_runtime_dlq(cur))
        if "find-source-run" in argv:
            run_key = argv[argv.index("--run-key") + 1]
            with psycopg.connect(dsn, autocommit=True) as conn:
                row = conn.execute(
                    "SELECT workflow_uuid FROM dbos.workflow_status "
                    "WHERE queue_name = 'okg-okg-ci-source-sync' "
                    "AND deduplication_id = %s ORDER BY created_at DESC LIMIT 1",
                    (run_key,),
                ).fetchone()
            return _ok({"dbos_workflow_id": str(row[0]) if row else None})
        if "enqueue-source" in argv:
            source = argv[argv.index("--source") + 1]
            run_mode = argv[argv.index("--run-mode") + 1]
            supersedes = (
                argv[argv.index("--supersedes-workflow-id") + 1]
                if "--supersedes-workflow-id" in argv else None
            )
            workflow_id = f"wf-{source}"
            run_key = argv[argv.index("--run-key") + 1]
            args = [[source], run_mode, True, "closure", [], True, True, False, [], supersedes]
            inputs, serialization = _dbos_args(args)
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(
                    "INSERT INTO dbos.workflow_status "
                    "(workflow_uuid, status, name, application_version, queue_name, "
                    "deduplication_id, inputs, serialization) "
                    "VALUES (%s, 'PENDING', 'okg_source_sync', 'v-current', "
                    "'okg-okg-ci-source-sync', %s, %s, %s)",
                    (workflow_id, run_key, inputs, serialization),
                )
            workflow_sources[workflow_id] = source
            return _ok({"dbos_workflow_id": workflow_id})
        if "enqueue-fenced-recovery" in argv:
            from okg.substrate.runtime.admin import client_enqueue_fenced_recovery

            workflow_id = client_enqueue_fenced_recovery(
                deployment="okg-ci",
                dsn=dsn,
                workflow_name=argv[argv.index("enqueue-fenced-recovery") + 1],
                promotion_id=argv[argv.index("--promotion-id") + 1],
                deduplication_id=argv[argv.index("--deduplication-id") + 1],
            )
            fenced_recovery_workflows.add(workflow_id)
            return _ok({"workflow_id": workflow_id})
        if "debug-workflow" in argv:
            workflow_id = argv[-1]
            if workflow_id in fenced_recovery_workflows:
                # The fenced witness row above is a real DBOS enqueue, but no
                # worker process runs in this fixture, so drive it to its
                # terminal state here rather than polling forever.
                output = {"any_failed": False}
                serialized, serialization = _dbos_output(output)
                with psycopg.connect(dsn, autocommit=True) as conn:
                    conn.execute(
                        "UPDATE dbos.workflow_status "
                        "SET status = 'SUCCESS', output = %s, serialization = %s, "
                        "updated_at = (EXTRACT(epoch FROM now()) * 1000)::bigint "
                        "WHERE workflow_uuid = %s",
                        (serialized, serialization, workflow_id),
                    )
                return _ok({
                    "workflow_id": workflow_id,
                    "status": "SUCCESS",
                    "output": output,
                })
            source = workflow_sources[workflow_id]
            if workflow_id not in workflow_published:
                with psycopg.connect(dsn, autocommit=True) as conn:
                    ChronosGraphStore.from_connection(conn).apply_facts(
                        [NodeFact(f"case:source:{source}", "case", {}, {"id": source}, {"v": 1})],
                        source="default",
                    )
                published = Publisher(dsn, "okg-ci", catalog_identity=deployment_identity).cycle()
                assert published["status"] == "published"
                with psycopg.connect(dsn, autocommit=True) as conn:
                    serialized, serialization = _dbos_output({
                        "any_failed": False,
                        "generation_id": published["generation_id"],
                    })
                    conn.execute(
                        "UPDATE dbos.workflow_status SET status = 'SUCCESS', "
                        "output = %s, serialization = %s "
                        "WHERE workflow_uuid = %s",
                        (serialized, serialization, workflow_id),
                    )
                workflow_published.add(workflow_id)
            return _ok({"workflow_id": workflow_id, "status": "SUCCESS", "output": {"any_failed": False}})
        if "ready" in argv:
            if ready_index < len(CI_SOURCES):
                source = CI_SOURCES[ready_index]
                ready_index += 1
                return CommandResult(1, json.dumps({
                    "ok": False,
                    "source_sync_status": {"convergence": {
                        "ok": False,
                        "state": "blocked",
                        "recovery_plan": {"actions": [{
                            "action_kind": "completed_scope_reconcile",
                            "source_name": source,
                            "run_mode": "scope_complete",
                        }]},
                    }},
                }), "")
            if ready_index == len(CI_SOURCES):
                ready_index += 1
                return CommandResult(1, json.dumps({
                    "ok": False,
                    "source_sync_status": {"convergence": {
                        "ok": True, "state": "ready", "recovery_plan": {"actions": []}
                    }},
                }), "")
            return _ok(aggregated_readiness())
        if "release" in argv:
            return _ok({"ok": True, "release_revision": revision})
        if "ingest" in argv:
            previous = os.environ.get("OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION")
            supplied = env.get("OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION")
            if supplied:
                os.environ["OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION"] = supplied
            else:
                smoke_counter += 1
                with psycopg.connect(dsn, autocommit=True) as conn:
                    case_id = f"case:smoke:{smoke_counter}"
                    person_id = f"person:smoke:{smoke_counter}"
                    ChronosGraphStore.from_connection(conn).apply_facts([
                        NodeFact(case_id, "case", {}, {"id": case_id}, {"v": 1}),
                        NodeFact(person_id, "person", {}, {"id": person_id}, {"v": 1}),
                        EdgeFact(
                            case_id, person_id, "reported_by",
                            source_record_id={"id": f"edge:{smoke_counter}"},
                            source_revision={"v": 1},
                        ),
                    ], source="default", strict_endpoints=True)
            try:
                published = Publisher(dsn, "okg-ci", catalog_identity=deployment_identity).cycle()
            finally:
                if supplied:
                    if previous is None:
                        os.environ.pop("OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION", None)
                    else:
                        os.environ["OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION"] = previous
            return _ok({"publishes": [published], "results": [{"status": "OK"}]})
        raise AssertionError(argv)

    result = run_deployment_upgrade(
        UpgradeOptions(
            deployment="okg-ci",
            readiness_profile="ci",
            state_file=tmp_path / "operation.json",
            backup_dir=tmp_path / "backups",
        ),
        command_runner=runner,
    )
    assert result["ok"] is True, json.dumps(result, indent=2, default=str)
    assert result["status"] == "applied"
    assert [row["phase"] for row in result["phases"]][-1] == "readiness"
    final = result["phases"][-1]["final_proof"]
    assert final["ok"] is True
    assert all(final["checks"].values())
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        retained = dict(
            ChronosGraphStore.from_connection(conn, ensure_schema=False)
            .get_generation(predecessor).metadata or {}
        )
        dlq = current_runtime_dlq(cur)
    assert retained == predecessor_metadata
    assert dlq["current_count"] == 0
    assert dlq["historical_count"] >= 1
    persisted = json.loads((tmp_path / "operation.json").read_text())
    assert len(persisted["source_recovery_workflows"]) == len(CI_SOURCES)
    assert all(
        row["workflow_id"] and row["terminal_status"] == "SUCCESS"
        for row in persisted["source_recovery_workflows"]
    )

    # Exercise the same post-up collector the controller invokes before it may
    # advance its active baseline. Authorities below are backed by this real
    # PostgreSQL/DBOS state; only the systemd/MCP/network process boundaries
    # remain replaced in the fixture.
    operation_receipt = tmp_path / "controller-operation.json"
    operation_receipt.write_text(json.dumps({
        "schema": "okg.okg_ci_autoupdate_operation.v2",
        "operation_id": "dirty-state-proof",
        "status": "succeeded",
        "exit_code": 0,
        "worker_state": "active",
        "candidate_revision": revision,
        "plan_digest": "sha256:" + "1" * 64,
        "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
        "baseline_disposition": "pending",
        "state_file": str(tmp_path / "operation.json"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }))

    def proof_runner(argv) -> tuple[int, str, str]:
        command = list(argv)
        if "metrics" in command:
            return 0, json.dumps({
                "generation_id": latest_generation(),
                "nodes_live_delta": 1,
                "edges_live_delta": 1,
            }), ""
        # The query probe is a Python script. Match it by its path: every okg
        # command here also starts with the Python interpreter
        # (`running_okg_argv()` is `python -P -m okg.substrate.cli.main`), so an
        # interpreter-name match routed `ready`, `release` and the DLQ read
        # into this branch.
        if str(query_probe_path()) in command:
            generation_id = command[command.index("--generation-id") + 1]
            with psycopg.connect(dsn, autocommit=True) as conn:
                checkout = ChronosGraphStore.from_connection(
                    conn, ensure_schema=False
                ).checkout_generation(generation_id)
                count = int(checkout.query(
                    "SELECT count(*) AS n FROM okg.graph_nodes"
                )[0]["n"])
            return 0, json.dumps({
                "ok": count > 0,
                "pinned_generation_id": generation_id,
                "node_count": count,
            }), ""
        result = runner(command, {"OKG_DSN": dsn})
        return result.returncode, result.stdout, result.stderr

    live_proof = collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=operation_receipt,
        dsn=dsn,
        runner=proof_runner,
    )
    assert live_proof["ok"] is True, json.dumps(live_proof, indent=2, default=str)
    assert all(live_proof["checks"].values())
    assert {row["status"] for row in live_proof["evidence"]} == {"pass"}


@pytest.mark.parametrize(
    ("target_db", "refusal"),
    [
        ("okg_ci", "not a disposable test database"),
        ("okg_test_someone_else", "the configured test database is"),
    ],
)
def test_catalog_claim_refuses_a_dsn_outside_the_test_database(
    monkeypatch: pytest.MonkeyPatch,
    target_db: str,
    refusal: str,
) -> None:
    """The okg-ci catalog claim above runs real CLI commands that resolve the
    DSN from okg-ci's own manifest. The guard refuses, before any command
    runs or any variable changes, a target that is not the configured test
    database, and points okg-ci at the test database when it is."""
    configured = "postgres://127.0.0.1:1/okg_test_guard"
    monkeypatch.setenv("OKG_TEST_DSN", configured)
    monkeypatch.setenv("OKG_CI_DSN", "postgres://127.0.0.1:1/okg_ci")
    deployment_dir = Path(resolve_deployment("okg-ci").deployment_dir)

    with pytest.raises(pytest.fail.Exception, match=refusal):
        point_deployment_at_test_database(
            deployment_dir, f"postgres://127.0.0.1:1/{target_db}", monkeypatch,
        )
    assert os.environ["OKG_CI_DSN"].endswith("/okg_ci")

    assert point_deployment_at_test_database(
        deployment_dir, configured, monkeypatch,
    ) == "OKG_CI_DSN"
    assert os.environ["OKG_CI_DSN"] == configured


@pytest.mark.parametrize(
    "mutation",
    ["missing_doctor", "default_dlq", "cross_mcp_generation", "missing_release"],
)
def test_ci_final_proof_rejects_missing_defaulted_or_cross_bound_evidence(
    tmp_path: Path,
    mutation: str,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "phase_results": [
            {"phase": "catalog_apply", "ok": True},
            {"phase": "nomos_baseline", "ok": True},
        ]
    }))
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci", readiness_profile="ci"),
        dsn="postgres://unused",
        resolution={"target": {}},
        env={},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(
            {} if mutation == "default_dlq"
            else {"current_count": 0, "historical_count": 1}
        ),
        data={"smoke_publish": {"publishes": [{"generation_id": "gen-2"}]}},
    )
    release = {"ok": True, "release_revision": "rev-1"}
    readiness = {
        "database": {"ok": True},
        "generation": {
            "status": "published",
            "generation_id": "gen-2",
            "count_integrity": {
                "ok": True,
                "checkpoint": {"node_count": 2, "edge_count": 1},
                "pinned": {"node_count": 2, "edge_count": 1},
            },
        },
        "queues": {"active_queue_depth": 0, "current_dlq": 0},
        "source_freshness": {"ok": True, "missing": [], "stale": []},
        "source_sync_liveness": {
            "ok": True, "missing_producers": [], "stale_producers": [],
        },
        "source_sync_status": {"convergence": {"ok": True, "state": "ready"}},
        "nomos_audit": {"ok": True},
        "nomos_runtime": {"ok": True},
        "doctor": {"ok": True},
        "mcp": {"ok": True, "generation_id": "gen-2"},
    }
    if mutation == "missing_doctor":
        readiness.pop("doctor")
    elif mutation == "default_dlq":
        pass
    elif mutation == "cross_mcp_generation":
        readiness["mcp"]["generation_id"] = "gen-1"
    else:
        release.pop("release_revision")
    proof = _collect_ci_deployment_final_proof(ctx, release=release, readiness=readiness)
    assert proof["ok"] is False
    assert not all(proof["checks"].values())
