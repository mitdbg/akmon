from __future__ import annotations

from contextlib import nullcontext
import subprocess
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from okg.substrate.deployment_promotion_operations import (
    ProductionPromotionOperations,
    PromotionOperationConfig,
    _clear_restore_read_only_default,
    _database_recreation_spec,
    _file_digest,
    _launchctl_job_is_absent,
    _promotion_service_action,
    _recreate_database,
    _serial_restore_environment,
    _timescaledb_extension_version,
    _timescaledb_restore_context,
)
from okg.substrate.errors import DeploymentEnvironmentInvalid


def test_first_cutover_starts_services_after_legacy_retirement() -> None:
    assert _promotion_service_action({"ok": False, "status": "stopped"}) == "start"


def test_existing_sealed_cutover_restarts_services() -> None:
    assert _promotion_service_action({"ok": True, "status": "healthy"}) == "restart"


def test_partial_service_authority_blocks_promotion() -> None:
    with pytest.raises(DeploymentEnvironmentInvalid, match="partial or invalid"):
        _promotion_service_action({"ok": False, "status": "invalid"})


def test_live_predecessor_after_activation_routes_to_verified_restart() -> None:
    assert (
        _promotion_service_action(
            {"ok": False, "status": "invalid"},
            previous_artifact_id="sha256:" + "a" * 64,
        )
        == "restart"
    )


def test_production_restart_reaches_handoff_with_live_previous_authority(
    monkeypatch,
    tmp_path,
) -> None:
    plan = {
        "environment_id": "production",
        "adapter": "systemd",
        "systemd_scope": "user",
        "postgres_image": "okg-pg17:local",
        "launcher": {"python": "/usr/bin/python3"},
        "topology": [],
        "artifacts": [],
        "commands": {},
        "plan_digest": "sha256:" + "b" * 64,
        "release_status": {
            "artifact_id": "sha256:" + "c" * 64,
            "previous_artifact_id": "sha256:" + "a" * 64,
        },
    }
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan=plan,
            adapter="systemd",
            launcher_python="/usr/bin/python3",
            systemd_scope="user",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    production = SimpleNamespace(
        id="production",
        graph_deployment_name="okg-workspace",
        database=SimpleNamespace(dsn="postgresql://production/graph"),
        runtime=SimpleNamespace(state_root=tmp_path / "state"),
    )
    context = SimpleNamespace(
        production=production,
        state={"active_operation": {"operation_id": "sha256:" + "d" * 64}},
    )
    monkeypatch.setattr(operations, "_current_service_plan", lambda _env: plan)
    monkeypatch.setattr(
        "okg.substrate.environment_services.environment_release_handoff_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "incomplete"},
    )
    monkeypatch.setattr(
        "okg.substrate.environment_services.environment_process_authority_status",
        lambda _environment: {
            "ok": False,
            "status": "invalid",
            "message": "live predecessor differs from current definition",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.environment_services.install_environment_services",
        lambda *_args, **_kwargs: SimpleNamespace(
            to_dict=lambda: {"environment_id": "production", "paths": []}
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.environment_services.reconcile_environment_database_credentials",
        lambda _environment: {"ok": True},
    )
    observed: list[str] = []

    def execute(_plan, *, action, environment):
        assert environment is production
        observed.append(action)
        return [{"returncode": 0}]

    monkeypatch.setattr(
        "okg.substrate.environment_services.execute_environment_service_action",
        execute,
    )

    receipt = operations.restart_services(context)

    assert observed == ["install", "restart"]
    assert receipt["ok"] is True
    assert receipt["service_action"] == "restart"


def test_activation_receipt_is_true_only_after_service_handoff(
    monkeypatch,
    tmp_path,
) -> None:
    plan = {"topology": [], "artifacts": []}
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan=plan,
            adapter="systemd",
            launcher_python="/usr/bin/python3",
            systemd_scope="user",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    production = SimpleNamespace(
        id="production",
        runtime=SimpleNamespace(state_root=tmp_path / "state"),
    )
    context = SimpleNamespace(
        production=production,
        staging=SimpleNamespace(id="staging"),
        release=SimpleNamespace(artifact_id="sha256:" + "a" * 64),
        approval=SimpleNamespace(
            approved_by="operator@example.test",
            definition_digest="sha256:" + "b" * 64,
        ),
        state={"active_operation": {"operation_id": "sha256:" + "c" * 64}},
    )
    result = {
        "ok": False,
        "status": "incomplete",
        "artifact_id": context.release.artifact_id,
        "handoff": {"ok": False, "status": "incomplete"},
    }
    monkeypatch.setattr(
        "okg.substrate.environment_services.activate_environment_release_with_handoff",
        lambda *_args, **_kwargs: dict(result),
    )

    incomplete = operations.activate_release(context)

    assert incomplete["ok"] is False
    assert incomplete["authoritative"] is True
    assert incomplete["handoff"]["status"] == "incomplete"

    context.state["active_operation"]["operation_id"] = "sha256:" + "d" * 64
    result.update(
        {
            "ok": True,
            "status": "complete",
            "handoff": {"ok": True, "status": "complete"},
        }
    )
    complete = operations.activate_release(context)

    assert complete["ok"] is True
    assert complete["handoff"]["status"] == "complete"


def test_source_sync_replay_boundary_rollback_inventory_is_explicit() -> None:
    from okg.substrate.runtime.services import (
        source_sync_replay_boundary_rollback_inventory,
    )

    inventory = source_sync_replay_boundary_rollback_inventory(
        deployment="okg-workspace",
        live_version="okg-runtime-source-sync-v2",
    )

    assert inventory["rollback_mode"] == (
        "verified_database_restore_and_previous_release"
    )
    assert inventory["in_place_history_reversal"] is False
    assert inventory["restore_required"] is True
    assert inventory["sequence"][-1] == (
        "reopen writers only after readiness succeeds"
    )


def test_migrate_database_receipts_include_replay_boundary_and_rollback(
    monkeypatch,
    tmp_path,
) -> None:
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    monkeypatch.setattr(
        "okg.substrate.db.migrate.migrate",
        lambda _dsn, deployment_name: SimpleNamespace(
            statements_executed=3,
            extension_versions={"timescaledb": "2.28.2"},
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.supersede_incompatible_"
        "source_sync_workflows",
        lambda **_kwargs: {"cancelled_count": 2},
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.services.source_sync_replay_boundary_"
        "rollback_inventory",
        lambda **_kwargs: {"restore_required": True},
    )
    context = SimpleNamespace(
        production=SimpleNamespace(
            database=SimpleNamespace(dsn="postgres://production/graph"),
            graph_deployment_name="okg-workspace",
            runtime=SimpleNamespace(state_root=tmp_path / "state"),
        ),
        release=SimpleNamespace(
            inputs=SimpleNamespace(schema_checksum="sha256:" + "a" * 64)
        ),
        state={
            "active_operation": {"operation_id": "sha256:" + "b" * 64}
        },
    )

    receipt = operations.migrate_database(context)

    assert receipt["source_sync_replay_boundary"] == {"cancelled_count": 2}
    assert receipt["source_sync_rollback_inventory"] == {
        "restore_required": True
    }


@pytest.mark.postgres
def test_recreate_database_removes_candidate_only_schema(okg_dsn: str) -> None:
    dbname = f"okg_promotion_recreate_{uuid4().hex[:12]}"
    base = conninfo_to_dict(okg_dsn)
    maintenance = dict(base)
    maintenance["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**maintenance)
    target = dict(base)
    target["dbname"] = dbname
    target_dsn = make_conninfo(**target)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        # template0, as the production recreate path uses (#2034): a cluster
        # prepared by ops/pg carries TimescaleDB in template1, so a clone of
        # template1 already has the extension this test creates below.
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                sql.Identifier(dbname)
            )
        )
    try:
        with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE EXTENSION timescaledb")
            cur.execute("CREATE SCHEMA candidate_only")
            cur.execute("CREATE TABLE candidate_only.new_table (id integer)")

        recreation_spec = _database_recreation_spec(
            target_dsn,
            expected_database=dbname,
        )
        _recreate_database(
            target_dsn,
            expected_database=dbname,
            recreation_spec=recreation_spec,
        )

        with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('candidate_only.new_table')")
            assert cur.fetchone()[0] is None
            cur.execute("SHOW default_transaction_read_only")
            assert cur.fetchone()[0] == "on"

        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
                cur.execute("CREATE TABLE blocked_during_restore (id integer)")

        _clear_restore_read_only_default(
            target_dsn,
            expected_database=dbname,
        )
        with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
            cur.execute("SHOW default_transaction_read_only")
            assert cur.fetchone()[0] == "off"

        with psycopg.connect(
            maintenance_dsn, autocommit=True, connect_timeout=5
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (dbname,),
            )
            cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(dbname)))

        _recreate_database(
            target_dsn,
            expected_database=dbname,
            recreation_spec=recreation_spec,
        )
        with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == dbname
    finally:
        with psycopg.connect(
            maintenance_dsn, autocommit=True, connect_timeout=5
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (dbname,),
            )
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
            )


def test_promotion_command_error_redacts_dsn(tmp_path) -> None:
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )

    with pytest.raises(DeploymentEnvironmentInvalid) as exc_info:
        operations._run(["/usr/bin/false", "postgres://user:secret@localhost/db"])

    assert exc_info.value.data["command"] == ["/usr/bin/false", "<redacted-dsn>"]


def test_promotion_backup_keeps_database_credentials_out_of_argv(
    tmp_path,
    monkeypatch,
) -> None:
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    observed: dict[str, object] = {}

    def run(argv, *, env=None, **_kwargs):
        observed.update(argv=list(argv), env=dict(env or {}))
        output = tmp_path / "backups/production/promotion-test.dump"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"backup")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(operations, "_run", run)
    monkeypatch.setattr(operations, "_tool", lambda name: name)
    context = SimpleNamespace(
        approval=SimpleNamespace(promotion_id="promotion-test"),
        production=SimpleNamespace(
            id="production",
            database=SimpleNamespace(
                dsn=(
                    "postgresql://operator:secret@127.0.0.1:55434/"
                    "okg_workspace"
                )
            ),
            runtime=SimpleNamespace(state_root=tmp_path / "state"),
        ),
        state={
            "active_operation": {"operation_id": "sha256:" + "a" * 64}
        },
    )

    result = operations.create_backup(context)

    assert result["ok"] is True
    assert "secret" not in " ".join(observed["argv"])
    assert "postgresql://" not in " ".join(observed["argv"])
    assert observed["env"]["PGPASSWORD"] == "secret"


def test_verify_backup_drops_database_when_version_probe_fails(
    monkeypatch,
    tmp_path,
) -> None:
    backup_path = tmp_path / "promotion.dump"
    backup_path.write_bytes(b"backup")
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    context = SimpleNamespace(
        state={
            "transition_results": {
                "create_backup": {
                    "backup_id": backup_path.name,
                    "backup_path": str(backup_path),
                    "backup_digest": _file_digest(backup_path),
                }
            }
        },
        production=SimpleNamespace(
            database=SimpleNamespace(
                dsn="postgres://production/okg_workspace"
            ),
            graph_deployment_name="okg-workspace",
        ),
    )
    dropped: list[tuple[str, str]] = []

    def fail_version_probe(_dsn: str) -> str:
        raise RuntimeError("version probe failed")

    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._create_disposable_database",
        lambda _dsn: ("postgres://disposable", "okg_promotion_verify_failure"),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._timescaledb_extension_version",
        fail_version_probe,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._drop_disposable_database",
        lambda dsn, dbname: dropped.append((dsn, dbname)),
    )

    with pytest.raises(RuntimeError, match="version probe failed"):
        operations.verify_backup(context)

    assert dropped == [
        ("postgres://production/okg_workspace", "okg_promotion_verify_failure")
    ]


def test_restore_backup_keeps_read_only_guard_when_identity_fails(
    monkeypatch,
    tmp_path,
) -> None:
    backup_path = tmp_path / "promotion.dump"
    backup_path.write_bytes(b"backup")
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    context = SimpleNamespace(
        approval=SimpleNamespace(promotion_id="promotion-test"),
        state={
            "active_operation": {
                "operation_id": "sha256:" + "a" * 64,
            },
            "transition_results": {
                "create_backup": {
                    "backup_id": backup_path.name,
                    "backup_path": str(backup_path),
                    "backup_digest": _file_digest(backup_path),
                }
            },
        },
        production=SimpleNamespace(
            database=SimpleNamespace(
                dsn="postgres://production/okg_workspace",
                name="okg_workspace",
            ),
            graph_deployment_name="okg-workspace",
            runtime=SimpleNamespace(state_root=tmp_path / "state"),
        ),
    )
    cleared: list[tuple[str, str]] = []
    recreation_spec = {
        "database": "okg_workspace",
        "owner": "postgres",
        "encoding": "UTF8",
        "collate": "C",
        "ctype": "C",
        "timescaledb_version": "2.19.3",
    }
    monkeypatch.setattr(operations, "_stop_services", lambda _context: None)
    monkeypatch.setattr(operations, "_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operations, "_tool", lambda name: name)
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._database_recreation_spec",
        lambda *_args, **_kwargs: recreation_spec,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._recreate_database",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._timescaledb_restore_context",
        lambda *_args, **_kwargs: nullcontext(),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._database_identity",
        lambda *_args, **_kwargs: (True, False),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion_operations._clear_restore_read_only_default",
        lambda dsn, *, expected_database: cleared.append(
            (dsn, expected_database)
        ),
    )

    result = operations.restore_backup(context)

    assert result["ok"] is False
    assert result["schema_ok"] is True
    assert result["ownership_ok"] is False
    assert cleared == []


def test_restore_disables_parallel_maintenance_workers(monkeypatch) -> None:
    monkeypatch.setenv("PGOPTIONS", "-c statement_timeout=0")

    assert _serial_restore_environment() == {
        "PGOPTIONS": (
            "-c statement_timeout=0 "
            "-c default_transaction_read_only=off "
            "-c max_parallel_maintenance_workers=0"
        )
    }


def test_launchctl_absent_job_is_an_idempotent_stop(monkeypatch) -> None:
    observed: list[list[str]] = []

    def run(argv, **_kwargs):
        observed.append(list(argv))
        return subprocess.CompletedProcess(argv, 113, "", "Could not find service")

    monkeypatch.setattr(subprocess, "run", run)

    assert _launchctl_job_is_absent(
        ["/bin/launchctl", "bootout", "gui/501/org.okg.ui.example"]
    )
    assert observed == [["/bin/launchctl", "print", "gui/501/org.okg.ui.example"]]
    assert not _launchctl_job_is_absent(["/usr/bin/false"])
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(
            argv,
            5,
            "",
            "Input/output error",
        ),
    )
    assert not _launchctl_job_is_absent(
        ["/bin/launchctl", "bootout", "gui/501/org.okg.ui.example"]
    )


def test_stop_services_tolerates_already_absent_launchctl_job(
    monkeypatch,
    tmp_path,
) -> None:
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    monkeypatch.setattr(
        operations,
        "_current_service_plan",
        lambda _environment: {
            "commands": {
                "stop": [
                    {
                        "argv": [
                            "/bin/launchctl",
                            "bootout",
                            "gui/501/org.okg.ui.example",
                        ]
                    },
                    {
                        "argv": [
                            "docker",
                            "compose",
                            "--project-name",
                            "example",
                            "stop",
                            "postgres",
                        ]
                    },
                ]
            }
        },
    )
    observed: list[tuple[object, object]] = []
    monkeypatch.setattr(
        "okg.substrate.environment_services.fence_environment_host_services",
        lambda plan, *, environment: observed.append((plan, environment)),
    )

    production = object()
    operations._stop_services(SimpleNamespace(production=production))
    assert observed == [(operations._current_service_plan(production), production)]


def test_stop_services_delegates_attested_receipt_cleanup(
    monkeypatch,
    tmp_path,
) -> None:
    operations = ProductionPromotionOperations(
        PromotionOperationConfig(
            source_repo=tmp_path,
            approved_service_plan={},
            adapter="launchd",
            launcher_python="/usr/bin/python3",
            systemd_scope="system",
            postgres_image="okg-pg17:local",
            backup_root=tmp_path / "backups",
        )
    )
    production = SimpleNamespace(
        runtime=SimpleNamespace(
            state_root=tmp_path / "state",
            worker_service="org.okg.worker.production",
        ),
        ui=SimpleNamespace(service="org.okg.ui.production"),
    )
    monkeypatch.setattr(
        operations,
        "_current_service_plan",
        lambda _environment: {
            "commands": {
                "stop": [
                    {"argv": ["systemctl", "--user", "stop", "okg.service"]},
                    {
                        "argv": [
                            "docker",
                            "compose",
                            "--project-name",
                            "production",
                            "stop",
                            "postgres",
                        ]
                    },
                ]
            }
        },
    )
    observed: list[tuple[object, object]] = []
    monkeypatch.setattr(
        "okg.substrate.environment_services.fence_environment_host_services",
        lambda plan, *, environment: observed.append((plan, environment)),
    )

    operations._stop_services(SimpleNamespace(production=production))
    assert observed == [(operations._current_service_plan(production), production)]


@pytest.mark.postgres
def test_timescaledb_restore_context_brackets_restore_mode(okg_dsn: str) -> None:
    dbname = f"okg_promotion_timescale_{uuid4().hex[:12]}"
    base = conninfo_to_dict(okg_dsn)
    maintenance = dict(base)
    maintenance["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**maintenance)
    target = dict(base)
    target["dbname"] = dbname
    target_dsn = make_conninfo(**target)
    source_version = _timescaledb_extension_version(okg_dsn)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        cur.execute(
            sql.SQL("ALTER DATABASE {} SET default_transaction_read_only = on").format(
                sql.Identifier(dbname)
            )
        )
    try:
        with _timescaledb_restore_context(target_dsn, version=source_version):
            with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT current_setting('timescaledb.restoring'), extversion "
                    "FROM pg_extension WHERE extname = 'timescaledb'"
                )
                assert cur.fetchone() == ("on", source_version)
        with psycopg.connect(target_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT current_setting('timescaledb.restoring')")
            assert cur.fetchone()[0] == "off"
    finally:
        with psycopg.connect(
            maintenance_dsn, autocommit=True, connect_timeout=5
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (dbname,),
            )
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
            )
