"""Concrete production operations for the deployment promotion controller."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any
from uuid import uuid4

from okg.substrate.db.connection import connect as db_connect
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from okg.substrate.deployment_promotion import (
    PromotionContext,
    PromotionOperations,
    _digest,
)
from okg.substrate.deployments.environment import DeploymentEnvironment
from okg.substrate.errors import DeploymentEnvironmentInvalid
from okg.substrate.postgres_restore import (
    timescaledb_extension_version as _timescaledb_extension_version,
)
from okg.substrate.postgres_restore import (
    timescaledb_restore_context as _timescaledb_restore_context,
)
from okg.substrate.sealed_release import SealedRelease, verify_sealed_release


# How long publish_generation waits for the freshly restarted worker to
# register a DBOS application version before it refuses the enqueue.
# Bounded, so a production database that will never have a worker still
# fails the promotion rather than hanging it.
PROMOTION_RUNTIME_REGISTRATION_WAIT_SECONDS = 120.0


@dataclass(frozen=True)
class PromotionOperationConfig:
    source_repo: Path
    approved_service_plan: Mapping[str, Any]
    adapter: str
    launcher_python: str
    systemd_scope: str
    postgres_image: str
    backup_root: Path
    drain_timeout_seconds: float = 600.0
    readiness_timeout_seconds: float = 30.0


class ProductionPromotionOperations:
    """Execute promotion transitions against production authorities."""

    def __init__(self, config: PromotionOperationConfig) -> None:
        self.config = config

    def operations(self) -> PromotionOperations:
        return PromotionOperations(
            capture_previous_health=self.capture_previous_health,
            fence_writers=self.fence_writers,
            drain_work=self.drain_work,
            capture_previous_release=self.capture_previous_release,
            create_backup=self.create_backup,
            verify_backup=self.verify_backup,
            activate_release=self.activate_release,
            migrate_database=self.migrate_database,
            activate_policy=self.activate_policy,
            restart_services=self.restart_services,
            publish_generation=self.publish_generation,
            prove_production_ready=self.prove_production_ready,
            restore_backup=self.restore_backup,
            activate_previous_release=self.activate_previous_release,
            restart_previous_services=self.restart_previous_services,
            prove_previous_ready=self.prove_previous_ready,
            reopen_writers=self.reopen_writers,
            inspect_operation=self.inspect_operation,
        )

    def fence_writers(self, context: PromotionContext) -> Mapping[str, Any]:
        from okg.substrate.runtime.promotion_fence import acquire_promotion_fence

        active = context.state["active_operation"]
        row = acquire_promotion_fence(
            deployment=context.production.graph_deployment_name,
            dsn=context.production.database.dsn,
            promotion_id=context.approval.promotion_id,
            metadata={
                "artifact_id": context.approval.artifact_id,
                "operation_id": active["operation_id"],
            },
        )
        row.pop("receipt_digest", None)
        return self._receipt(context, "fence_writers", row)

    def drain_work(self, context: PromotionContext) -> Mapping[str, Any]:
        from okg.substrate.runtime.admin import client_drain

        result = client_drain(
            deployment=context.production.graph_deployment_name,
            dsn=context.production.database.dsn,
            max_wait_seconds=self.config.drain_timeout_seconds,
            stable_zero_seconds=0.0,
        )
        return self._receipt(
            context,
            "drain_work",
            {
                "drained": result.drained,
                "pending_workflows": result.in_flight_at_timeout,
                "elapsed_seconds": result.elapsed_seconds,
                "pending": result.pending_workflows,
            },
            ok=result.drained,
        )

    def capture_previous_release(self, context: PromotionContext) -> Mapping[str, Any]:
        from okg.substrate.release_activation import environment_release_status

        status = environment_release_status(context.production)
        return self._receipt(
            context,
            "capture_previous_release",
            {
                "artifact_id": status.get("artifact_id"),
                "input_id": status.get("input_id"),
                "release_status": status,
            },
            ok=bool(status.get("ok") and status.get("artifact_id")),
        )

    def capture_previous_health(self, context: PromotionContext) -> Mapping[str, Any]:
        """Record the generation-pinned baseline without requiring it to be clean."""
        from okg.substrate.environment_readiness import environment_readiness_report

        previous = context.state["transition_results"]["capture_previous_release"]
        artifact_id = str(previous["artifact_id"])
        plan = self._current_service_plan(context.production)
        self._require_approved_service_configuration(plan)
        report = environment_readiness_report(
            context.production,
            plan,
            timeout_seconds=self.config.readiness_timeout_seconds,
        )
        observed = (report.get("release") or {}).get("artifact_id")
        generation_id = (report.get("generation") or {}).get("generation_id")
        return self._receipt(
            context,
            "capture_previous_health",
            {
                "artifact_id": observed,
                "generation_id": generation_id,
                "health_ok": bool(report.get("ok")),
                "plan_digest": plan["plan_digest"],
                "readiness": report,
            },
            ok=bool(observed == artifact_id and generation_id),
        )

    def create_backup(self, context: PromotionContext) -> Mapping[str, Any]:
        backup_id = f"{context.approval.promotion_id}.dump"
        path = self.config.backup_root / context.production.id / backup_id
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.chmod(0o700)
        database, child_env = _libpq_command_target(
            context.production.database.dsn
        )
        self._run(
            [
                self._tool("pg_dump"),
                "--format=custom",
                "--file",
                str(path),
                database,
            ],
            env=child_env,
        )
        path.chmod(0o600)
        return self._receipt(
            context,
            "create_backup",
            {
                "backup_id": backup_id,
                "backup_path": str(path),
                "backup_digest": _file_digest(path),
                "size_bytes": path.stat().st_size,
            },
        )

    def verify_backup(self, context: PromotionContext) -> Mapping[str, Any]:
        backup = self._backup_receipt(context)
        path = Path(str(backup["backup_path"]))
        if _file_digest(path) != backup["backup_digest"]:
            raise _error("promotion backup digest changed", backup_path=str(path))
        disposable_dsn, dbname = _create_disposable_database(
            context.production.database.dsn
        )
        try:
            timescaledb_version = _timescaledb_extension_version(
                context.production.database.dsn
            )
            with _timescaledb_restore_context(
                disposable_dsn,
                version=timescaledb_version,
            ):
                database, child_env = _libpq_command_target(disposable_dsn)
                child_env.update(_serial_restore_environment())
                self._run(
                    [
                        self._tool("pg_restore"),
                        "--exit-on-error",
                        "--no-owner",
                        "--dbname",
                        database,
                        str(path),
                    ],
                    env=child_env,
                )
            schema_ok, ownership_ok = _database_identity(
                disposable_dsn,
                context.production.graph_deployment_name,
            )
        finally:
            _drop_disposable_database(context.production.database.dsn, dbname)
        return self._receipt(
            context,
            "verify_backup",
            {
                "backup_id": backup["backup_id"],
                "backup_digest": backup["backup_digest"],
                "restore_verified": True,
                "schema_ok": schema_ok,
                "ownership_ok": ownership_ok,
            },
            ok=schema_ok and ownership_ok,
        )

    def activate_release(self, context: PromotionContext) -> Mapping[str, Any]:
        return self._activate(context, context.release, "activate_release")

    def migrate_database(self, context: PromotionContext) -> Mapping[str, Any]:
        from okg.substrate.db.migrate import migrate
        from okg.substrate.runtime.services import (
            source_sync_replay_boundary_rollback_inventory,
            supersede_incompatible_source_sync_workflows,
        )
        from okg.substrate.runtime.source_sync_constants import (
            SOURCE_SYNC_APPLICATION_VERSION,
        )

        result = migrate(
            context.production.database.dsn,
            deployment_name=context.production.graph_deployment_name,
        )
        replay_boundary = supersede_incompatible_source_sync_workflows(
            dsn=context.production.database.dsn,
            deployment=context.production.graph_deployment_name,
            live_version=SOURCE_SYNC_APPLICATION_VERSION,
        )
        rollback_inventory = source_sync_replay_boundary_rollback_inventory(
            deployment=context.production.graph_deployment_name,
            live_version=SOURCE_SYNC_APPLICATION_VERSION,
        )
        return self._receipt(
            context,
            "migrate_database",
            {
                "statements_executed": result.statements_executed,
                "extension_versions": result.extension_versions,
                "schema_checksum": context.release.inputs.schema_checksum,
                "source_sync_replay_boundary": replay_boundary,
                "source_sync_rollback_inventory": rollback_inventory,
            },
        )

    def activate_policy(self, context: PromotionContext) -> Mapping[str, Any]:
        claim = self._run_release_cli(
            context,
            "catalog",
            "ownership",
            "claim",
            "--deployment",
            context.production.id,
            "--force",
            "--json",
        )
        ownership = _json_stdout(claim)
        if ownership.get("claimed") is not True:
            raise _error(
                "approved sealed artifact did not claim catalog ownership",
                ownership_status=ownership.get("status"),
            )
        result = self._run_release_cli(
            context,
            "catalog",
            "load",
            "--deployment",
            context.production.id,
            "--apply",
            "--json",
        )
        payload = _json_stdout(result)
        return self._receipt(
            context,
            "activate_policy",
            {
                "catalog_fingerprint": context.release.inputs.catalog_fingerprint,
                "ownership": ownership,
                "catalog": payload,
            },
        )

    def restart_services(self, context: PromotionContext) -> Mapping[str, Any]:
        return self._restart(context, "restart_services")

    def publish_generation(self, context: PromotionContext) -> Mapping[str, Any]:
        from okg.substrate.publish_proof import (
            assert_publish_proof,
            surface_capacity_warning,
        )
        from okg.substrate.runtime.admin import client_enqueue_publisher_cycle

        result = client_enqueue_publisher_cycle(
            deployment=context.production.graph_deployment_name,
            dsn=context.production.database.dsn,
            promotion_id=context.approval.promotion_id,
            # restart_services runs immediately before this step and does
            # not itself wait for the worker to register a DBOS
            # application version. On a first-ever promotion onto a fresh
            # production database that registration can land after this
            # call starts, so wait a bounded window rather than refusing
            # a worker that is still booting.
            registration_wait_seconds=(
                PROMOTION_RUNTIME_REGISTRATION_WAIT_SECONDS
            ),
        )
        summary = result.summary or {}
        proof = assert_publish_proof(summary, dsn=context.production.database.dsn)
        # Decision 23a: an over-bar lock hold passes; keep it visible.
        capacity_warning = surface_capacity_warning(
            proof, surface="promotion publish_generation",
        )
        return self._receipt(
            context,
            "publish_generation",
            {
                "workflow_id": result.workflow_id,
                "workflow_status": result.status,
                "generation_id": summary.get("generation_id"),
                "publish_proof": proof,
                **(
                    {"capacity_warning": capacity_warning}
                    if capacity_warning is not None
                    else {}
                ),
            },
            ok=result.status == "SUCCESS" and bool(proof.get("ok")),
        )

    def prove_production_ready(self, context: PromotionContext) -> Mapping[str, Any]:
        return self._readiness(
            context,
            "prove_production_ready",
            context.approval.artifact_id,
        )

    def restore_backup(self, context: PromotionContext) -> Mapping[str, Any]:
        backup = self._backup_receipt(context)
        path = Path(str(backup["backup_path"]))
        if _file_digest(path) != backup["backup_digest"]:
            raise _error("refusing restore from a changed backup", backup_path=str(path))
        self._stop_services(context)
        recreation_path = (
            context.production.runtime.state_root
            / "promotions/recovery"
            / f"{context.approval.promotion_id}.database.json"
        )
        if recreation_path.is_file() and not recreation_path.is_symlink():
            recreation_spec = json.loads(recreation_path.read_text(encoding="utf-8"))
        else:
            recreation_spec = _database_recreation_spec(
                context.production.database.dsn,
                expected_database=context.production.database.name,
            )
            _atomic_write_json(recreation_path, recreation_spec)
        _recreate_database(
            context.production.database.dsn,
            expected_database=context.production.database.name,
            recreation_spec=recreation_spec,
        )
        with _timescaledb_restore_context(
            context.production.database.dsn,
            version=str(recreation_spec["timescaledb_version"]),
        ):
            database, child_env = _libpq_command_target(
                context.production.database.dsn
            )
            child_env.update(_serial_restore_environment())
            self._run(
                [
                    self._tool("pg_restore"),
                    "--exit-on-error",
                    "--no-owner",
                    "--dbname",
                    database,
                    str(path),
                ],
                env=child_env,
            )
        schema_ok, ownership_ok = _database_identity(
            context.production.database.dsn,
            context.production.graph_deployment_name,
        )
        if schema_ok and ownership_ok:
            _clear_restore_read_only_default(
                context.production.database.dsn,
                expected_database=context.production.database.name,
            )
        return self._receipt(
            context,
            "restore_backup",
            {
                "backup_id": backup["backup_id"],
                "backup_digest": backup["backup_digest"],
                "schema_ok": schema_ok,
                "ownership_ok": ownership_ok,
            },
            ok=schema_ok and ownership_ok,
        )

    def activate_previous_release(self, context: PromotionContext) -> Mapping[str, Any]:
        previous = context.state["transition_results"]["capture_previous_release"]
        artifact_id = str(previous["artifact_id"])
        root = (
            context.production.release.artifact_store
            / artifact_id.removeprefix("sha256:")
        )
        release = verify_sealed_release(root, expected_artifact_id=artifact_id)
        return self._activate(context, release, "activate_previous_release")

    def restart_previous_services(self, context: PromotionContext) -> Mapping[str, Any]:
        return self._restart(context, "restart_previous_services")

    def prove_previous_ready(self, context: PromotionContext) -> Mapping[str, Any]:
        previous = context.state["transition_results"]["capture_previous_release"]
        return self._readiness(
            context,
            "prove_previous_ready",
            str(previous["artifact_id"]),
        )

    def reopen_writers(self, context: PromotionContext) -> Mapping[str, Any]:
        from okg.substrate.runtime.promotion_fence import release_promotion_fence

        row = release_promotion_fence(
            deployment=context.production.graph_deployment_name,
            dsn=context.production.database.dsn,
            promotion_id=context.approval.promotion_id,
        )
        row.pop("receipt_digest", None)
        return self._receipt(context, "reopen_writers", row)

    def inspect_operation(
        self, context: PromotionContext, name: str, operation_id: str
    ) -> Mapping[str, Any] | None:
        path = self._receipt_path(context, operation_id)
        if not path.is_file() or path.is_symlink():
            recovery_operations = {
                "restore_backup": self.restore_backup,
                "activate_previous_release": self.activate_previous_release,
                "restart_previous_services": self.restart_previous_services,
                "prove_previous_ready": self.prove_previous_ready,
                "reopen_writers": self.reopen_writers,
            }
            recovery = recovery_operations.get(name)
            return recovery(context) if recovery is not None else None
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("operation") != name or raw.get("operation_id") != operation_id:
            raise _error("promotion operation receipt identity changed", path=str(path))
        return raw

    def _activate(
        self, context: PromotionContext, release: SealedRelease, operation: str
    ) -> Mapping[str, Any]:
        from okg.substrate.environment_services import (
            activate_environment_release_with_handoff,
        )

        result = activate_environment_release_with_handoff(
            context.production,
            release,
            source_repo=self.config.source_repo,
            activated_by=context.approval.approved_by,
            adapter=self.config.adapter,
            launcher_python=self.config.launcher_python,
            systemd_scope=self.config.systemd_scope,
            postgres_image=self.config.postgres_image,
            staging_environment=context.staging,
            approved_definition_digest=context.approval.definition_digest,
            plan_validator=self._require_approved_service_configuration,
        )
        fields = dict(result)
        ok = fields.pop("ok", False) is True
        return self._receipt(context, operation, fields, ok=ok)

    def _restart(self, context: PromotionContext, operation: str) -> Mapping[str, Any]:
        from okg.substrate.environment_services import (
            environment_release_handoff_status,
            environment_service_lifecycle_transaction,
            environment_process_authority_status,
            execute_environment_service_action,
            install_environment_services,
            reconcile_environment_database_credentials,
        )

        plan = self._current_service_plan(context.production)
        self._require_approved_service_configuration(plan)
        release_status = plan.get("release_status") or {}
        handoff = environment_release_handoff_status(
            context.production,
            expected_artifact_id=release_status.get("artifact_id"),
            expected_definition_digest=release_status.get("definition_digest"),
            expected_pointer_digest=release_status.get("active_pointer_digest"),
        )
        if handoff.get("ok") is True:
            return self._receipt(
                context,
                operation,
                {
                    "artifact_id": release_status.get("artifact_id"),
                    "plan_digest": plan["plan_digest"],
                    "service_action": "none",
                    "handoff": handoff,
                    "legacy_shutdown": None,
                    "database_credentials": None,
                    "installed": None,
                    "install_results": [],
                    "results": [],
                },
            )
        authority = environment_process_authority_status(context.production)
        action = _promotion_service_action(
            authority,
            previous_artifact_id=(plan.get("release_status") or {}).get(
                "previous_artifact_id"
            ),
        )
        legacy_shutdown = None
        if action == "start":
            legacy_shutdown = _json_stdout(
                self._run(
                    [
                        self._tool("uv"),
                        "run",
                        "okg",
                        "dogfood",
                        "down",
                        "--deployment",
                        context.production.graph_deployment_name,
                        "--adapter",
                        self.config.adapter,
                        "--apply",
                        "--json",
                    ],
                    env={"OKG_DSN": context.production.database.dsn},
                    cwd=self.config.source_repo,
                )
            )
        with environment_service_lifecycle_transaction(context.production):
            installed = install_environment_services(context.production, plan)
            credentials = reconcile_environment_database_credentials(
                context.production
            )
            install_results = execute_environment_service_action(
                plan,
                action="install",
                environment=context.production,
            )
            if not all(row.get("returncode") == 0 for row in install_results):
                results = install_results
            else:
                results = execute_environment_service_action(
                    plan,
                    action=action,
                    environment=context.production,
                )
        ok = all(row.get("returncode") == 0 for row in results)
        return self._receipt(
            context,
            operation,
            {
                "artifact_id": (plan.get("release_status") or {}).get("artifact_id"),
                "plan_digest": plan["plan_digest"],
                "service_action": action,
                "legacy_shutdown": legacy_shutdown,
                "database_credentials": credentials,
                "installed": installed.to_dict(),
                "install_results": install_results,
                "results": results,
            },
            ok=ok,
        )

    def _run_release_cli(
        self,
        context: PromotionContext,
        *args: str,
    ) -> subprocess.CompletedProcess[str]:
        descriptor = (
            context.production.runtime.state_root / "release-launcher.json"
        )
        launcher = (
            context.release.repo_root
            / "src/okg/substrate/runtime/release_launcher.py"
        )
        return self._run(
            [
                "/usr/bin/python3",
                str(launcher),
                "--descriptor",
                str(descriptor),
                "--",
                "-m",
                "okg.substrate.cli.main",
                *args,
            ],
            env={"OKG_DSN": context.production.database.dsn},
            cwd=context.release.repo_root,
        )

    def _stop_services(self, context: PromotionContext) -> None:
        from okg.substrate.environment_services import (
            fence_environment_host_services,
        )

        plan = self._current_service_plan(context.production)
        fence_environment_host_services(
            plan,
            environment=context.production,
        )

    def _readiness(
        self, context: PromotionContext, operation: str, artifact_id: str
    ) -> Mapping[str, Any]:
        from okg.substrate.environment_readiness import environment_readiness_report

        plan = self._current_service_plan(context.production)
        self._require_approved_service_configuration(plan)
        report = environment_readiness_report(
            context.production,
            plan,
            timeout_seconds=self.config.readiness_timeout_seconds,
        )
        observed = (report.get("release") or {}).get("artifact_id")
        ok = bool(report.get("ok") and observed == artifact_id)
        return self._receipt(
            context,
            operation,
            {
                "artifact_id": observed,
                "generation_id": (report.get("generation") or {}).get("generation_id"),
                "plan_digest": plan["plan_digest"],
                "readiness": report,
            },
            ok=ok,
        )

    def _current_service_plan(
        self, environment: DeploymentEnvironment
    ) -> Mapping[str, Any]:
        from okg.substrate.environment_services import render_environment_services

        return render_environment_services(
            environment,
            adapter=self.config.adapter,
            launcher_python=self.config.launcher_python,
            systemd_scope=self.config.systemd_scope,
            postgres_image=self.config.postgres_image,
        )

    def _require_approved_service_configuration(self, plan: Mapping[str, Any]) -> None:
        expected = _service_configuration_digest(self.config.approved_service_plan)
        observed = _service_configuration_digest(plan)
        if expected != observed:
            raise _error(
                "production service configuration changed after approval",
                expected=expected,
                observed=observed,
            )

    def _backup_receipt(self, context: PromotionContext) -> Mapping[str, Any]:
        backup = context.state["transition_results"].get("create_backup")
        if not isinstance(backup, Mapping):
            raise _error("promotion backup receipt is missing")
        return backup

    def _receipt(
        self,
        context: PromotionContext,
        operation: str,
        fields: Mapping[str, Any],
        *,
        ok: bool = True,
    ) -> dict[str, Any]:
        active = context.state["active_operation"]
        receipt = {
            "ok": ok,
            "operation": operation,
            "operation_id": active["operation_id"],
            "authoritative": True,
            **dict(fields),
        }
        receipt["receipt_digest"] = _digest(receipt)
        _atomic_write_json(self._receipt_path(context, active["operation_id"]), receipt)
        return receipt

    def _receipt_path(self, context: PromotionContext, operation_id: str) -> Path:
        digest = operation_id.removeprefix("sha256:")
        return context.production.runtime.state_root / "promotions/receipts" / f"{digest}.json"

    def _run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        values = dict(os.environ)
        values.update(env or {})
        result = subprocess.run(
            list(argv),
            text=True,
            capture_output=True,
            env=values,
            cwd=cwd,
            check=False,
        )
        if result.returncode != 0:
            raise _error(
                "promotion command failed",
                command=["<redacted-dsn>" if "://" in item else item for item in argv],
                returncode=result.returncode,
                stdout=result.stdout[-4000:],
                stderr=result.stderr[-4000:],
            )
        return result

    @staticmethod
    def _tool(name: str) -> str:
        path = shutil.which(name)
        if path is None:
            raise _error("required promotion tool is missing", tool=name)
        return path


def _service_configuration_digest(plan: Mapping[str, Any]) -> str:
    artifacts = [
        dict(item)
        for item in plan.get("artifacts") or ()
        if isinstance(item, Mapping)
    ]
    return _digest(
        {
            "environment_id": plan.get("environment_id"),
            "adapter": plan.get("adapter"),
            "systemd_scope": plan.get("systemd_scope"),
            "postgres_image": plan.get("postgres_image"),
            "launcher": plan.get("launcher"),
            "topology": plan.get("topology"),
            "artifacts": artifacts,
            "commands": plan.get("commands"),
        }
    )


def _is_database_stop_command(argv: Sequence[str]) -> bool:
    return bool(
        argv
        and Path(argv[0]).name == "docker"
        and "compose" in argv
        and len(argv) >= 2
        and list(argv[-2:]) == ["stop", "postgres"]
    )


def _database_identity(dsn: str, deployment: str) -> tuple[bool, bool]:
    with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.schema_migrations')")
        schema_ok = cur.fetchone()[0] is not None
        cur.execute("SELECT to_regclass('okg.database_owner')")
        if cur.fetchone()[0] is None:
            return schema_ok, False
        cur.execute(
            "SELECT owner_deployment_name FROM okg.database_owner WHERE singleton"
        )
        row = cur.fetchone()
        return schema_ok, bool(row and row[0] == deployment)


def _create_disposable_database(base_dsn: str) -> tuple[str, str]:
    info = conninfo_to_dict(base_dsn)
    dbname = f"okg_promotion_verify_{uuid4().hex[:16]}"
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    with db_connect(
        make_conninfo(**maintenance), autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        # TEMPLATE template0 is load-bearing: the default template1 on an
        # ops/pg image already carries TimescaleDB at the CLUSTER's version
        # (issue #1972), so a database cloned from it would make the
        # caller's version-pinned CREATE EXTENSION no-op at the wrong
        # version and timescaledb_restore_context() would refuse.
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                sql.Identifier(dbname)
            )
        )
    target = dict(info)
    target["dbname"] = dbname
    return make_conninfo(**target), dbname


def _database_recreation_spec(
    base_dsn: str,
    *,
    expected_database: str,
) -> dict[str, str]:
    info = conninfo_to_dict(base_dsn)
    dbname = str(info.get("dbname") or "")
    if dbname != expected_database or dbname in {"", "postgres", "template0", "template1"}:
        raise _error(
            "refusing to recreate an unexpected production database",
            expected=expected_database,
            observed=dbname,
        )
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    with db_connect(
        make_conninfo(**maintenance), autocommit=True, connect_timeout=5
    ) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT pg_get_userbyid(datdba) AS owner,
                   pg_encoding_to_char(encoding) AS encoding,
                   datcollate AS collate,
                   datctype AS ctype
              FROM pg_database
             WHERE datname = %s
            """,
            (dbname,),
        )
        database = cur.fetchone()
        if database is None:
            raise _error("production database disappeared before restore", database=dbname)
    return {
        "database": dbname,
        "owner": str(database["owner"]),
        "encoding": str(database["encoding"]),
        "collate": str(database["collate"]),
        "ctype": str(database["ctype"]),
        "timescaledb_version": _timescaledb_extension_version(base_dsn),
    }


def _recreate_database(
    base_dsn: str,
    *,
    expected_database: str,
    recreation_spec: Mapping[str, Any] | None = None,
) -> None:
    info = conninfo_to_dict(base_dsn)
    dbname = str(info.get("dbname") or "")
    if dbname != expected_database or dbname in {"", "postgres", "template0", "template1"}:
        raise _error(
            "refusing to recreate an unexpected production database",
            expected=expected_database,
            observed=dbname,
        )
    spec = dict(
        recreation_spec
        or _database_recreation_spec(
            base_dsn,
            expected_database=expected_database,
        )
    )
    if spec.get("database") != dbname:
        raise _error(
            "database recreation specification belongs to another database",
            expected=dbname,
            observed=spec.get("database"),
        )
    required = ("owner", "encoding", "collate", "ctype", "timescaledb_version")
    if any(not isinstance(spec.get(field), str) or not spec[field] for field in required):
        raise _error("database recreation specification is incomplete")
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    with db_connect(
        make_conninfo(**maintenance), autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
        )
        cur.execute(
            sql.SQL(
                "CREATE DATABASE {} WITH OWNER {} ENCODING {} "
                "LC_COLLATE {} LC_CTYPE {} TEMPLATE template0"
            ).format(
                sql.Identifier(dbname),
                sql.Identifier(spec["owner"]),
                sql.Literal(spec["encoding"]),
                sql.Literal(spec["collate"]),
                sql.Literal(spec["ctype"]),
            )
        )
        cur.execute(
            sql.SQL("ALTER DATABASE {} SET default_transaction_read_only = on").format(
                sql.Identifier(dbname)
            )
        )


def _clear_restore_read_only_default(
    base_dsn: str,
    *,
    expected_database: str,
) -> None:
    info = conninfo_to_dict(base_dsn)
    dbname = str(info.get("dbname") or "")
    if dbname != expected_database or dbname in {"", "postgres", "template0", "template1"}:
        raise _error(
            "refusing to clear the restore guard on an unexpected database",
            expected=expected_database,
            observed=dbname,
        )
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    with db_connect(
        make_conninfo(**maintenance), autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER DATABASE {} RESET default_transaction_read_only").format(
                sql.Identifier(dbname)
            )
        )


def _drop_disposable_database(base_dsn: str, dbname: str) -> None:
    if not dbname.startswith("okg_promotion_verify_"):
        raise _error("refusing to drop an unexpected backup verification database")
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    with db_connect(
        make_conninfo(**info), autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(dbname)))


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _serial_restore_environment() -> dict[str, str]:
    existing = os.environ.get("PGOPTIONS", "").strip()
    restore_options = (
        "-c default_transaction_read_only=off -c max_parallel_maintenance_workers=0"
    )
    return {"PGOPTIONS": " ".join(part for part in (existing, restore_options) if part)}


def _libpq_command_target(dsn: str) -> tuple[str, dict[str, str]]:
    """Return a database name plus libpq env without placing secrets on argv."""
    info = conninfo_to_dict(dsn)
    database = str(info.get("dbname") or "")
    if not database:
        raise _error("database DSN does not name a database")
    env: dict[str, str] = {}
    mapping = {
        "host": "PGHOST",
        "hostaddr": "PGHOSTADDR",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
    }
    for key, variable in mapping.items():
        value = info.get(key)
        if value is not None:
            env[variable] = str(value)
    env["PGDATABASE"] = database
    return database, env


def _launchctl_job_is_absent(argv: Sequence[str]) -> bool:
    if len(argv) != 3 or Path(argv[0]).name != "launchctl" or argv[1] != "bootout":
        return False
    target = argv[2]
    if not target.startswith("gui/") or target.count("/") < 2:
        return False
    result = subprocess.run(
        [argv[0], "print", target],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 113 and "Could not find service" in result.stderr


def _promotion_service_action(
    authority: Mapping[str, Any],
    *,
    previous_artifact_id: Any = None,
) -> str:
    status = str(authority.get("status") or "")
    if authority.get("ok") and status == "healthy":
        return "restart"
    if previous_artifact_id:
        # After activation, a live predecessor is intentionally invalid against
        # the current definition. The restart inventory performs the exact
        # predecessor receipt, ancestry, manager, and process checks.
        return "restart"
    if status == "stopped":
        return "start"
    raise _error(
        "production environment process authority is partial or invalid",
        authority_status=status or "unknown",
    )


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _json_stdout(result: subprocess.CompletedProcess[str]) -> Mapping[str, Any]:
    try:
        payload = json.loads(result.stdout)
    except ValueError as exc:
        raise _error("promotion command returned invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise _error("promotion command JSON is not an object")
    return payload


def _error(message: str, **data: Any) -> DeploymentEnvironmentInvalid:
    return DeploymentEnvironmentInvalid(
        message,
        data=data,
        fix_hint="recover the active promotion before accepting production work",
    )
