from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate import deployment_apply as deployment_apply_module
from okg.substrate.deployment_apply import (
    ApplyCommandResult,
    _default_runner,
    _enclosing_checkout,
    _observed_runtime_authority as _real_observed_runtime_authority,
    _validate_source_enqueue,
    apply_deployment_plan,
    deployment_apply_health,
    plan_deployment_apply,
    record_worker_acknowledgment,
)
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import SubstrateError
from okg.substrate.cli.main import main
from scripts.prove_deployment_apply import _queue_counts


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _observe_test_worker_from_desired_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate a replacement process booted from each test deployment root."""
    from okg.substrate import deployment_apply as deployment_apply_module

    def observe(*, deployment, deployment_dir, dsn):
        desired = deployment_apply_module.desired_deployment_state(
            deployment=deployment,
            deployment_dir=deployment_dir,
            dsn=dsn,
        )
        expected = desired["service_authority"]
        return {
            "authority_source": "running_worker_process",
            "deployment": deployment,
            **{
                field: expected[field]
                for field in (
                    "working_directory",
                    "process_executable",
                    "interpreter",
                    "import_root",
                    "code_revision",
                    "database",
                )
            },
        }

    monkeypatch.setattr(
        deployment_apply_module,
        "_observed_runtime_authority",
        observe,
    )


def _deployment(tmp_path: Path, name: str) -> Path:
    root = tmp_path / name
    root.mkdir()
    (root / "deployment.yaml").write_text(
        f"name: {name}\nmodules: []\nsource_registry: source_registry.yaml\n"
        "runtime:\n  enabled: false\n",
        encoding="utf-8",
    )
    (root / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "tests@okg.example"],
        cwd=root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "OKG Tests"],
        cwd=root,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(
        ["git", "commit", "-qm", "test deployment"],
        cwd=root,
        check=True,
    )
    return root


def _reset_apply_state(dsn: str, deployment: str) -> None:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for table in (
            "okg.deployment_apply_runs",
            "okg.deployment_worker_ack",
            "okg.deployment_write_ownership",
        ):
            cur.execute("SELECT to_regclass(%s)", (table,))
            if cur.fetchone()[0] is not None:
                cur.execute(f"DELETE FROM {table} WHERE deployment_name = %s", (deployment,))
        # Stable deployment names make a repeated local run prove the same
        # behavior. Clear catalog history from the prior invocation; without
        # this, the fail-closed ownership guard correctly rejects the shared
        # test database as unclaimed mixed history.
        cur.execute("SELECT to_regclass('okg.catalog_versions')")
        if cur.fetchone()[0] is not None:
            cur.execute(
                "DELETE FROM okg.catalog_versions WHERE deployment_name = %s",
                (deployment,),
            )
        conn.commit()


def _service_install_success(
    argv: list[str],
    env: dict[str, str],
    *,
    deployment: str,
    dsn: str,
    working_directory: Path,
) -> ApplyCommandResult:
    # Issue #2301: the running okg, not `uv run okg` from the cwd.
    assert argv[:4] == running_okg_argv()
    assert argv[4:7] == ["dogfood", "service", "install"]
    assert argv[argv.index("--deployment") + 1] == deployment
    adapter = argv[argv.index("--adapter") + 1]
    assert adapter in {"launchd", "systemd"}
    assert argv[argv.index("--repo-root") + 1] == str(
        working_directory.resolve()
    )
    assert argv[argv.index("--dsn") + 1] == "${OKG_DEPLOYMENT_APPLY_DSN}"
    assert dsn not in json.dumps(argv)
    assert env == {"OKG_DEPLOYMENT_APPLY_DSN": dsn}
    return ApplyCommandResult(
        0,
        json.dumps({
            "schema": "okg.dogfood.service_adapter.v1",
            "ok": True,
            "deployment": deployment,
            "adapter": adapter,
            "destination": "/tmp/test-runtime-worker-service",
            "written": "/tmp/test-runtime-worker-service",
            "content_sha256": "sha256:test-service-content",
        }),
    )


def test_worker_reload_dry_run_then_apply_converges_and_replay_is_write_free(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.identity req.runtime-authority.reconcile "
        "test.runtime-authority.identity test.runtime-authority.reconcile",
    )
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-e2e"
    root = _deployment(tmp_path, deployment)
    # Worker reload runs only with the runtime on: with it off,
    # `okg runtime worker` refuses to start (issue #3628).
    _enable_runtime(root)
    _reset_apply_state(test_db_dsn, deployment)

    dry_run = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    before_health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert before_health["ok"] is False
    assert before_health["status"] == "blocked"
    # Genuine drift blocks.
    assert "stage_needed:catalog_ownership" in before_health["problems"]
    # The runtime is on, so a missing worker acknowledgement is a problem.
    assert "stage_needed:worker_reload" in before_health["problems"]
    assert dry_run["mode"] == "dry-run"
    assert [row["stage"] for row in dry_run["actions"]] == [
        "catalog_ownership", "catalog_apply", "nomos_baseline",
        "source_supervision", "worker_reload",
    ]
    assert dry_run["live"]["receipt"] is None
    # Issue #3580: a dry run that needs work says it applied nothing, which
    # stages are pending, and the exact command that applies them.
    assert dry_run["ok"] is True
    assert dry_run["applied"] is False
    assert dry_run["pending_stages"] == [
        row["stage"] for row in dry_run["actions"] if row["needed"]
    ]
    assert "catalog_ownership" in dry_run["pending_stages"]
    assert dry_run["next_command"].endswith(
        f"okg deployment apply {deployment} --apply --confirm {dry_run['plan_id']}"
    ), dry_run["next_command"]
    assert dry_run["summary"].startswith("dry run: nothing was applied;")
    assert dry_run["next_command"] in dry_run["summary"]

    calls: list[list[str]] = []

    def runner(argv, env):
        calls.append(argv)
        if argv[4:7] == ["dogfood", "service", "install"]:
            return _service_install_success(
                argv, env, deployment=deployment, dsn=test_db_dsn,
                working_directory=root,
            )
        assert argv[4:6] == ["dogfood", "restart"]
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment=deployment,
            deployment_dir=root,
            active_sources=[],
            active_schedules=[],
        )
        return ApplyCommandResult(0, json.dumps({"ok": True, "adapter": "test"}))

    applied = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=dry_run["plan_id"],
        command_runner=runner,
        worker_ack_timeout_seconds=1,
    )
    assert applied["applied"] is True
    assert applied["no_op"] is True
    worker_receipt = applied["stage_receipts"]["worker_reload"]["postcondition"]
    assert worker_receipt["service_install"]["content_sha256"] == (
        "sha256:test-service-content"
    )
    assert worker_receipt["ack"]["service_authority"] == (
        dry_run["desired_state"]["service_authority"]
    )
    assert worker_receipt["ack"]["observed_runtime_authority"] == {
        "authority_source": "running_worker_process",
        "deployment": deployment,
        **{
            field: dry_run["desired_state"]["service_authority"][field]
            for field in (
                "working_directory",
                "process_executable",
                "interpreter",
                "import_root",
                "code_revision",
                "database",
            )
        },
    }
    assert [argv[4:7] for argv in calls] == [
        ["dogfood", "service", "install"],
        ["dogfood", "restart", "--deployment"],
    ]
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is True

    replay_plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert replay_plan["no_op"] is True
    assert replay_plan["pending_stages"] == []
    assert replay_plan["next_command"] is None
    assert replay_plan["summary"] == (
        "dry run: the deployment is converged; nothing to apply"
    )
    before_calls = len(calls)
    replay = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=replay_plan["plan_id"],
        command_runner=runner,
    )
    assert replay["write_free"] is True
    # A real apply never inherits the dry run's wording.
    assert replay["summary"] == "applied nothing: the deployment was already converged"
    assert applied["summary"].startswith("applied ")
    assert "dry run" not in applied["summary"]
    for stage in applied["completed_stages"]:
        assert stage in applied["summary"]
    assert len(calls) == before_calls

    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.catalog_versions
               SET catalog_fingerprint = 'sha256:catalog-only-drift'
             WHERE catalog_version_id = (
               SELECT catalog_version_id FROM okg.catalog_versions
                WHERE deployment_name = %s AND status = 'applied'
                ORDER BY catalog_version_id DESC LIMIT 1
             )
            """,
            (deployment,),
        )
        conn.commit()
    catalog_only = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    needed = {row["stage"]: row["needed"] for row in catalog_only["actions"]}
    assert needed["catalog_apply"] is True
    assert needed["source_supervision"] is False
    assert needed["worker_reload"] is False


def test_deployment_apply_bootstraps_absent_launchagent_and_records_exact_ack(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.reconcile test.runtime-authority.reconcile",
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.platform.system", lambda: "Darwin",
    )
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-absent-service"
    root = _deployment(tmp_path, deployment)
    # Worker reload runs only with the runtime on: with it off,
    # `okg runtime worker` refuses to start (issue #3628).
    _enable_runtime(root)
    _reset_apply_state(test_db_dsn, deployment)
    plan = plan_deployment_apply(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
    )
    command_batches: list[list[str]] = []
    audits = [
        {
            "ok": False,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
        {
            "ok": True,
            "status": "single_executor",
            "process_count": 2,
            "root_count": 1,
            "split_executor": False,
            "roots": [{
                "pid": 303,
                "ppid": 1,
                "command": (
                    f"uv run okg runtime worker --deployment {deployment}"
                ),
            }],
            "processes": [],
        },
    ]

    def run_control_commands(commands):
        displays = [row["display"] for row in commands]
        command_batches.append(displays)
        if displays[0].startswith("launchctl print"):
            return [{
                "command": displays[0],
                "returncode": 113,
                "stdout": "",
                "stderr": "Could not find service",
            }]
        assert displays[0].startswith("launchctl bootstrap")
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment=deployment,
            deployment_dir=root,
            active_sources=[],
            active_schedules=[],
        )
        return [
            {
                "command": display,
                "returncode": 0,
                "stdout": "",
                "stderr": "",
            }
            for display in displays
        ]

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._run_control_commands",
        run_control_commands,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: audits.pop(0),
    )

    def runner(argv, env):
        if argv[4:7] == ["dogfood", "service", "install"]:
            return _service_install_success(
                argv,
                env,
                deployment=deployment,
                dsn=test_db_dsn,
                working_directory=root,
            )
        assert argv[4:6] == ["dogfood", "restart"]
        output = io.StringIO()
        with redirect_stdout(output):
            returncode = main(argv[4:])
        return ApplyCommandResult(returncode, output.getvalue())

    applied = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=plan["plan_id"],
        command_runner=runner,
        worker_ack_timeout_seconds=1,
    )

    worker_receipt = applied["stage_receipts"]["worker_reload"]["postcondition"]
    assert applied["no_op"] is True
    assert worker_receipt["control"]["restart_start_mode"] == (
        "bootstrap_from_absent"
    )
    assert worker_receipt["ack"]["service_authority"] == (
        plan["desired_state"]["service_authority"]
    )
    assert command_batches[0][0].startswith("launchctl print")
    assert command_batches[1][0].startswith("launchctl bootstrap")
    assert command_batches[1][1].startswith("launchctl enable")
    assert not any(
        display.startswith("launchctl bootout")
        for batch in command_batches
        for display in batch
    )


def test_plan_runtime_authority_binds_working_directory_and_database(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.identity test.runtime-authority.identity",
    )
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-runtime-authority"
    root = _deployment(tmp_path, deployment)
    sync_root = tmp_path / "canonical-sync-root"
    sync_root.mkdir()
    manifest = root / "deployment.yaml"
    manifest.write_text(
        manifest.read_text() + f"sync_root: {sync_root}\n",
        encoding="utf-8",
    )
    _reset_apply_state(test_db_dsn, deployment)

    plan = plan_deployment_apply(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
    )
    authority = plan["desired_state"]["service_authority"]
    assert authority["authority_source"] == (
        "deployment_manifest_and_selected_dsn"
    )
    assert authority["working_directory"] == str(sync_root.resolve())
    assert authority["adapter"] in {"launchd", "systemd"}
    assert deployment in authority["service_id"]
    assert authority["command"] == "uv run okg runtime worker"
    assert authority["process_executable"] == str(
        sync_root.resolve() / ".venv" / "bin" / "python3"
    )
    assert authority["interpreter"] == authority["process_executable"]
    assert authority["import_root"] == str(sync_root.resolve() / "src")
    assert set(authority["release"]) == {
        "deployment_root_hash",
        "manifest_fingerprint",
        "catalog_fingerprint",
        "schema_fingerprint",
        "writer_fingerprint",
        "source_registry_fingerprint",
        "schedule_fingerprint",
    }
    assert set(authority["database"]) == {"host", "port", "dbname"}
    assert authority["database"]["dbname"] == (
        psycopg.conninfo.conninfo_to_dict(test_db_dsn)["dbname"]
    )
    assert "user" not in authority["database"]
    assert "password" not in json.dumps(authority).lower()


@pytest.mark.parametrize(
    "mismatch_field",
    (
        "working_directory",
        "process_executable",
        "interpreter",
        "import_root",
        "code_revision",
        "database",
    ),
)
def test_runtime_authority_rejects_observed_process_identity_mismatch(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    mismatch_field: str,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    deployment = f"deployment-apply-observed-{mismatch_field.replace('_', '-')}"
    root = _deployment(tmp_path, deployment)
    manifest = root / "deployment.yaml"
    manifest.write_text(
        manifest.read_text().replace(
            "runtime:\n  enabled: false\n",
            "runtime:\n  enabled: true\n",
        ),
        encoding="utf-8",
    )
    _reset_apply_state(okg_workspace_isolated_runtime, deployment)
    desired = deployment_apply_module.desired_deployment_state(
        deployment=deployment,
        deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )
    observed = {
        "authority_source": "running_worker_process",
        "deployment": deployment,
        **{
            field: desired["service_authority"][field]
            for field in (
                "working_directory",
                "process_executable",
                "interpreter",
                "import_root",
                "code_revision",
                "database",
            )
        },
    }
    observed[mismatch_field] = (
        {"host": "wrong", "port": "0", "dbname": "wrong"}
        if mismatch_field == "database"
        else f"wrong-{mismatch_field}"
    )
    monkeypatch.setattr(
        deployment_apply_module,
        "_observed_runtime_authority",
        lambda **_kwargs: observed,
    )

    record_worker_acknowledgment(
        dsn=okg_workspace_isolated_runtime,
        deployment=deployment,
        deployment_dir=root,
        active_sources=[],
        active_schedules=[],
    )
    plan = plan_deployment_apply(
        deployment=deployment,
        deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )

    worker_action = next(
        row for row in plan["actions"] if row["stage"] == "worker_reload"
    )
    assert worker_action["needed"] is True
    # Every fingerprint matches; only the process identity differs (#2502).
    assert worker_action["reason"] == "worker_ack_identity_mismatch"
    assert worker_action["mismatches"] == [
        f"identity {mismatch_field} differs: "
        f"desired={desired['service_authority'][mismatch_field]!r} "
        f"observed={observed[mismatch_field]!r}"
    ]
    assert plan["live"]["worker_ack"]["observed_runtime_authority"] == observed


@pytest.mark.parametrize(
    "fingerprint_field",
    (
        "desired_state_digest",
        "manifest_fingerprint",
        "catalog_fingerprint",
        "schema_fingerprint",
        "writer_fingerprint",
        "schedule_fingerprint",
    ),
)
def test_worker_ack_rejects_stale_release_fingerprint(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    fingerprint_field: str,
) -> None:
    deployment = f"deployment-apply-stale-{fingerprint_field.replace('_', '-')}"
    root = _deployment(tmp_path, deployment)
    # Worker reload runs only with the runtime on: with it off,
    # `okg runtime worker` refuses to start (issue #3628).
    _enable_runtime(root)
    _reset_apply_state(okg_workspace_isolated_runtime, deployment)
    record_worker_acknowledgment(
        dsn=okg_workspace_isolated_runtime,
        deployment=deployment,
        deployment_dir=root,
        active_sources=[],
        active_schedules=[],
    )
    with psycopg.connect(okg_workspace_isolated_runtime) as conn:
        conn.execute(
            psycopg.sql.SQL(
                "UPDATE okg.deployment_worker_ack "
                "SET {} = 'sha256:stale' "
                "WHERE deployment_name = %s"
            ).format(psycopg.sql.Identifier(fingerprint_field)),
            (deployment,),
        )
        conn.commit()

    plan = plan_deployment_apply(
        deployment=deployment,
        deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )
    worker_action = next(
        row for row in plan["actions"] if row["stage"] == "worker_reload"
    )
    assert worker_action["needed"] is True
    assert worker_action["reason"] == "worker_ack_fingerprint_drift"
    assert f"fingerprint {fingerprint_field} differs" in worker_action["mismatches"]


@pytest.mark.parametrize(
    ("runtime_enabled", "expected_ready"),
    [(False, True), (True, False)],
)
def test_runtime_authority_absent_worker_ack_only_warns_when_disabled(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    runtime_enabled: bool,
    expected_ready: bool,
    record_property,
) -> None:
    """A required worker without an ack is unknown; a disabled one is benign.

    Runtime-enabled deployments fail closed because there is no exact process
    authority receipt. Runtime-disabled deployments cannot produce an ack and
    therefore retain the migration-safe warning behavior.
    """
    test_db_dsn = okg_workspace_isolated_runtime
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    deployment = (
        "deployment-apply-ack-absent-enabled"
        if runtime_enabled
        else "deployment-apply-ack-absent-disabled"
    )
    root = _deployment(tmp_path, deployment)
    if runtime_enabled:
        manifest = root / "deployment.yaml"
        manifest.write_text(
            manifest.read_text().replace(
                "runtime:\n  enabled: false\n",
                "runtime:\n  enabled: true\n",
            ),
            encoding="utf-8",
        )
    _reset_apply_state(test_db_dsn, deployment)

    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )

    def runner(argv, env):
        if argv[4:7] == ["dogfood", "service", "install"]:
            return _service_install_success(
                argv, env, deployment=deployment, dsn=test_db_dsn,
                working_directory=root,
            )
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment=deployment,
            deployment_dir=root,
            active_sources=[],
            active_schedules=[],
        )
        return ApplyCommandResult(0, json.dumps({"ok": True}))

    apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=plan["plan_id"],
        command_runner=runner,
        worker_ack_timeout_seconds=1,
    )
    assert deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )["ok"] is True

    # Drop ONLY the ack, leaving catalog/ownership/nomos converged. This is
    # the exact state of every already-provisioned deployment at merge time.
    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.deployment_worker_ack WHERE deployment_name = %s",
            (deployment,),
        )
        conn.commit()

    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is expected_ready
    if runtime_enabled:
        assert health["status"] == "blocked"
        assert "stage_needed:worker_reload" in health["problems"]
        assert health["warnings"] == []
    else:
        # Issue #3628 (u3runtimeoff A): with the runtime off no worker can
        # run, so there is nothing to wait for: ready, no warnings.
        assert health["status"] == "ready"
        assert health["warnings"] == []
        assert health["problems"] == []

    replan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    reasons = {row["stage"]: row["reason"] for row in replan["actions"]}
    if runtime_enabled:
        # The stage is still *needed*, so `deployment apply` keeps converging it.
        assert reasons["worker_reload"] == "worker_ack_absent"
    else:
        assert reasons["worker_reload"] == "not_needed_runtime_disabled"


def test_runtime_only_drift_does_not_enqueue_sources(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-runtime-only"
    root = _deployment(tmp_path, deployment)
    # Worker reload runs only with the runtime on: with it off,
    # `okg runtime worker` refuses to start (issue #3628).
    _enable_runtime(root)
    _reset_apply_state(test_db_dsn, deployment)
    first = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )

    def runner(argv, env):
        if argv[4:7] == ["dogfood", "service", "install"]:
            return _service_install_success(
                argv, env, deployment=deployment, dsn=test_db_dsn,
                working_directory=root,
            )
        assert argv[4:6] == ["dogfood", "restart"]
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment=deployment,
            deployment_dir=root,
            active_sources=[],
            active_schedules=[],
        )
        return ApplyCommandResult(0, json.dumps({"ok": True}))

    apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=first["plan_id"],
        command_runner=runner,
        worker_ack_timeout_seconds=1,
    )
    manifest = root / "deployment.yaml"
    manifest.write_text(
        manifest.read_text().replace(
            "runtime:\n  enabled: true\n",
            "runtime:\n  enabled: true\n  dbos_pool_size: 7\n",
        ),
        encoding="utf-8",
    )
    runtime_only = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    needed = {row["stage"]: row["needed"] for row in runtime_only["actions"]}
    assert needed["catalog_ownership"] is False
    assert needed["catalog_apply"] is False
    assert needed["source_supervision"] is False
    assert needed["worker_reload"] is True


def test_apply_refuses_stale_confirmation_before_writes(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-stale"
    root = _deployment(tmp_path, deployment)
    _reset_apply_state(test_db_dsn, deployment)

    with pytest.raises(SubstrateError, match="confirmation does not match"):
        apply_deployment_plan(
            deployment=deployment,
            deployment_dir=root,
            dsn=test_db_dsn,
            confirm="sha256:stale",
        )
    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.deployment_apply_runs')")
        table = cur.fetchone()[0]
        if table is not None:
            cur.execute(
                "SELECT count(*) FROM okg.deployment_apply_runs WHERE deployment_name=%s",
                (deployment,),
            )
            assert cur.fetchone()[0] == 0


def test_apply_replans_under_lock_and_refuses_concurrent_winner(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-lock-race"
    root = _deployment(tmp_path, deployment)
    _reset_apply_state(test_db_dsn, deployment)
    confirmed = plan_deployment_apply(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
    )
    original_plan = deployment_apply_module.plan_deployment_apply
    calls = 0

    def runner(argv, env):
        if argv[4:7] == ["dogfood", "service", "install"]:
            return _service_install_success(
                argv, env, deployment=deployment, dsn=test_db_dsn,
                working_directory=root,
            )
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment=deployment,
            deployment_dir=root,
            active_sources=[],
            active_schedules=[],
        )
        return ApplyCommandResult(0, json.dumps({"ok": True}))

    def plan_with_concurrent_winner(**kwargs):
        nonlocal calls
        calls += 1
        stale_plan = original_plan(**kwargs)
        if calls == 1:
            # A competing caller wins and fully converges after this caller's
            # pre-lock plan, but before this caller acquires the advisory lock.
            monkeypatch.setattr(
                deployment_apply_module,
                "plan_deployment_apply",
                original_plan,
            )
            try:
                winner = deployment_apply_module.apply_deployment_plan(
                    deployment=deployment,
                    deployment_dir=root,
                    dsn=test_db_dsn,
                    confirm=stale_plan["plan_id"],
                    command_runner=runner,
                    worker_ack_timeout_seconds=1,
                )
                assert winner["no_op"] is True
            finally:
                monkeypatch.setattr(
                    deployment_apply_module,
                    "plan_deployment_apply",
                    plan_with_concurrent_winner,
                )
        return stale_plan

    monkeypatch.setattr(
        deployment_apply_module,
        "plan_deployment_apply",
        plan_with_concurrent_winner,
    )
    with pytest.raises(SubstrateError, match="became stale before"):
        deployment_apply_module.apply_deployment_plan(
            deployment=deployment,
            deployment_dir=root,
            dsn=test_db_dsn,
            confirm=confirmed["plan_id"],
            command_runner=runner,
            worker_ack_timeout_seconds=1,
        )

    assert calls == 2
    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.deployment_apply_runs "
            "WHERE deployment_name = %s",
            (deployment,),
        )
        # The winner owns the only receipt. The stale caller performed no
        # schema, receipt, or stage write after it acquired the lock.
        assert cur.fetchone()[0] == 1


def test_child_commands_inherit_operator_environment(monkeypatch) -> None:
    captured = {}

    class Result:
        returncode = 0
        stdout = "{}"
        stderr = ""

    def fake_run(argv, **kwargs):
        captured.update(kwargs["env"])
        assert argv == ["uv", "run", "okg", "--help"]
        return Result()

    monkeypatch.setenv("OKG_DEPLOYMENT_APPLY_PROOF_ENV", "preserved")
    monkeypatch.setattr("subprocess.run", fake_run)
    result = _default_runner(["uv", "run", "okg", "--help"], {"EXTRA": "1"})
    assert result.returncode == 0
    assert captured["OKG_DEPLOYMENT_APPLY_PROOF_ENV"] == "preserved"
    assert captured["EXTRA"] == "1"


def test_source_enqueue_refuses_zero_exit_error_payload() -> None:
    with pytest.raises(SubstrateError, match="invalid success"):
        _validate_source_enqueue(
            dsn="not consulted",
            deployment="okg-ci",
            source="docs",
            run_key="deployment-apply:docs",
            payload={"ok": False},
        )


# Aside name used to hide dbos.workflow_status while proving the
# no-DBOS-authority branch. Fixed (not per-run-unique) so the self-heal
# preamble can always find a leftover from a crashed prior run.
_DBOS_ASIDE_NAME = "workflow_status_hidden_by_proof_test"


def _regclass(cur, name: str):
    cur.execute("SELECT to_regclass(%s)", (name,))
    return cur.fetchone()[0]


@pytest.mark.serial_db
def test_proof_refuses_legacy_jobs_without_dbos_authority(
    clean_runtime: str,
) -> None:
    """_queue_counts must refuse to count legacy okg.jobs when
    dbos.workflow_status is absent.

    Absence is ESTABLISHED, never assumed: on any test DB where DBOS ever
    initialized, the table exists forever (clean_runtime only deletes its
    rows), so asserting ambient absence fails permanently there. The
    table is renamed aside for the duration and restored in finally.
    Rename, not DROP+recreate — the DBOS library owns that table's DDL
    and a guessed recreation could poison later tests. The rename runs on
    an autocommit connection so the committed DDL is visible to
    _queue_counts' own connection instead of blocking its to_regclass
    probe on our AccessExclusiveLock; serial_db because the rename is
    database-global.
    """
    hidden = False
    with psycopg.connect(
        clean_runtime, autocommit=True
    ) as conn, conn.cursor() as cur:
        # Self-heal state orphaned by a hard-killed prior run: a leftover
        # okg.jobs (its finally never ran), and the aside — restore it
        # when the real table is gone, drop it (stale rows) when a fresh
        # real table was minted alongside it.
        cur.execute("DROP TABLE IF EXISTS okg.jobs")
        if _regclass(cur, f"dbos.{_DBOS_ASIDE_NAME}") is not None:
            if _regclass(cur, "dbos.workflow_status") is None:
                cur.execute(
                    f"ALTER TABLE dbos.{_DBOS_ASIDE_NAME} "
                    "RENAME TO workflow_status"
                )
            else:
                cur.execute(f"DROP TABLE dbos.{_DBOS_ASIDE_NAME}")
        if _regclass(cur, "dbos.workflow_status") is not None:
            cur.execute(
                "ALTER TABLE dbos.workflow_status "
                f"RENAME TO {_DBOS_ASIDE_NAME}"
            )
            hidden = True
    # Everything after the rename runs under the try: a failure anywhere
    # past this point (even okg.jobs setup) must still restore the table.
    try:
        with psycopg.connect(
            clean_runtime, autocommit=True
        ) as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE okg.jobs (status TEXT NOT NULL)")
            cur.execute("INSERT INTO okg.jobs (status) VALUES ('queued')")
        report = _queue_counts(clean_runtime, "okg-ci")
        assert report["source"] == "dbos.workflow_status"
        assert report["available"] is False
        assert report["active"] is None
        assert report["dlq"] is None
    finally:
        # Fresh autocommit connection: the body's connection state is
        # unknown after a failure, and the restore must always land.
        with psycopg.connect(
            clean_runtime, autocommit=True
        ) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS okg.jobs")
            if hidden:
                cur.execute(
                    f"ALTER TABLE dbos.{_DBOS_ASIDE_NAME} "
                    "RENAME TO workflow_status"
                )


def test_cli_refuses_apply_dsn_override_before_worker_control(capsys) -> None:
    rc = main([
        "deployment", "apply", "okg-ci",
        "--apply", "--confirm", "sha256:test",
        "--dsn", "postgres://postgres:secret@127.0.0.1:5433/isolated",
        "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.err or captured.out)
    assert rc == 1
    assert "refuses --dsn" in payload["error"]["message"]
    assert "secret" not in json.dumps(payload)


def test_deployment_apply_json_error_preserves_datetime_timeout_and_redacts(
    tmp_path: Path,
    monkeypatch,
    capsys,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.errors test.runtime-authority.errors",
    )
    deployment_dir = tmp_path / "okg-workspace"
    deployment_dir.mkdir()
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            name="okg-workspace",
            deployment_dir=deployment_dir,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment._resolve_deployment_dsn_from_dir",
        lambda _args, _deployment_dir: (
            "postgres://operator:top-secret@db/okg_workspace"
        ),
    )

    def fail_apply(**_kwargs):
        error = SubstrateError(
            "replacement worker acknowledgment timed out",
            data={
                "last_acknowledged_at": datetime(
                    2026, 7, 27, 3, 0, tzinfo=timezone.utc
                ),
                "deployment_dir": deployment_dir,
                "dsn": "postgres://operator:top-secret@db/okg_workspace",
            },
            fix_hint="restart the canonical worker and retry deployment apply",
        )
        error.code = "worker_ack_timeout"
        raise error

    monkeypatch.setattr(
        "okg.substrate.deployment_apply.apply_deployment_plan",
        fail_apply,
    )

    rc = main([
        "deployment",
        "apply",
        "okg-workspace",
        "--apply",
        "--confirm",
        "sha256:test",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == "worker_ack_timeout"
    assert payload["error"]["message"] == (
        "replacement worker acknowledgment timed out"
    )
    assert payload["error"]["fix_hint"].startswith("restart the canonical")
    assert payload["error"]["data"]["last_acknowledged_at"] == (
        "2026-07-27T03:00:00+00:00"
    )
    encoded = json.dumps(payload)
    assert "top-secret" not in encoded
    assert "operator:***@db" in encoded


def test_apply_refuses_owner_mismatch_without_force(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-owner-mismatch"
    root = _deployment(tmp_path, deployment)
    _reset_apply_state(test_db_dsn, deployment)
    initial = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    desired = initial["desired_state"]
    from okg.substrate.catalog.ownership import ensure_catalog_ownership_schema

    with psycopg.connect(test_db_dsn) as conn:
        ensure_catalog_ownership_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.deployment_write_ownership (
                  deployment_name, owner_fingerprint, schema_fingerprint,
                  catalog_fingerprint, status
                ) VALUES (%s,'sha256:foreign',%s,%s,'active')
                """,
                (deployment, desired["schema_fingerprint"], desired["catalog_fingerprint"]),
            )
        conn.commit()
    with pytest.raises(SubstrateError, match="another writer") as exc:
        plan_deployment_apply(
            deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
        )
    assert "ownership diagnose" in str(exc.value.fix_hint)
    assert "--force" not in str(exc.value.fix_hint)


def test_apply_plan_treats_this_writers_v1_owner_key_as_refresh(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    """#2229: a row keyed by this writer's pre-#2229 (schema-derived) key is
    the same writer; the plan refreshes ownership instead of refusing."""
    from okg.substrate.catalog.ownership import (
        ensure_catalog_ownership_schema,
        legacy_writer_fingerprint,
    )

    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-v1-owner-key"
    root = _deployment(tmp_path, deployment)
    _reset_apply_state(test_db_dsn, deployment)
    desired = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )["desired_state"]
    old_schema = "sha256:" + "5" * 64
    legacy = legacy_writer_fingerprint(
        deployment_name=deployment,
        schema_fingerprint=old_schema,
        writer_scope="deployment_root",
        environment_id=None,
        deployment_root_hash=desired["deployment_root_hash"],
        worktree_root_hash=desired["worktree_root_hash"],
    )
    assert legacy != desired["writer_fingerprint"]
    with psycopg.connect(test_db_dsn) as conn:
        ensure_catalog_ownership_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.deployment_write_ownership (
                  deployment_name, owner_fingerprint, schema_fingerprint,
                  catalog_fingerprint, status
                ) VALUES (%s,%s,%s,%s,'active')
                """,
                (deployment, legacy, old_schema, desired["catalog_fingerprint"]),
            )
        conn.commit()

    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    ownership_stage = next(
        action for action in plan["actions"]
        if action["stage"] == "catalog_ownership"
    )
    assert ownership_stage == {
        "stage": "catalog_ownership",
        "needed": True,
        "reason": "refresh_needed",
    }
    assert plan["live"]["ownership"]["reason"] == "same_writer_legacy_key"


def test_failed_service_install_is_durable_and_non_ready(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-failed-reload"
    root = _deployment(tmp_path, deployment)
    # Worker reload runs only with the runtime on: with it off,
    # `okg runtime worker` refuses to start (issue #3628).
    _enable_runtime(root)
    _reset_apply_state(test_db_dsn, deployment)
    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )

    def runner(argv, _env):
        assert argv[4:7] == ["dogfood", "service", "install"]
        return ApplyCommandResult(7, "", "supervisor unavailable")

    with pytest.raises(SubstrateError, match="definition install failed"):
        apply_deployment_plan(
            deployment=deployment,
            deployment_dir=root,
            dsn=test_db_dsn,
            confirm=plan["plan_id"],
            command_runner=runner,
        )
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is False
    assert "apply_failed" in health["problems"]
    assert health["failed_stage"] == "worker_reload"
    assert "deployment apply" in health["next_command"]


def test_failed_worker_reload_clears_old_ack_and_remains_non_ready(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.reconcile test.runtime-authority.reconcile",
    )
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-failed-reload"
    root = _deployment(tmp_path, deployment)
    # Worker reload runs only with the runtime on: with it off,
    # `okg runtime worker` refuses to start (issue #3628).
    _enable_runtime(root)
    _reset_apply_state(test_db_dsn, deployment)
    record_worker_acknowledgment(
        dsn=test_db_dsn,
        deployment=deployment,
        deployment_dir=root,
        active_sources=[],
        active_schedules=[],
    )
    manifest = root / "deployment.yaml"
    manifest.write_text(
        manifest.read_text().replace(
            "runtime:\n  enabled: true\n",
            "runtime:\n  enabled: true\n  dbos_pool_size: 7\n",
        ),
        encoding="utf-8",
    )
    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert next(
        row for row in plan["actions"] if row["stage"] == "worker_reload"
    )["needed"] is True

    def runner(argv, env):
        if argv[4:7] == ["dogfood", "service", "install"]:
            with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM okg.deployment_worker_ack "
                    "WHERE deployment_name = %s",
                    (deployment,),
                )
                assert cur.fetchone()[0] == 0
            return _service_install_success(
                argv, env, deployment=deployment, dsn=test_db_dsn,
                working_directory=root,
            )
        assert argv[4:6] == ["dogfood", "restart"]
        return ApplyCommandResult(7, "", "supervisor unavailable")

    with pytest.raises(SubstrateError, match="worker reload failed"):
        apply_deployment_plan(
            deployment=deployment,
            deployment_dir=root,
            dsn=test_db_dsn,
            confirm=plan["plan_id"],
            command_runner=runner,
        )
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is False
    assert "apply_failed" in health["problems"]
    assert health["failed_stage"] == "worker_reload"
    assert health["worker_ack"] is None
    assert "deployment apply" in health["next_command"]


def _enable_runtime(root: Path) -> None:
    manifest = root / "deployment.yaml"
    manifest.write_text(
        manifest.read_text().replace(
            "runtime:\n  enabled: false\n", "runtime:\n  enabled: true\n",
        ),
        encoding="utf-8",
    )


def test_runtime_off_does_not_enqueue_a_changed_source(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    """Issue #3628: with the runtime off there is no job queue (its tables
    do not exist), and `okg ingest` runs sources inline. Apply used to enqueue
    each changed source anyway and fail after the catalog stage succeeded."""
    deployment = "deployment-apply-runtime-off-source"
    root = _deployment(tmp_path, deployment)
    (root / "source_registry.yaml").write_text(
        "sources:\n  docs:\n"
        "    ownership_id: deployment-apply-runtime-off-source.docs\n",
        encoding="utf-8",
    )
    _reset_apply_state(okg_workspace_isolated_runtime, deployment)

    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )

    (row,) = [a for a in plan["actions"] if a["stage"] == "source_supervision"]
    assert row == {
        "stage": "source_supervision",
        "needed": False,
        "sources": [],
        "removed_sources": [],
        "reason": "not_needed_runtime_disabled",
    }
    # Not vacuous: the same source is enqueued once the runtime is on.
    _enable_runtime(root)
    enabled = plan_deployment_apply(
        deployment=deployment, deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )
    (row,) = [a for a in enabled["actions"] if a["stage"] == "source_supervision"]
    assert (row["needed"], row["sources"]) == (True, ["docs"])


def test_runtime_off_apply_runs_no_worker_or_queue_command(
    tmp_path: Path, okg_workspace_isolated_runtime: str,
) -> None:
    """Issue #3628: with the runtime off, `--apply` runs neither
    `okg jobs enqueue-source` nor a worker service install or restart, and
    still converges. The runner fails on any call."""
    deployment = "deployment-apply-runtime-off-no-commands"
    root = _deployment(tmp_path, deployment)
    (root / "source_registry.yaml").write_text(
        "sources:\n  docs:\n"
        "    ownership_id: deployment-apply-runtime-off-no-commands.docs\n",
        encoding="utf-8",
    )
    _reset_apply_state(okg_workspace_isolated_runtime, deployment)
    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )
    assert plan["no_op"] is False  # the catalog stages still need applying

    def no_command(argv, _env):
        raise AssertionError(f"runtime-off apply ran a command: {argv}")

    applied = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
        confirm=plan["plan_id"],
        command_runner=no_command,
    )

    assert applied["applied"] is True
    assert "source_supervision" not in applied["completed_stages"]
    assert "worker_reload" not in applied["completed_stages"]
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
    )
    assert (health["ok"], health["status"], health["warnings"]) == (True, "ready", [])


def test_resume_rechecks_completed_stage_postconditions(
    tmp_path: Path, okg_workspace_isolated_runtime: str, monkeypatch,
) -> None:
    test_db_dsn = okg_workspace_isolated_runtime
    deployment = "deployment-apply-resume-drift"
    root = _deployment(tmp_path, deployment)
    _enable_runtime(root)  # sources are enqueued only into a running runtime
    (root / "source_registry.yaml").write_text(
        "sources:\n  docs:\n"
        "    ownership_id: deployment-apply-resume-drift.docs\n",
        encoding="utf-8",
    )
    _reset_apply_state(test_db_dsn, deployment)
    monkeypatch.setattr(
        "okg.substrate.deployment_apply._validate_source_enqueue",
        lambda **kwargs: {"payload": kwargs["payload"], "observed_workflow": {}},
    )
    first_plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )

    def fail_source(argv, _env):
        if argv[4:6] == ["jobs", "enqueue-source"]:
            return ApplyCommandResult(9, "", "injected source failure")
        raise AssertionError(argv)

    with pytest.raises(SubstrateError, match="source supervision enqueue failed"):
        apply_deployment_plan(
            deployment=deployment,
            deployment_dir=root,
            dsn=test_db_dsn,
            confirm=first_plan["plan_id"],
            command_runner=fail_source,
        )

    # Simulate live catalog drift after the stage receipt was written. A safe
    # resume must trust the fresh plan and rerun this idempotent stage instead
    # of treating the historical receipt as a permanent skip authorization.
    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.catalog_versions
               SET catalog_fingerprint = 'sha256:injected-drift'
             WHERE catalog_version_id = (
               SELECT catalog_version_id FROM okg.catalog_versions
                WHERE deployment_name = %s AND status = 'applied'
                ORDER BY catalog_version_id DESC LIMIT 1
             )
            """,
            (deployment,),
        )
        conn.commit()

    resumed_plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert next(
        row for row in resumed_plan["actions"] if row["stage"] == "catalog_apply"
    )["needed"] is True

    calls: list[list[str]] = []

    def resume_runner(argv, env):
        calls.append(argv)
        if argv[4:6] == ["jobs", "enqueue-source"]:
            assert argv[argv.index("--dsn") + 1] == test_db_dsn
            return ApplyCommandResult(0, json.dumps({"ok": True}))
        if argv[4:7] == ["dogfood", "service", "install"]:
            return _service_install_success(
                argv, env, deployment=deployment, dsn=test_db_dsn,
                working_directory=root,
            )
        if argv[4:6] == ["dogfood", "restart"]:
            record_worker_acknowledgment(
                dsn=test_db_dsn,
                deployment=deployment,
                deployment_dir=root,
                active_sources=["docs"],
                active_schedules=[],
            )
            return ApplyCommandResult(0, json.dumps({"ok": True}))
        raise AssertionError(argv)

    resumed = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=resumed_plan["plan_id"],
        command_runner=resume_runner,
        worker_ack_timeout_seconds=1,
    )
    assert resumed["no_op"] is True
    assert resumed["completed_stages"].count("catalog_apply") == 1
    assert [argv[4:6] for argv in calls] == [
        ["jobs", "enqueue-source"],
        ["dogfood", "service"],
        ["dogfood", "restart"],
    ]

def test_sealed_plan_and_apply_refuse_before_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    root = _deployment(tmp_path, "sealed-refusal")
    authority = {
        "environment_id": "sealed-refusal-production",
        "service_id": "org.okg.runtime-worker.sealed-refusal-production",
        "adapter": "launchd",
    }
    monkeypatch.setattr(
        deployment_apply_module,
        "_environment_authority_for_apply",
        lambda **_kwargs: authority,
    )
    monkeypatch.setattr(
        deployment_apply_module.psycopg,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("database must not be touched"),
    )
    calls: list[list[str]] = []
    kwargs = {
        "deployment": "sealed-refusal",
        "deployment_dir": root,
        "dsn": "postgresql://localhost:5432/sealed_refusal",
    }

    with pytest.raises(SubstrateError) as plan_error:
        plan_deployment_apply(**kwargs)
    with pytest.raises(SubstrateError):
        apply_deployment_plan(
            **kwargs,
            confirm="sha256:stale",
            command_runner=lambda argv, _env: (
                calls.append(argv) or ApplyCommandResult(0)
            ),
        )

    assert calls == []
    assert plan_error.value.data["executable"] is False
    assert plan_error.value.data["authority"]["service_id"] == authority["service_id"]
    assert plan_error.value.data["remedy"] == (
        "uv run okg environment service restart sealed-refusal-production "
        "--adapter launchd --apply --json"
    )


def test_partial_sealed_markers_fail_closed_before_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    root = _deployment(tmp_path, "partial-sealed")
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "partial-sealed-production")
    monkeypatch.setattr(
        deployment_apply_module.psycopg,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("database must not be touched"),
    )

    with pytest.raises(SubstrateError, match="incomplete sealed environment") as error:
        plan_deployment_apply(
            deployment="partial-sealed",
            deployment_dir=root,
            dsn="postgresql://localhost:5432/partial_sealed",
        )

    assert error.value.data["executable"] is False
    assert "OKG_ENVIRONMENT_DESCRIPTOR" in error.value.data["missing"]


@pytest.mark.parametrize(
    ("system", "intended", "conflict", "decoy"),
    (
        (
            "Darwin",
            "org.okg.runtime-worker.inventory-check",
            "org.okg.runtime-worker.inventory-check-production",
            "org.okg.runtime-worker.inventory-checker",
        ),
        (
            "Linux",
            "okg-runtime-worker@inventory-check.service",
            "okg-runtime-worker@inventory-check-production.service",
            "okg-runtime-worker@inventory-checker.service",
        ),
    ),
)
def test_plan_refuses_conflicting_supervisor_definition_before_effects(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    system: str,
    intended: str,
    conflict: str,
    decoy: str,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    root = _deployment(tmp_path, "inventory-check")
    monkeypatch.setattr(deployment_apply_module.platform, "system", lambda: system)
    monkeypatch.setattr(
        deployment_apply_module,
        "_environment_authority_for_apply",
        lambda **_kwargs: None,
    )

    with pytest.raises(SubstrateError, match="conflicting worker supervisor") as error:
        plan_deployment_apply(
            deployment="inventory-check",
            deployment_dir=root,
            dsn=okg_workspace_isolated_runtime,
            supervisor_inventory=lambda _adapter: [
                (intended, ("okg", "runtime", "worker", "--deployment", "inventory-check")),
                (conflict, ("okg", "runtime", "worker", "--deployment=inventory-check")),
                (decoy, ("okg", "mcp", "serve", "--deployment", "inventory-check")),
            ],
        )

    assert error.value.data["intended_service_id"] == intended
    assert error.value.data["conflicting_service_ids"] == [conflict]


def test_plan_preserves_intended_supervisor_and_ignores_name_decoy(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    root = _deployment(tmp_path, "inventory-positive")
    monkeypatch.setattr(deployment_apply_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        deployment_apply_module,
        "_environment_authority_for_apply",
        lambda **_kwargs: None,
    )
    plan = plan_deployment_apply(
        deployment="inventory-positive",
        deployment_dir=root,
        dsn=okg_workspace_isolated_runtime,
        supervisor_inventory=lambda _adapter: [
            (
                "okg-runtime-worker@inventory-positive.service",
                ("okg", "runtime", "worker", "--deployment", "inventory-positive"),
            ),
            (
                "okg-runtime-worker@inventory-positive-production.service",
                ("okg", "runtime", "worker", "--deployment", "inventory-positive-production"),
            ),
        ],
    )
    assert plan["desired_state"]["service_authority"]["service_id"] == (
        "okg-runtime-worker@inventory-positive.service"
    )


def test_launchd_inventory_reads_worker_and_non_worker_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import plistlib
    from okg.substrate import deployment_apply as deployment_apply_module

    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    root = tmp_path / "Library" / "LaunchAgents"
    root.mkdir(parents=True)
    definitions = {
        "org.okg.runtime-worker.sample": [
            "python", "-m", "okg.substrate.runtime.worker", "--deployment", "sample",
        ],
        "org.okg.runtime-worker.sample-mcp": [
            "okg", "mcp", "serve", "--deployment", "sample",
        ],
    }
    for label, argv in definitions.items():
        (root / f"{label}.plist").write_bytes(
            plistlib.dumps({"Label": label, "ProgramArguments": argv})
        )

    inventory = deployment_apply_module._default_supervisor_inventory("launchd")
    by_id = dict(inventory)

    assert deployment_apply_module._command_targets_deployment(
        by_id["org.okg.runtime-worker.sample"], "sample"
    )
    assert not deployment_apply_module._command_targets_deployment(
        by_id["org.okg.runtime-worker.sample-mcp"], "sample"
    )


def test_systemd_inventory_expands_instance_and_ignores_non_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    root = tmp_path / ".config" / "systemd" / "user"
    root.mkdir(parents=True)
    (root / "okg-runtime-worker@sample.service").write_text(
        "[Service]\nExecStart=python -m okg.substrate.cli.main runtime worker --deployment %i\n",
        encoding="utf-8",
    )
    (root / "okg-runtime-worker@sample-mcp.service").write_text(
        "[Service]\nExecStart=okg mcp serve --deployment sample\n",
        encoding="utf-8",
    )

    inventory = deployment_apply_module._default_supervisor_inventory("systemd")
    by_id = dict(inventory)

    assert deployment_apply_module._command_targets_deployment(
        by_id["okg-runtime-worker@sample.service"], "sample"
    )
    assert not deployment_apply_module._command_targets_deployment(
        by_id["okg-runtime-worker@sample-mcp.service"], "sample"
    )

@pytest.mark.parametrize(
    "database_block",
    (
        "",
        "database:\n  name: malformed_db\n  host_port: invalid\n",
        "database:\n  name: malformed_db\n",
    ),
)
def test_checked_in_incomplete_sealed_authority_fails_closed_before_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    database_block: str,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    repo = tmp_path / "repo"
    repo.mkdir()
    root = _deployment(repo, "malformed-sealed")
    descriptor = repo / "ops" / "environments" / "malformed-sealed-production"
    descriptor.mkdir(parents=True)
    (descriptor / "environment.yaml").write_text(
        "api_version: okg.environment/v1\n"
        "id: malformed-sealed-production\n"
        "tier: production\n"
        "graph_deployment_name: malformed-sealed\n"
        "release:\n  mode: sealed\n"
        + database_block,
        encoding="utf-8",
    )
    monkeypatch.setattr(
        deployment_apply_module.psycopg,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("database must not be touched"),
    )

    with pytest.raises(
        SubstrateError, match="incomplete sealed environment authority"
    ) as error:
        plan_deployment_apply(
            deployment="malformed-sealed",
            deployment_dir=root,
            dsn="postgresql://localhost:5432/malformed_db",
        )

    assert error.value.data["executable"] is False


def test_plain_install_worker_is_started_by_hand_and_checked_against_this_okg(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deployment folder outside any checkout: apply never reloads the worker.

    Lane W1 (worker-keeps-plain-install-current). The worker's identity is the
    okg installation itself, read for real here (the autouse fake is undone),
    and doctor's fix is to (re)start `okg runtime worker`.
    """
    test_db_dsn = okg_workspace_isolated_runtime
    monkeypatch.setattr(
        deployment_apply_module,
        "_observed_runtime_authority",
        _real_observed_runtime_authority,
    )
    deployment = "deployment-apply-process-worker"
    root = (tmp_path / "plain" / deployment).resolve()
    root.mkdir(parents=True)
    if _enclosing_checkout(root) is not None:
        pytest.skip("the temporary folder is inside a Git checkout")
    (root / "deployment.yaml").write_text(
        f"name: {deployment}\nmodules: []\nsource_registry: source_registry.yaml\n"
        "runtime:\n  enabled: true\n",
        encoding="utf-8",
    )
    (root / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    _reset_apply_state(test_db_dsn, deployment)

    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    authority = plan["desired_state"]["service_authority"]
    assert authority["adapter"] == "process"
    if not authority["installed_code_revision"]:
        pytest.skip(authority["installed_code_revision_reason"])
    reload = next(row for row in plan["actions"] if row["stage"] == "worker_reload")
    assert reload["needed"] and reload["reason"] == "worker_ack_absent"

    def runner(argv, env):
        raise AssertionError(f"apply must not run a service command: {argv}")

    with pytest.raises(SubstrateError, match="does not supervise") as refused:
        apply_deployment_plan(
            deployment=deployment,
            deployment_dir=root,
            dsn=test_db_dsn,
            confirm=plan["plan_id"],
            command_runner=runner,
            worker_ack_timeout_seconds=1,
        )
    assert refused.value.fix_hint.startswith("start the worker:")
    assert f"runtime worker --deployment {deployment}" in refused.value.fix_hint

    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is False
    assert health["problems"] == ["stage_needed:worker_reload"]
    assert health["message"] == "no runtime worker has started for this deployment yet"
    assert health["next_command"].startswith("start the worker:")

    # The worker boots from this same okg and records its acknowledgment.
    record_worker_acknowledgment(
        dsn=test_db_dsn,
        deployment=deployment,
        deployment_dir=root,
        active_sources=[],
        active_schedules=[],
    )
    assert deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )["ok"] is True

    # A worker from another environment is refused by name, with a restart fix.
    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.deployment_worker_ack SET observed_runtime_authority = "
            "jsonb_set(observed_runtime_authority, '{python_prefix}', "
            "'\"/elsewhere/.venv\"') WHERE deployment_name = %s",
            (deployment,),
        )
        conn.commit()
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is False
    reload = next(row for row in health["actions"] if row["stage"] == "worker_reload")
    assert reload["mismatches"][0].startswith("identity python_prefix differs")
    assert "restart it" in health["message"]
    assert "start it again" in health["next_command"]


def test_plain_install_with_runtime_off_applies_without_a_worker(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The workspace profile's default (runtime off) must not need a worker.

    With no worker allowed to run, apply finishes its stages and converges
    instead of asking for a worker `okg runtime worker` would refuse to start;
    with the runtime off the worker steps are never needed (#3628).
    """
    test_db_dsn = okg_workspace_isolated_runtime
    monkeypatch.setattr(
        deployment_apply_module,
        "_observed_runtime_authority",
        _real_observed_runtime_authority,
    )
    deployment = "deployment-apply-process-runtime-off"
    root = (tmp_path / "plain" / deployment).resolve()
    root.mkdir(parents=True)
    if _enclosing_checkout(root) is not None:
        pytest.skip("the temporary folder is inside a Git checkout")
    (root / "deployment.yaml").write_text(
        f"name: {deployment}\nmodules: []\nsource_registry: source_registry.yaml\n"
        "runtime:\n  enabled: false\n",
        encoding="utf-8",
    )
    (root / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    _reset_apply_state(test_db_dsn, deployment)

    plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert plan["desired_state"]["service_authority"]["adapter"] == "process"

    def runner(argv, env):
        raise AssertionError(f"apply must not run a service command: {argv}")

    result = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=root,
        dsn=test_db_dsn,
        confirm=plan["plan_id"],
        command_runner=runner,
        worker_ack_timeout_seconds=1,
    )
    assert result["no_op"] is True
    assert not any(row["needed"] for row in result["actions"])
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=root, dsn=test_db_dsn,
    )
    assert health["ok"] is True
    assert health["status"] == "ready"


def test_a_dry_run_against_dsn_withholds_the_apply_command(
    tmp_path: Path,
    okg_workspace_isolated_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--apply` refuses --dsn and plans against the manifest's database, so
    a dry run read through --dsn must not hand out an apply command."""
    from okg.substrate.cli.main import main

    deployment = "deployment-apply-dsn-dry-run"
    root = _deployment(tmp_path, deployment)
    # The CLI resolves the manifest by name, which needs schema_dir.
    with (root / "deployment.yaml").open("a", encoding="utf-8") as handle:
        handle.write("schema_dir: schemas\n")
    (root / "schemas").mkdir()
    (root / "schemas" / ".keep").write_text("", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "commit", "-qm", "schema_dir"], cwd=root, check=True)
    _reset_apply_state(okg_workspace_isolated_runtime, deployment)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    rc = main([
        "deployment", "apply", deployment,
        "--dsn", okg_workspace_isolated_runtime, "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "dry-run"
    assert payload["pending_stages"]
    assert payload["next_command"] is None
    assert payload["summary"].startswith("dry run against --dsn: nothing was applied;")
    assert "not --dsn" in payload["summary"]
