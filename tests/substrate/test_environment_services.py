from __future__ import annotations

import copy
import json
import os
import plistlib
import shlex
import shutil
import stat
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote

import pytest
import psycopg
import yaml

import okg.substrate.environment_clone as environment_clone
import okg.substrate.environment_services as environment_services
import okg.substrate.environment_readiness as environment_readiness
import okg.substrate.release_activation as release_activation
from okg.substrate.cli.main import _build_parser, main
from okg.substrate.cli.deployment_readiness import (
    _ready_postgres_capacity_repair_commands,
)
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.deployments.environment import (
    load_environment,
    resolve_postgres_capacity_authority,
)
from okg.substrate.db.wal_settings import graph_wal_command_args
from okg.substrate.environment_services import (
    environment_release_handoff_status,
    environment_service_authority_status,
    environment_service_installation_status,
    execute_environment_service_action,
    install_environment_services,
    reconcile_environment_database_credentials,
    render_environment_services,
    write_environment_service_supervision_evidence,
)
from okg.substrate.environment_readiness import environment_readiness_report
from okg.substrate.mcp.surface_fleet import discover_registered_servers
from okg.substrate.mcp.surface_freshness import mcp_surface_report
from okg.substrate.source_sync_producers import make_webhook_http_server
from okg.substrate.runtime import release_authority
from okg.substrate.runtime.environment_exec import load_environment_file


FIXTURES = Path(__file__).parent / "fixtures" / "deployment_environments"
_REAL_PENDING_DBOS_WORKFLOWS = environment_services._pending_dbos_workflows


@pytest.fixture(autouse=True)
def _unit_dbos_has_no_pending_workflows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unit lifecycle tests inject DBOS state; the real lifecycle uses Postgres."""
    monkeypatch.setattr(
        environment_services,
        "_pending_dbos_workflows",
        lambda _environment: (),
    )


def _launch_prefix(environment: Any, name: str) -> str:
    """A per-launch bytecode cache directory as the release launcher records it."""
    return str(
        release_authority.service_pycache_root(environment.runtime.state_root)
        / f"launch-{name}"
    )


def _launched(arguments: Sequence[str], prefix: str) -> list[str]:
    """The live interpreter argv the launcher execs for role ``arguments``."""
    return ["-I", "-B", "-X", f"pycache_prefix={prefix}", *list(arguments)[1:]]


def _environment_fixture(
    tmp_path: Path,
) -> tuple[Path, dict[str, str]]:
    root = tmp_path / "environments"
    shutil.copytree(FIXTURES, root)
    for descriptor in root.glob("*/environment.yaml"):
        raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
        raw["runtime"]["config"]["enabled"] = True
        descriptor.write_text(
            yaml.safe_dump(raw, sort_keys=False),
            encoding="utf-8",
        )
    values = {
        "TEST_STATE_ROOT": str(tmp_path / "state"),
        "TEST_STAGING_DSN": (
            "postgresql://postgres:staging-secret@127.0.0.1:55435/fixture_staging"
        ),
        "TEST_PRODUCTION_DSN": (
            "postgresql://postgres:production-secret@127.0.0.1:55434/fixture_production"
        ),
    }
    return root, values


def _load(
    root: Path,
    environment_id: str,
    values: dict[str, str],
):
    environment = load_environment(
        environment_id,
        environments_dir=root,
        environ=values,
    )
    # Service-install tests run without root. Bind the fixture's explicit
    # container identity to the current process while production descriptors
    # retain the image-qualified postgres uid/gid (1000:1000).
    return replace(
        environment,
        database=replace(
            environment.database,
            container_uid=os.getuid(),
            container_gid=os.getgid(),
        ),
    )


def _stub_host_fence_authority(
    monkeypatch: pytest.MonkeyPatch,
    environment: Any,
    plan: Mapping[str, Any],
) -> None:
    release_status = dict(plan.get("release_status") or {})
    release_status.update(
        {
            "ok": True,
            "status": "active",
            "artifact_id": release_status.get("artifact_id")
            or "sha256:" + "a" * 64,
            "definition_digest": release_status.get("definition_digest")
            or "sha256:" + "b" * 64,
            "active_pointer_digest": release_status.get("active_pointer_digest")
            or "sha256:" + "c" * 64,
        }
    )
    service_ids = tuple(
        (plan.get("handoff_topology") or {}).get("stop_services") or ()
    )
    inventory = {
        "target_artifact_id": release_status["artifact_id"],
        "target_definition_digest": release_status["definition_digest"],
        "target_pointer_digest": release_status["active_pointer_digest"],
        "previous_artifact_id": None,
        "previous_definition_digest": None,
        "previous_pointer_digest": None,
        "plan_digest": plan.get("plan_digest"),
        "worker": {"generation": "current", "state": "stopped", "identity": None},
        "services": {
            service_id: {
                "generation": "current",
                "state": "stopped",
                "identity": None,
            }
            for service_id in service_ids
        },
    }
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda _environment, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_attest_restart_inventory_stopped",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_record_predecessor_stop",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_remove_environment_authority_receipts",
        lambda *_args, **_kwargs: None,
    )


def _stub_receiptless_managers_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep unit installs independent of the host's real service manager."""
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": {
                "adapter": "systemd",
                "load_state": "not-found",
                "result": "success",
                "exec_main_code": 0,
                "exec_main_status": 0,
                "exit_status_observed": True,
            },
        },
    )


def _set_values(
    monkeypatch: pytest.MonkeyPatch,
    values: dict[str, str],
) -> None:
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _artifact(plan: dict, kind: str) -> dict:
    return next(row for row in plan["artifacts"] if row["kind"] == kind)


def test_supervision_systemd_render_owns_complete_environment_topology(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)

    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )

    assert plan["environment_id"] == "fixture-staging"
    assert plan["launchable"] is False
    assert plan["release_status"]["status"] == "missing"
    worker = _artifact(plan, "worker_service")
    ui = _artifact(plan, "ui_service")
    compose = _artifact(plan, "database_compose")
    assert "runtime worker" not in worker["content"]
    assert f"OKG_SERVICE_ID={environment.runtime.worker_service}" in (worker["content"])
    assert (
        str(environment.runtime.state_root / "supervisor/release_launcher.py")
        in (worker["content"])
    )
    assert (
        str(environment.runtime.state_root / "release-launcher.json")
        in (worker["content"])
    )
    assert "-m okg.viz" in ui["content"]
    assert str(environment.ui.port) in ui["content"]
    assert environment.database.container in compose["content"]
    assert environment.database.volume in compose["content"]
    compose_payload = yaml.safe_load(compose["content"])
    assert compose_payload["services"]["postgres"]["ports"] == ["127.0.0.1:55435:5432"]
    assert "staging-secret" not in repr(plan)


def test_systemd_restart_enables_current_units_only_after_obsolete_disable(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    service_artifacts = [
        row
        for row in plan["artifacts"]
        if str(row.get("kind") or "").endswith("service")
    ]
    current_ids = tuple(str(row["service_id"]) for row in service_artifacts)
    commands = environment_services._lifecycle_commands(
        environment,
        adapter="systemd",
        systemd_scope="user",
        compose_path=Path(_artifact(plan, "database_compose")["path"]),
        service_artifacts=service_artifacts,
        handoff_service_ids=(*current_ids, "org.okg.obsolete.issue1305"),
    )["restart"]
    stop_index = next(
        index for index, row in enumerate(commands) if "stop" in row["argv"]
    )
    disable_index = next(
        index
        for index, row in enumerate(commands)
        if "disable" in row["argv"]
        and "org.okg.obsolete.issue1305.service" in row["argv"]
    )
    enable_index = next(
        index
        for index, row in enumerate(commands)
        if "enable" in row["argv"] and "--now" in row["argv"]
    )
    assert stop_index < disable_index < enable_index
    assert all(
        "enable" not in row["argv"]
        for row in commands[: disable_index + 1]
    )
    assert "postgresql://" not in repr(plan)
    topology = {row["kind"]: row for row in plan["topology"]}
    assert topology["worker"]["dbos_identity"] == "fixture-staging:dbos"
    assert topology["embedded_webhook"]["owner_service"] == (
        environment.runtime.worker_service
    )
    assert topology["embedded_webhook"]["endpoint"] == "127.0.0.1:8089"


def test_postgres_compose_renders_declared_resource_limits_and_worker_capacity(
    tmp_path: Path,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )
    root, values = _environment_fixture(tmp_path)
    descriptor = root / "fixture-staging/environment.yaml"
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    raw["database"]["shm_size"] = "2gb"
    raw["database"]["memory_limit"] = "6gb"
    descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    environment = _load(root, "fixture-staging", values)

    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )
    compose = yaml.safe_load(_artifact(plan, "database_compose")["content"])

    postgres = compose["services"]["postgres"]
    # An init runs as PID 1, so an orphaned healthcheck cannot reset Postgres
    # (tests/ci/test_postgres_container_init.py).
    init = postgres.get("init")
    assert init is True
    assert postgres["shm_size"] == "2gb"
    assert postgres["mem_limit"] == "6gb"
    assert postgres["command"] == [
        "postgres",
        "-c",
        "max_worker_processes=32",
        "-c",
        "max_parallel_workers=8",
        "-c",
        "timescaledb.max_background_workers=8",
        *graph_wal_command_args(),
    ]


def test_postgres_worker_capacity_requires_reserved_headroom(
    tmp_path: Path,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )
    root, values = _environment_fixture(tmp_path)
    capacity_path = root / "fixture-production/postgres-capacity.yaml"
    raw = yaml.safe_load(capacity_path.read_text(encoding="utf-8"))
    raw["max_worker_processes"] = 16
    raw["max_parallel_workers"] = 8
    raw["timescaledb_max_background_workers"] = 8
    capacity_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(
        DeploymentConfigError,
        match="worker capacity leaves 0 process\\(es\\) of headroom",
    ):
        _load(root, "fixture-production", values)


def test_postgres_compose_applies_environment_wal_overrides(tmp_path: Path) -> None:
    """database.wal overrides named WAL settings; the rest keep the defaults."""
    root, values = _environment_fixture(tmp_path)
    descriptor = root / "fixture-staging/environment.yaml"
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    raw["database"]["wal"] = {"max_wal_size": "2GB", "checkpoint_timeout": "10min"}
    descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    environment = _load(root, "fixture-staging", values)

    plan = render_environment_services(environment, adapter="systemd", environ=values)
    command = yaml.safe_load(_artifact(plan, "database_compose")["content"])[
        "services"
    ]["postgres"]["command"]

    assert "max_wal_size=2GB" in command
    assert "checkpoint_timeout=10min" in command
    assert "min_wal_size=1GB" in command
    assert "wal_compression=zstd" in command
    assert "max_wal_size=4GB" not in command
    assert environment.to_dict()["database"]["wal_settings"]["max_wal_size"] == "2GB"


def test_environment_refuses_an_unknown_wal_setting(tmp_path: Path) -> None:
    root, values = _environment_fixture(tmp_path)
    descriptor = root / "fixture-staging/environment.yaml"
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    raw["database"]["wal"] = {"max_wal_sise": "2GB"}
    descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(
        DeploymentConfigError,
        match=r"database\.wal: unknown setting\(s\) max_wal_sise",
    ):
        _load(root, "fixture-staging", values)


def test_postgres_capacity_authority_resolves_production_descriptor_without_dsn(
    tmp_path: Path,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )
    root, _values = _environment_fixture(tmp_path)
    descriptor = root / "fixture-production/environment.yaml"

    authority = resolve_postgres_capacity_authority(
        deployment_name="fixture-graph",
        deployment_dir=tmp_path / "deployments/fixture",
        environ={"OKG_ENVIRONMENT_DESCRIPTOR": str(descriptor)},
    )

    assert authority["environment_id"] == "fixture-production"
    assert authority["max_worker_processes"] == 32
    assert authority["timescaledb_max_background_workers"] == 8
    assert authority["timescaledb_job_worker_reserve"] == 4
    assert authority["configured_headroom"] == 16


def test_postgres_capacity_repair_commands_parse_with_concrete_adapter() -> None:
    commands = _ready_postgres_capacity_repair_commands("fixture-production")

    assert len(commands) == 2
    for command in commands:
        argv = shlex.split(command)
        assert argv[:3] == ["uv", "run", "okg"]
        assert "<" not in command
        assert "|" not in command
        assert ">" not in command
        parser = _build_parser(argv[3:])
        parsed = parser.parse_args(argv[3:])
        assert parsed.environment == "fixture-production"
        assert parsed.adapter in {"launchd", "systemd"}
        assert parsed.apply is True


def test_postgres_worker_capacity_survives_rerender_and_restart_plan(
    tmp_path: Path,
    record_property,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-production", values)
    first = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )
    install_environment_services(environment, first)

    reloaded = _load(root, "fixture-production", values)
    second = render_environment_services(
        reloaded,
        adapter="systemd",
        environ=values,
    )
    first_compose = _artifact(first, "database_compose")["content"]
    second_compose = _artifact(second, "database_compose")["content"]
    second_postgres = yaml.safe_load(second_compose)["services"]["postgres"]

    assert second["installation"]["ok"] is True
    assert second_compose == first_compose
    assert second_postgres["command"] == [
        "postgres",
        "-c",
        "max_worker_processes=32",
        "-c",
        "max_parallel_workers=8",
        "-c",
        "timescaledb.max_background_workers=8",
        *graph_wal_command_args(),
    ]
    restart_commands = second["commands"]["restart"]
    assert any(
        "postgres.compose.yaml" in " ".join(command["argv"])
        for command in restart_commands
    )


def test_staging_container_and_worker_do_not_auto_restart(
    tmp_path: Path,
) -> None:
    # PACT bound-host-storage-pressure / req.host.staging-governance: a staging
    # clone must never resume replay from a daemon/container restart alone.
    import plistlib

    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)

    launchd_plan = render_environment_services(
        environment, adapter="launchd", environ=values,
    )
    compose = yaml.safe_load(
        _artifact(launchd_plan, "database_compose")["content"]
    )
    assert compose["services"]["postgres"]["restart"] == "no"

    worker = plistlib.loads(
        _artifact(launchd_plan, "worker_service")["content"].encode("utf-8")
    )
    assert worker["KeepAlive"] is False
    # The read-only UI service is unaffected -- it keeps auto-restarting.
    ui = plistlib.loads(
        _artifact(launchd_plan, "ui_service")["content"].encode("utf-8")
    )
    assert ui["KeepAlive"] is True

    systemd_plan = render_environment_services(
        environment, adapter="systemd", environ=values,
    )
    worker_unit = _artifact(systemd_plan, "worker_service")["content"]
    assert "Restart=no" in worker_unit
    assert "Restart=on-failure" not in worker_unit


def test_production_container_and_worker_still_auto_restart(
    tmp_path: Path,
) -> None:
    import plistlib

    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-production", values)

    launchd_plan = render_environment_services(
        environment, adapter="launchd", environ=values,
    )
    compose = yaml.safe_load(
        _artifact(launchd_plan, "database_compose")["content"]
    )
    assert compose["services"]["postgres"]["restart"] == "unless-stopped"

    worker = plistlib.loads(
        _artifact(launchd_plan, "worker_service")["content"].encode("utf-8")
    )
    assert worker["KeepAlive"] is True

    systemd_plan = render_environment_services(
        environment, adapter="systemd", environ=values,
    )
    assert "Restart=on-failure" in (
        _artifact(systemd_plan, "worker_service")["content"]
    )


def test_supervision_install_keeps_secret_only_in_mode_0600_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )

    installed = install_environment_services(environment, plan)

    assert {Path(row["path"]) for row in plan["artifacts"]}.issubset(
        set(installed.paths)
    )
    assert environment.runtime.state_root / "supervisor/install.json" in (
        installed.paths
    )
    env_file = environment.runtime.local_env_file
    env_text = env_file.read_text(encoding="utf-8")
    assert 'TEST_STAGING_DSN="postgresql://' in env_text
    assert (
        environment_services._env_assignment(
            "TEST_STATE_ROOT", values["TEST_STATE_ROOT"]
        )
        in env_text
    )
    assert "OKG_POSTGRES_PASSWORD" not in env_text
    assert "OKG_MCP_RO_DSN=" in env_text
    assert "OKG_MCP_RW_DSN=" in env_text
    assert "user=okg_mcp" in env_text
    assert "user=app_rw" in env_text
    assert "password=okg_mcp" not in env_text
    assert "password=okg_rw" not in env_text
    role_credentials = (
        environment.runtime.state_root / "supervisor/database-role-credentials.json"
    )
    role_payload = json.loads(role_credentials.read_text(encoding="utf-8"))
    assert stat.S_IMODE(role_credentials.stat().st_mode) == 0o600
    assert set(role_payload["passwords"]) == {"app_ro", "app_rw", "okg_mcp"}
    assert all(len(value) >= 32 for value in role_payload["passwords"].values())
    password_file = environment.runtime.state_root / "supervisor/postgres-password"
    assert password_file.read_text(encoding="utf-8") == "staging-secret\n"
    assert stat.S_IMODE(password_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    launcher = environment.runtime.state_root / "supervisor/release_launcher.py"
    assert stat.S_IMODE(launcher.stat().st_mode) == 0o555
    assert "from okg" not in launcher.read_text(encoding="utf-8")
    for path in installed.paths:
        if path in {env_file, password_file} or path.name == "install.json":
            continue
        assert "staging-secret" not in path.read_text(encoding="utf-8")
    assert environment_service_installation_status(environment, plan)["ok"] is True

    worker_path = Path(_artifact(plan, "worker_service")["path"])
    worker_path.write_text("tampered\n", encoding="utf-8")
    installation = environment_service_installation_status(environment, plan)
    assert installation["ok"] is False
    assert "content changed" in installation["message"]


@pytest.mark.parametrize("owner_index", [4, 5], ids=["uid", "gid"])
def test_supervision_install_sets_and_verifies_postgres_secret_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner_index: int,
) -> None:
    # Host ownership of the secret is only enforced where a host chown
    # actually reaches the container -- i.e. not on macOS, where Docker
    # Desktop's bind-mount translation reports the container's own uid.
    # This test asserts the enforcing half, so it declares the platform
    # instead of inheriting whichever host runs it. Without the pin it
    # passes on Linux CI and fails on a macOS workstation.
    monkeypatch.setattr(environment_services.sys, "platform", "linux")

    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )
    password_artifact = _artifact(plan, "postgres_password_file")
    assert password_artifact["owner"] == {
        "uid": os.getuid(),
        "gid": os.getgid(),
    }

    install_environment_services(environment, plan)
    password_file = Path(password_artifact["path"])
    observed = password_file.stat()
    assert stat.S_IMODE(observed.st_mode) == 0o600
    assert (observed.st_uid, observed.st_gid) == (os.getuid(), os.getgid())
    assert environment_service_installation_status(environment, plan)["ok"] is True

    real_stat = Path.stat

    def drifted_stat(
        path: Path,
        *,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        result = real_stat(path, follow_symlinks=follow_symlinks)
        if path != password_file:
            return result
        values = list(result)
        values[owner_index] = result[owner_index] + 1
        return os.stat_result(values)

    monkeypatch.setattr(Path, "stat", drifted_stat)
    installation = environment_service_installation_status(environment, plan)
    assert installation["ok"] is False
    assert "owner changed" in installation["message"]


def test_supervision_secret_rotation_requires_reinstall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    original = _load(root, "fixture-staging", values)
    plan = render_environment_services(original, adapter="systemd", environ=values)
    install_environment_services(original, plan)

    rotated_values = dict(values)
    rotated_values["TEST_STAGING_DSN"] = (
        "postgresql://postgres:rotated-secret@127.0.0.1:55435/fixture_staging"
    )
    rotated = _load(root, "fixture-staging", rotated_values)
    rotated_plan = render_environment_services(
        rotated,
        adapter="systemd",
        environ=rotated_values,
    )

    installation = environment_service_installation_status(rotated, rotated_plan)
    assert installation["ok"] is False
    assert "content changed" in installation["message"]
    assert rotated_plan["startable"] is False

    install_environment_services(rotated, rotated_plan)
    assert environment_service_installation_status(rotated, rotated_plan)["ok"]


def test_refresh_carries_only_matching_previous_installation_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    previous_artifact_id = "sha256:" + "a" * 64
    previous_plan_digest = "sha256:" + "b" * 64
    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )
    plan["release_status"]["previous_artifact_id"] = previous_artifact_id
    previous_topology = copy.deepcopy(
        plan["handoff_topology"]["generations"]["current"]
    )
    plan["handoff_topology"]["generations"]["previous"] = previous_topology
    plan["plan_digest"] = environment_services._service_plan_digest(plan)
    supervisor = environment.runtime.state_root / "supervisor"
    supervisor.mkdir(parents=True, exist_ok=True)
    marker_path = supervisor / "install.json"
    marker_path.write_text(
        json.dumps(
            {
                "schema": "okg.environment_service_install/v1",
                "environment_id": environment.id,
                "artifact_id": previous_artifact_id,
                "plan_digest": previous_plan_digest,
                "adapter": "systemd",
                "systemd_scope": "system",
                "launcher_python": "/usr/bin/python3",
                "postgres_image": "okg-pg17:local",
                "service_topology": previous_topology,
            }
        ),
        encoding="utf-8",
    )
    marker_path.chmod(0o600)
    assert environment_services._previous_installation_binding(
        environment,
        plan,
    ) == {
        "artifact_id": previous_artifact_id,
        "plan_digest": previous_plan_digest,
        "adapter": "systemd",
        "systemd_scope": "system",
        "launcher_python": "/usr/bin/python3",
        "postgres_image": "okg-pg17:local",
        "service_topology": previous_topology,
    }

    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["adapter"] = "launchd"
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    assert (
        environment_services._previous_installation_binding(
            environment,
            plan,
        )
        is None
    )

    marker["adapter"] = "systemd"
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    monkeypatch.setattr(
        environment_services,
        "_require_environment_service_install_authority",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(plan["release_status"]),
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    install_environment_services(environment, plan)
    installation = environment_service_installation_status(environment, plan)
    assert installation["ok"] is True
    assert installation["previous_installation"] == {
        "artifact_id": previous_artifact_id,
        "plan_digest": previous_plan_digest,
        "adapter": "systemd",
        "systemd_scope": "system",
        "launcher_python": "/usr/bin/python3",
        "postgres_image": "okg-pg17:local",
        "service_topology": previous_topology,
    }

    install_environment_services(environment, plan)
    repeated = environment_service_installation_status(environment, plan)
    assert repeated["ok"] is True
    assert repeated["previous_installation"] == installation[
        "previous_installation"
    ]


def test_refresh_stopped_probe_requires_absent_host_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = {
        "commands": {
            "stop": [
                {
                    "argv": [
                        "launchctl",
                        "bootout",
                        "gui/501/org.okg.worker.fixture",
                    ]
                },
                {
                    "argv": [
                        "docker",
                        "compose",
                        "stop",
                        "postgres",
                    ]
                },
            ]
        }
    }
    monkeypatch.setattr(
        environment_services,
        "_run_command",
        lambda _argv: subprocess.CompletedProcess(
            _argv,
            113,
            stdout="",
            stderr="Could not find service",
        ),
    )
    assert environment_services._environment_host_services_stopped(plan)

    monkeypatch.setattr(
        environment_services,
        "_run_command",
        lambda _argv: subprocess.CompletedProcess(_argv, 0, stdout="", stderr=""),
    )
    assert not environment_services._environment_host_services_stopped(plan)


def test_supervisor_status_reads_systemd_user_scope_from_a_real_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The systemd-user probe keeps direct coverage against a fake systemctl.

    Every other install test stubs `_supervisor_process_status` so it stays
    independent of whichever service manager the host runs. This test is the
    one that still drives the real subprocess, against a `systemctl` this test
    owns, so the argv, the property parse, and the failure envelope are all
    still asserted somewhere.
    """
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = {
        "environment_id": environment.id,
        "adapter": "systemd",
        "systemd_scope": "user",
    }

    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    argv_log = tmp_path / "systemctl-argv.txt"
    fake = bin_dir / "systemctl"
    fake.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$@" > {shlex.quote(str(argv_log))}\n'
        'if [ -n "$FAKE_SYSTEMCTL_FAILS" ]; then\n'
        '  echo "Failed to connect to bus: No medium found" >&2\n'
        "  exit 1\n"
        "fi\n"
        "cat <<'PROPERTIES'\n"
        "LoadState=not-found\n"
        "MainPID=0\n"
        "ActiveState=inactive\n"
        "SubState=dead\n"
        "Job=\n"
        "Result=success\n"
        "ExecMainCode=0\n"
        "ExecMainStatus=0\n"
        "ExecMainPID=0\n"
        "InvocationID=\n"
        "PROPERTIES\n",
        encoding="utf-8",
    )
    fake.chmod(0o700)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    service_id = environment.runtime.worker_service

    observed = environment_services._supervisor_process_status(
        environment,
        service_id=service_id,
        plan=plan,
    )

    assert observed["ok"] is True
    assert observed["status"] == "stopped"
    assert observed["pid"] is None
    assert observed["environment_id"] == environment.id
    assert observed["service_id"] == service_id
    assert observed["manager_outcome"]["adapter"] == "systemd"
    assert observed["manager_outcome"]["load_state"] == "not-found"
    assert observed["manager_outcome"]["result"] == "success"
    assert observed["manager_outcome"]["exit_status_observed"] is True
    recorded = argv_log.read_text(encoding="utf-8").splitlines()
    assert recorded[0] == "--user"
    assert recorded[1] == "show"
    assert "--property=LoadState" in recorded
    assert "--property=MainPID" in recorded
    assert recorded[-1] == f"{service_id}.service"

    monkeypatch.setenv("FAKE_SYSTEMCTL_FAILS", "1")

    refused = environment_services._supervisor_process_status(
        environment,
        service_id=service_id,
        plan=plan,
    )

    assert refused["ok"] is False
    assert refused["status"] == "invalid"
    assert refused["pid"] is None
    assert refused["error"] == "DeploymentConfigError"
    assert "systemd main-pid readback failed" in refused["message"]
    assert "Failed to connect to bus" in refused["message"]
    assert "manager_outcome" not in refused


def test_supervision_reconciles_loopback_database_role_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    altered: list[str] = []

    class Rows:
        def fetchall(self):
            return [("app_ro",), ("app_rw",), ("okg_mcp",)]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params=None):
            if isinstance(query, str):
                assert "pg_roles" in query
                return Rows()
            altered.append(str(query))
            assert params is None
            return Rows()

        def commit(self):
            return None

    monkeypatch.setattr(
        environment_services, "db_connect", lambda *_args, **_kwargs: Connection(),
    )
    # The real role lock is proven against PostgreSQL in
    # tests/substrate/test_cluster_role_ddl_lock.py; here, only that it is
    # taken on every altered role, in the same transaction, before any ALTER.
    monkeypatch.setattr(
        environment_services,
        "lock_roles_for_alter",
        lambda conn, roles: altered.append(f"LOCK {sorted(roles)}"),
    )

    result = reconcile_environment_database_credentials(environment)

    assert result["ok"] is True
    assert result["roles"] == ["app_ro", "app_rw", "okg_mcp"]
    assert len(altered) == 5
    assert altered[0] == "LOCK ['app_ro', 'app_rw', 'okg_mcp']"
    assert all("ALTER ROLE" in statement for statement in altered[1:4])
    assert "GRANT CONNECT ON DATABASE" in altered[-1]


def test_supervision_refuses_to_manage_remote_database_role_credentials(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    values["TEST_STAGING_DSN"] = (
        "postgresql://postgres:secret@db.example.invalid:55435/fixture_staging"
    )
    environment = _load(root, "fixture-staging", values)

    with pytest.raises(DeploymentConfigError, match="non-loopback"):
        reconcile_environment_database_credentials(environment)

    credentials = (
        environment.runtime.state_root / "supervisor/database-role-credentials.json"
    )
    assert not credentials.exists()


def test_supervision_install_rejects_remote_database_before_writing_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    values["TEST_STAGING_DSN"] = (
        "postgresql://postgres:secret@db.example.invalid:55435/fixture_staging"
    )
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )

    with pytest.raises(DeploymentConfigError, match="non-loopback"):
        install_environment_services(environment, plan)

    credentials = (
        environment.runtime.state_root / "supervisor/database-role-credentials.json"
    )
    assert not credentials.exists()


def test_staging_clone_start_guard_requires_fresh_exclusive_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    descriptor = root / "fixture-staging/environment.yaml"
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    raw["readiness"]["requires_clone_rebind"] = True
    descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    environment = _load(root, "fixture-staging", values)
    report_path = environment.runtime.state_root / "clone-rebind.json"

    with pytest.raises(DeploymentConfigError, match="clone-rebind evidence"):
        environment_services._require_staging_clone_start_ready(environment)

    rebound_at = datetime.now(timezone.utc)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "schema": "okg.environment_clone_rebind/v1",
                "ok": True,
                "apply": True,
                "captured_at": rebound_at.isoformat(),
                "target": {
                    "environment_id": environment.id,
                    "graph_deployment_name": environment.graph_deployment_name,
                },
                "ordinary_sessions_read_only": False,
            }
        ),
        encoding="utf-8",
    )
    report_path.chmod(0o600)

    class Query:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Connection:
        def __init__(self):
            self.queries = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params=None):
            self.queries.append(str(query))
            if "database_owner" in str(query):
                return Query((environment.graph_deployment_name,))
            raise AssertionError(f"unexpected query: {query}")

    connection = Connection()
    monkeypatch.setattr(
        environment_services, "db_connect", lambda *_args, **_kwargs: connection,
    )
    import okg.substrate.generation_resolver as generation_resolver

    latest = SimpleNamespace(
        generation_id="okg:fixture-staging:branch:default:gen:test",
        deployment_name=environment.graph_deployment_name,
        status="published",
        node_count=10,
        edge_count=5,
        created_at=(rebound_at + timedelta(seconds=1)).isoformat(),
        target_generation_id=None,
    )
    monkeypatch.setattr(
        generation_resolver,
        "resolve_okg_generation",
        lambda *_args, **_kwargs: latest_holder[0],
    )
    generation_inventory = {
        "total": 1,
        "matching": 1,
        "foreign": 0,
        "root_lineage": True,
    }
    monkeypatch.setattr(
        generation_resolver,
        "chronos_generation_inventory",
        lambda *_args, **_kwargs: generation_inventory,
    )
    import okg.substrate.generation_bootstrap as generation_bootstrap

    bootstrap_inventory = {
        "ok": True,
        "canonical": True,
        "generation_zero_count": 1,
        "non_bootstrap_count": 1,
    }
    monkeypatch.setattr(
        generation_bootstrap,
        "formal_bootstrap_generation_inventory",
        lambda _conn: bootstrap_inventory,
    )

    latest_holder = [latest]
    environment_services._require_staging_clone_start_ready(environment)

    latest.created_at = rebound_at.isoformat()
    with pytest.raises(DeploymentConfigError, match="post-rebind"):
        environment_services._require_staging_clone_start_ready(environment)
    latest.created_at = (rebound_at - timedelta(seconds=1)).isoformat()
    with pytest.raises(DeploymentConfigError, match="post-rebind"):
        environment_services._require_staging_clone_start_ready(environment)
    latest.created_at = (rebound_at + timedelta(seconds=1)).isoformat()

    latest_holder[0] = None
    with pytest.raises(DeploymentConfigError, match="post-rebind"):
        environment_services._require_staging_clone_start_ready(environment)
    latest_holder[0] = latest

    latest.created_at = "not-a-timestamp"
    with pytest.raises(DeploymentConfigError, match="timestamp is invalid"):
        environment_services._require_staging_clone_start_ready(environment)
    latest.created_at = "2026-07-12T12:00:00"
    with pytest.raises(DeploymentConfigError, match="timestamp lacks timezone"):
        environment_services._require_staging_clone_start_ready(environment)
    latest.created_at = (rebound_at + timedelta(seconds=1)).isoformat()

    bootstrap_inventory["ok"] = False
    with pytest.raises(DeploymentConfigError, match="bootstrap generation"):
        environment_services._require_staging_clone_start_ready(environment)
    bootstrap_inventory["ok"] = True

    generation_inventory["total"] = 2
    generation_inventory["foreign"] = 1
    with pytest.raises(DeploymentConfigError, match="post-rebind staging generation"):
        environment_services._require_staging_clone_start_ready(environment)

    generation_inventory["total"] = 1
    generation_inventory["foreign"] = 0
    latest.target_generation_id = 1783890405156857
    environment_services._require_staging_clone_start_ready(environment)

    generation_inventory["root_lineage"] = False
    with pytest.raises(DeploymentConfigError, match="predecessor-free"):
        environment_services._require_staging_clone_start_ready(environment)


def test_staging_supervision_bypass_requires_current_strict_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    rebound_at = datetime.now(timezone.utc)
    artifact_id = "sha256:" + "a" * 64
    plan = {
        "plan_digest": "sha256:" + "b" * 64,
        "release_status": {"artifact_id": artifact_id},
    }
    path = environment.runtime.state_root / "supervisor/promotion-supervision.json"
    assert not environment_services._validated_staging_supervision_exists(
        environment,
        plan=plan,
        rebound_at=rebound_at,
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(DeploymentConfigError, match="supervision evidence"):
        environment_services._validated_staging_supervision_exists(
            environment,
            plan=plan,
            rebound_at=rebound_at,
        )

    receipt = {
        "schema": "okg.environment_service_supervision_evidence/v1",
        "ok": True,
        "status": "completed",
        "captured_at": (rebound_at + timedelta(seconds=1)).isoformat(),
        "environment_id": environment.id,
        "artifact_id": artifact_id,
        "plan_digest": plan["plan_digest"],
        "action": "start",
        "evidence_path": str(path),
        "process_authority": {"ok": True, "status": "healthy"},
        "service_installation": {"ok": True, "status": "current"},
        "release_handoff": {"ok": True, "status": "complete"},
        "results": [{"returncode": 0}],
    }
    path.write_text(json.dumps(receipt), encoding="utf-8")
    assert environment_services._validated_staging_supervision_exists(
        environment,
        plan=plan,
        rebound_at=rebound_at,
    )

    previous_artifact_id = artifact_id
    next_artifact_id = "sha256:" + "c" * 64
    plan["release_status"] = {
        "artifact_id": next_artifact_id,
        "previous_artifact_id": previous_artifact_id,
    }
    receipt["artifact_id"] = previous_artifact_id
    receipt["plan_digest"] = "sha256:" + "d" * 64
    path.write_text(json.dumps(receipt), encoding="utf-8")
    process_holder = {"ok": False, "status": "stopped"}
    installation_holder = {
        "ok": True,
        "status": "current",
        "previous_installation": {
            "artifact_id": previous_artifact_id,
            "plan_digest": receipt["plan_digest"],
        },
    }
    hosts_stopped = {"value": True}
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda _environment: process_holder,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: installation_holder,
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_host_services_stopped",
        lambda _plan: hosts_stopped["value"],
    )
    assert environment_services._validated_staging_supervision_exists(
        environment,
        plan=plan,
        rebound_at=rebound_at,
    )

    for unsafe_process_status in ("healthy", "invalid"):
        process_holder["status"] = unsafe_process_status
        with pytest.raises(DeploymentConfigError, match="supervision evidence"):
            environment_services._validated_staging_supervision_exists(
                environment,
                plan=plan,
                rebound_at=rebound_at,
            )
    process_holder["status"] = "stopped"

    hosts_stopped["value"] = False
    with pytest.raises(DeploymentConfigError, match="supervision evidence"):
        environment_services._validated_staging_supervision_exists(
            environment,
            plan=plan,
            rebound_at=rebound_at,
        )
    hosts_stopped["value"] = True

    installation_holder["status"] = "invalid"
    installation_holder["ok"] = False
    with pytest.raises(DeploymentConfigError, match="supervision evidence"):
        environment_services._validated_staging_supervision_exists(
            environment,
            plan=plan,
            rebound_at=rebound_at,
        )
    installation_holder["status"] = "current"
    installation_holder["ok"] = True

    previous_binding = installation_holder["previous_installation"]
    previous_binding["artifact_id"] = "sha256:" + "e" * 64
    with pytest.raises(DeploymentConfigError, match="supervision evidence"):
        environment_services._validated_staging_supervision_exists(
            environment,
            plan=plan,
            rebound_at=rebound_at,
        )
    previous_binding["artifact_id"] = previous_artifact_id

    previous_binding["plan_digest"] = "sha256:" + "f" * 64
    with pytest.raises(DeploymentConfigError, match="supervision evidence"):
        environment_services._validated_staging_supervision_exists(
            environment,
            plan=plan,
            rebound_at=rebound_at,
        )
    previous_binding["plan_digest"] = receipt["plan_digest"]

    receipt["captured_at"] = rebound_at.isoformat()
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(DeploymentConfigError, match="supervision evidence"):
        environment_services._validated_staging_supervision_exists(
            environment,
            plan=plan,
            rebound_at=rebound_at,
        )


def test_staging_clone_start_guard_holds_publish_lock_through_body(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    events: list[str] = []
    connection = object()

    @contextmanager
    def connect(*_args, **_kwargs):
        events.append("connect")
        yield connection
        events.append("disconnect")

    @contextmanager
    def publish_lock(**kwargs):
        assert kwargs["conn"] is connection
        assert kwargs["deployment"] == environment.graph_deployment_name
        assert kwargs["operation"] == "staging service start"
        events.append("lock")
        yield
        events.append("unlock")

    monkeypatch.setattr(environment_services, "db_connect", connect)
    monkeypatch.setattr(
        "okg.substrate.runtime.source_sync_priority.publisher_publish_lock_on_connection",
        publish_lock,
    )
    monkeypatch.setattr(
        environment_services,
        "_require_staging_clone_start_ready",
        lambda *_args, **_kwargs: events.append("validated"),
    )

    with environment_services._staging_clone_start_guard(environment, {}):
        events.append("dispatch")

    assert events == [
        "connect",
        "lock",
        "validated",
        "dispatch",
        "unlock",
        "disconnect",
    ]


def test_require_aware_datetime_normalizes_or_refuses_boundaries() -> None:
    expected = datetime(2026, 7, 12, 16, 0, tzinfo=timezone.utc)
    assert (
        environment_services._require_aware_datetime(
            "2026-07-12T12:00:00-04:00",
            label="fixture timestamp",
        )
        == expected
    )
    assert (
        environment_services._require_aware_datetime(
            expected,
            label="fixture timestamp",
        )
        == expected
    )

    class MissingOffset(tzinfo):
        def utcoffset(self, _value):
            return None

    class StatefulOffset(tzinfo):
        def __init__(self) -> None:
            self.calls = 0

        def utcoffset(self, _value):
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError("second callback refused")
            return timedelta(0)

    with pytest.raises(DeploymentConfigError, match="lacks timezone"):
        environment_services._require_aware_datetime(
            datetime(2026, 7, 12, 16, 0, tzinfo=MissingOffset()),
            label="fixture timestamp",
        )
    with pytest.raises(DeploymentConfigError, match="cannot normalize"):
        environment_services._require_aware_datetime(
            "9999-12-31T23:59:59-23:59",
            label="fixture timestamp",
        )
    with pytest.raises(DeploymentConfigError, match="cannot normalize"):
        environment_services._require_aware_datetime(
            datetime(2026, 7, 12, 16, 0, tzinfo=StatefulOffset()),
            label="fixture timestamp",
        )


def test_supervision_evidence_is_generated_from_live_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": "sha256:" + "a" * 64,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_handoff_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "complete"},
    )

    for incomplete_authority in (
        {"ok": True},
        {"ok": 1, "status": "healthy"},
    ):
        with pytest.raises(DeploymentConfigError, match="healthy process"):
            write_environment_service_supervision_evidence(
                environment,
                plan,
                action="start",
                results=[
                    {"returncode": 0},
                    {
                        "returncode": 0,
                        "process_authority": incomplete_authority,
                    },
                ],
            )

    results = [
        {"returncode": 0},
        {
            "returncode": 0,
            "process_authority": {"ok": True, "status": "healthy"},
        },
    ]
    evidence = write_environment_service_supervision_evidence(
        environment,
        plan,
        action="start",
        results=results,
    )

    path = Path(evidence["path"])
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert persisted["schema"] == ("okg.environment_service_supervision_evidence/v1")
    assert persisted["artifact_id"] == "sha256:" + "a" * 64
    assert persisted["plan_digest"] == plan["plan_digest"]
    assert persisted["release_handoff"]["ok"] is True
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_wait_for_service_authority_reports_convergence_and_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    attempts = {"count": 0}

    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "active"},
    )

    def worker_authority(*_args, **_kwargs):
        attempts["count"] += 1
        healthy = attempts["count"] >= 2
        return {"ok": healthy, "status": "healthy" if healthy else "missing"}

    def service_authority(*_args, **_kwargs):
        healthy = attempts["count"] >= 2
        return {"ok": healthy, "status": "healthy" if healthy else "missing"}

    monkeypatch.setattr(
        environment_services,
        "environment_worker_authority_status",
        worker_authority,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_authority_status",
        service_authority,
    )
    monkeypatch.setattr(environment_services.time, "sleep", lambda _seconds: None)

    converged = environment_services._wait_for_service_authority(
        environment,
        timeout_seconds=1,
    )
    assert converged["ok"] is True
    assert converged["status"] == "healthy"

    attempts["count"] = 0
    timed_out = environment_services._wait_for_service_authority(
        environment,
        timeout_seconds=0,
    )
    assert timed_out["ok"] is False
    assert timed_out["status"] == "invalid"
    assert timed_out["message"] == "supervised process authority did not converge"


def test_supervision_status_refuses_partial_authority_without_host_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    observed: list[list[str]] = []
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "invalid",
            "message": "foreign process",
        },
    )

    results = execute_environment_service_action(
        plan,
        action="status",
        environment=environment,
        runner=lambda argv: observed.append(list(argv)),  # type: ignore[arg-type]
    )

    assert observed == []
    assert results[0]["returncode"] == 1
    assert results[0]["process_authority"]["status"] == "invalid"


@pytest.mark.parametrize(
    ("adapter", "expected_host_commands"),
    (("launchd", 2), ("systemd", 1)),
)
def test_clone_host_fence_binds_adapter_command_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
    expected_host_commands: int,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter=adapter,
        environ=values,
    )
    _stub_host_fence_authority(monkeypatch, environment, plan)

    evidence = environment_services.fence_environment_host_services(
        plan,
        environment=environment,
        runner=lambda argv: subprocess.CompletedProcess(list(argv), 0, "", ""),
    )

    assert evidence["ok"] is True
    assert evidence["expected_host_command_count"] == expected_host_commands
    assert len(evidence["results"]) == expected_host_commands


def test_clone_systemd_fence_accepts_explicitly_inactive_disabled_units(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )
    _stub_host_fence_authority(monkeypatch, environment, plan)
    monkeypatch.setattr(
        environment_services,
        "_run_command",
        lambda argv: subprocess.CompletedProcess(
            list(argv),
            3 if "is-active" in argv else 1,
            "inactive\n" if "is-active" in argv else "disabled\n",
            "",
        ),
    )

    evidence = environment_services.fence_environment_host_services(
        plan,
        environment=environment,
        runner=lambda argv: subprocess.CompletedProcess(
            list(argv),
            1,
            "",
            "units are not loaded",
        ),
    )

    assert evidence["ok"] is True
    assert evidence["results"][0]["already_absent"] is True


def test_clone_systemd_fence_rejects_supervisor_probe_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        environ=values,
    )
    _stub_host_fence_authority(monkeypatch, environment, plan)
    monkeypatch.setattr(
        environment_services,
        "_run_command",
        lambda argv: subprocess.CompletedProcess(
            list(argv),
            1,
            "",
            "Failed to connect to bus",
        ),
    )

    with pytest.raises(DeploymentConfigError, match="failed to fence"):
        environment_services.fence_environment_host_services(
            plan,
            environment=environment,
            runner=lambda argv: subprocess.CompletedProcess(
                list(argv),
                1,
                "",
                "units are not loaded",
            ),
        )


def test_readiness_prerequisites_are_bound_to_sealed_release_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    environment = replace(
        environment,
        readiness=replace(
            environment.readiness,
            prerequisite_pacts=("required-pact",),
        ),
    )
    artifact_root = tmp_path / "sealed-artifact"
    sealed_repo = artifact_root / "repo"
    sealed_repo.mkdir(parents=True)
    observed: list[tuple[Path, str]] = []

    def status(repo_root: Path, change_id: str, *, include_graph: bool):
        observed.append((repo_root, change_id))
        assert include_graph is False
        return {
            "ok": True,
            "status": "verification",
            "retired": True,
            "evidence_gaps": [],
            "task_evidence_issues": [],
            "next": [],
            "errors": [],
        }

    monkeypatch.setattr("okg.substrate.pact.cli._status", status)

    result = environment_readiness._probe_pact_prerequisites(
        environment,
        release={
            "artifact_root": str(artifact_root),
            "artifact_id": "sha256:" + "a" * 64,
            "code_revision": "b" * 40,
        },
    )

    assert result["ok"] is True
    assert result["repo_root"] == str(sealed_repo)
    assert observed == [(sealed_repo, "required-pact")]


def test_supervision_recomputed_forged_plan_cannot_target_foreign_environment(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    staging = _load(root, "fixture-staging", values)
    production = _load(root, "fixture-production", values)
    plan = render_environment_services(staging, adapter="systemd", environ=values)
    forged = copy.deepcopy(plan)
    forged["commands"]["logs"][0]["argv"] = [
        "systemctl",
        "stop",
        production.runtime.worker_service,
    ]
    forged["plan_digest"] = environment_services._service_plan_digest(forged)
    observed: list[list[str]] = []

    with pytest.raises(DeploymentConfigError, match="differs from current"):
        execute_environment_service_action(
            forged,
            action="logs",
            environment=staging,
            runner=lambda argv: observed.append(list(argv)),  # type: ignore[arg-type]
        )

    assert observed == []


def test_supervision_cli_apply_status_combines_host_and_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, values = _environment_fixture(tmp_path)
    _set_values(monkeypatch, values)

    rc = main(
        [
            "--json",
            "environment",
            "service",
            "status",
            "fixture-staging",
            "--environments-root",
            str(root),
            "--adapter",
            "systemd",
            "--apply",
        ]
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["results"][0]["returncode"] == 1
    assert payload["results"][0]["process_authority"]["status"] == "stopped"


@pytest.mark.parametrize("action", ["stop", "logs"])
def test_supervision_mutating_or_observing_processes_requires_healthy_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    observed: list[list[str]] = []
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "stopped"},
    )

    with pytest.raises(DeploymentConfigError, match="healthy process authority"):
        execute_environment_service_action(
            plan,
            action=action,
            environment=environment,
            runner=lambda argv: observed.append(list(argv)),  # type: ignore[arg-type]
        )
    assert observed == []


def test_activation_handoff_requires_install_restart_and_exact_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_id = "sha256:" + "a" * 64
    definition_digest = "sha256:" + "b" * 64
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": artifact_id,
        "definition_digest": definition_digest,
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": None,
    }
    process_status = {
        "ok": False,
        "status": "invalid",
        "message": "running processes still use the predecessor",
    }
    monkeypatch.setattr(
        environment_services,
        "_environment_release_status_locked",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_process_authority_status",
        lambda *_args, **_kwargs: dict(process_status),
    )
    monkeypatch.setattr(
        environment_services,
        "installed_environment_service_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "current",
            "plan_digest": "sha256:" + "d" * 64,
            "plan": None,
        },
    )

    incomplete = environment_release_handoff_status(environment, environ=values)

    assert incomplete["ok"] is False
    assert incomplete["status"] == "incomplete"
    assert [row["operation"] for row in incomplete["required_actions"]] == [
        "environment.service.install",
        "environment.service.restart",
        "environment.service.status",
    ]
    assert incomplete["required_actions"][2]["expected_artifact_id"] == artifact_id
    assert incomplete["required_actions"][2]["expected_definition_digest"] == (
        definition_digest
    )

    process_status.update({"ok": True, "status": "healthy"})
    complete = environment_release_handoff_status(environment, environ=values)

    assert complete["ok"] is True
    assert complete["status"] == "complete"
    assert complete["required_actions"] == []


def test_activation_coordinator_installs_restarts_and_requires_exact_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_id = "sha256:" + "a" * 64
    definition_digest = "sha256:" + "b" * 64
    pointer_digest = "sha256:" + "c" * 64
    release = SimpleNamespace(artifact_id=artifact_id)
    activation = SimpleNamespace(
        artifact_id=artifact_id,
        definition_digest=definition_digest,
        active_pointer_digest=pointer_digest,
        changed=True,
        to_dict=lambda: {"artifact_id": artifact_id, "changed": True},
    )
    handoffs = iter(
        [
            {"ok": False, "status": "incomplete"},
            {"ok": True, "status": "complete"},
        ]
    )
    monkeypatch.setattr(
        release_activation,
        "activate_environment_release",
        lambda **_kwargs: activation,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_handoff_status",
        lambda *_args, **_kwargs: next(handoffs),
    )
    plan = {
        "plan_digest": "sha256:" + "d" * 64,
        "release_status": {"previous_artifact_id": "sha256:" + "e" * 64},
    }
    monkeypatch.setattr(
        environment_services,
        "render_environment_services",
        lambda *_args, **_kwargs: plan,
    )
    monkeypatch.setattr(
        environment_services,
        "install_environment_services",
        lambda *_args, **_kwargs: SimpleNamespace(
            to_dict=lambda: {"environment_id": environment.id, "paths": []}
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "reconcile_environment_database_credentials",
        lambda *_args, **_kwargs: {"ok": True},
    )
    actions: list[str] = []

    def execute(_plan, *, action, **_kwargs):
        actions.append(action)
        return [{"returncode": 0}]

    monkeypatch.setattr(
        environment_services,
        "execute_environment_service_action",
        execute,
    )
    monkeypatch.setattr(
        environment_services,
        "write_environment_service_supervision_evidence",
        lambda *_args, **_kwargs: {"ok": True},
    )

    result = environment_services.activate_environment_release_with_handoff(
        environment,
        release,
        source_repo=tmp_path,
        activated_by="operator@example.test",
        adapter="systemd",
        environ=values,
    )

    assert result["ok"] is True
    assert result["status"] == "complete"
    assert result["service_action"] == "restart"
    assert result["handoff"] == {"ok": True, "status": "complete"}
    assert actions == ["install", "restart"]


def test_activation_coordinator_retry_after_handoff_has_no_service_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_id = "sha256:" + "a" * 64
    definition_digest = "sha256:" + "b" * 64
    pointer_digest = "sha256:" + "c" * 64
    monkeypatch.setattr(
        release_activation,
        "activate_environment_release",
        lambda **_kwargs: SimpleNamespace(
            artifact_id=artifact_id,
            definition_digest=definition_digest,
            active_pointer_digest=pointer_digest,
            changed=False,
            to_dict=lambda: {"artifact_id": artifact_id, "changed": False},
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_handoff_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "complete"},
    )
    monkeypatch.setattr(
        environment_services,
        "render_environment_services",
        lambda *_args, **_kwargs: pytest.fail("retry rendered a service plan"),
    )

    result = environment_services.activate_environment_release_with_handoff(
        environment,
        SimpleNamespace(artifact_id=artifact_id),
        source_repo=tmp_path,
        activated_by="operator@example.test",
        adapter="systemd",
        environ=values,
    )

    assert result["ok"] is True
    assert result["changed"] is False
    assert result["service_action"] is None
    assert result["install_results"] == []
    assert result["results"] == []


def test_activation_handoff_rejects_expected_pointer_changed_by_concurrent_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_a = "sha256:" + "a" * 64
    artifact_b = "sha256:" + "b" * 64
    definition_a = "sha256:" + "c" * 64
    definition_b = "sha256:" + "d" * 64
    pointer_a = "sha256:" + "e" * 64
    pointer_b = "sha256:" + "f" * 64
    process_read = False

    monkeypatch.setattr(
        environment_services,
        "_environment_release_status_locked",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": artifact_b,
            "definition_digest": definition_b,
            "active_pointer_digest": pointer_b,
            "previous_artifact_id": artifact_a,
        },
    )

    def process_status(*_args, **_kwargs):
        nonlocal process_read
        process_read = True
        return {"ok": True, "status": "healthy"}

    monkeypatch.setattr(
        environment_services,
        "environment_process_authority_status",
        process_status,
    )
    monkeypatch.setattr(
        environment_services,
        "installed_environment_service_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "current",
            "plan_digest": "sha256:" + "d" * 64,
            "plan": None,
        },
    )

    status = environment_release_handoff_status(
        environment,
        environ=values,
        expected_artifact_id=artifact_a,
        expected_definition_digest=definition_a,
        expected_pointer_digest=pointer_a,
    )

    assert status["ok"] is False
    assert status["status"] == "invalid"
    assert "active release changed" in status["message"]
    assert process_read is False


def test_handoff_readback_holds_shared_pointer_lock_against_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact = "sha256:" + "a" * 64
    definition = "sha256:" + "b" * 64
    pointer = "sha256:" + "c" * 64
    readback_entered = threading.Event()
    allow_readback = threading.Event()
    activation_entered = threading.Event()
    failures: list[BaseException] = []
    monkeypatch.setattr(
        environment_services,
        "_environment_release_status_locked",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": artifact,
            "definition_digest": definition,
            "active_pointer_digest": pointer,
            "previous_artifact_id": None,
        },
    )

    def process_status(*_args, **_kwargs):
        readback_entered.set()
        assert allow_readback.wait(timeout=5)
        return {"ok": True, "status": "healthy"}

    monkeypatch.setattr(
        environment_services,
        "environment_process_authority_status",
        process_status,
    )
    monkeypatch.setattr(
        environment_services,
        "installed_environment_service_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "current",
            "plan_digest": "sha256:" + "d" * 64,
            "plan": None,
        },
    )

    def readback() -> None:
        try:
            result = environment_release_handoff_status(
                environment,
                environ=values,
                expected_artifact_id=artifact,
                expected_definition_digest=definition,
                expected_pointer_digest=pointer,
            )
            assert result["ok"] is True
        except BaseException as exc:  # noqa: BLE001 - thread proof captures it.
            failures.append(exc)

    def activate() -> None:
        try:
            with release_activation.release_activation_lock(
                environment.release.active_pointer
            ):
                activation_entered.set()
        except BaseException as exc:  # noqa: BLE001 - thread proof captures it.
            failures.append(exc)

    readback_thread = threading.Thread(target=readback)
    activation_thread = threading.Thread(target=activate)
    readback_thread.start()
    assert readback_entered.wait(timeout=5)
    activation_thread.start()
    try:
        assert not activation_entered.wait(timeout=0.2)
    finally:
        allow_readback.set()
        readback_thread.join(timeout=5)
        activation_thread.join(timeout=5)
    assert not readback_thread.is_alive()
    assert not activation_thread.is_alive()
    assert activation_entered.is_set()
    assert failures == []


def test_supervision_restart_rotates_fully_verified_previous_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    current_artifact = "sha256:" + "c" * 64
    previous_artifact = "sha256:" + "a" * 64
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": current_artifact,
        "previous_artifact_id": previous_artifact,
    }
    plan["launchable"] = True
    plan["release_status"] = dict(release_status)
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_release_authority_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "artifact_id": previous_artifact,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "invalid",
            "message": "active artifact mismatch",
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    inventory = {
        "previous_artifact_id": previous_artifact,
        "services": {},
    }
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        environment_services,
        "_attest_restart_inventory_stopped",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_record_predecessor_stop",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_wait_for_service_authority",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    authority_path = environment.runtime.state_root / "worker-authority.json"
    authority_path.parent.mkdir(parents=True, exist_ok=True)
    authority_path.write_text("{}\n", encoding="utf-8")
    observed_commands: list[tuple[list[str], bool]] = []

    def run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        observed_commands.append((list(argv), authority_path.exists()))
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    results = execute_environment_service_action(
        plan,
        action="restart",
        environment=environment,
        runner=run,
    )

    assert observed_commands
    assert all(row["returncode"] == 0 for row in results)
    stop_index = next(
        index for index, (argv, _exists) in enumerate(observed_commands) if "stop" in argv
    )
    start_index = next(
        index
        for index, (argv, _exists) in enumerate(observed_commands)
        if "enable" in argv and "--now" in argv
    )
    assert observed_commands[stop_index][1] is True
    assert observed_commands[start_index][1] is False


@pytest.mark.parametrize("failure_phase", ["database", "stop"])
def test_restart_failure_before_attested_stop_preserves_authority_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": plan["handoff_topology"]["generations"]["current"][
            "definition_digest"
        ],
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": "sha256:" + "d" * 64,
    }
    plan["launchable"] = True
    plan["release_status"] = dict(release_status)
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "invalid",
            "message": "predecessor still active",
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: {"previous_artifact_id": release_status["previous_artifact_id"]},
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_stop_target_is_stopped",
        lambda *_args, **_kwargs: False,
    )
    authority = environment.runtime.state_root / "worker-authority.json"
    authority.parent.mkdir(parents=True, exist_ok=True)
    authority.write_text("predecessor\n", encoding="utf-8")

    def run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        should_fail = (
            failure_phase == "database"
            and "compose" in argv
            and "up" in argv
        ) or (failure_phase == "stop" and "stop" in argv)
        return subprocess.CompletedProcess(list(argv), 1 if should_fail else 0, "", "failed")

    results = execute_environment_service_action(
        plan,
        action="restart",
        environment=environment,
        runner=run,
    )

    assert any(row["returncode"] == 1 for row in results)
    assert authority.read_text(encoding="utf-8") == "predecessor\n"


@pytest.mark.parametrize(
    ("status", "started_at", "attempts", "updated_at", "accepted"),
    [
        ("ERROR", 1_000, 2, 1_100, False),
        ("CANCELLED", 1_000, 2, 1_100, False),
        ("MAX_RECOVERY_ATTEMPTS_EXCEEDED", 1_000, 10, 1_100, False),
        ("PENDING", 2_100, 2, 2_100, True),
        ("PENDING", 1_000, 3, 2_100, True),
        ("PENDING", 1_000, 2, 1_100, False),
    ],
)
def test_restart_recovery_requires_successor_owned_identity_preserving_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    started_at: int,
    attempts: int,
    updated_at: int,
    accepted: bool,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    inputs = '{"workflow":"same"}'
    captured = (
        {
            "workflow_id": "workflow-1",
            "started_at_epoch_ms": 1_000,
            "recovery_attempts": 2,
            "application_version": "app-v1",
            "inputs_present": True,
            "input_digest": environment_services.hashlib.sha256(
                inputs.encode("utf-8")
            ).hexdigest(),
            "updated_at_epoch_ms": 1_100,
        },
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs) -> None:
            return None

        def fetchall(self):
            return [
                (
                    "workflow-1",
                    status,
                    started_at,
                    attempts,
                    "app-v1",
                    inputs,
                    updated_at,
                )
            ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(
        environment_services, "db_connect", lambda *_args, **_kwargs: Connection(),
    )

    if accepted:
        environment_services._wait_for_dbos_workflow_recovery(
            environment,
            captured=captured,
            replacement_started_after_epoch_ms=2_000,
            timeout_seconds=0,
        )
    else:
        with pytest.raises(DeploymentConfigError, match="recovery"):
            environment_services._wait_for_dbos_workflow_recovery(
                environment,
                captured=captured,
                replacement_started_after_epoch_ms=2_000,
                timeout_seconds=0,
            )


def test_supervision_install_accepts_only_verified_previous_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    current_artifact = "sha256:" + "c" * 64
    previous_artifact = "sha256:" + "a" * 64
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": current_artifact,
        "previous_artifact_id": previous_artifact,
    }
    plan["launchable"] = True
    plan["release_status"] = dict(release_status)
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_release_authority_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "artifact_id": previous_artifact,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "invalid",
            "message": "active artifact mismatch",
        },
    )
    observed_releases: list[Mapping[str, Any]] = []
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda _environment, *, plan, current_release: (
            observed_releases.append(current_release)
            or {"ok": True}
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_installation_binding",
        lambda *_args, **_kwargs: {
            "artifact_id": previous_artifact,
            "plan_digest": "sha256:" + "d" * 64,
            "adapter": plan["adapter"],
            "systemd_scope": plan["systemd_scope"],
            "launcher_python": plan["launcher"]["python"],
            "postgres_image": plan["postgres_image"],
            "service_topology": plan["handoff_topology"]["generations"]["current"],
        },
    )

    installed = install_environment_services(environment, plan)

    assert installed.environment_id == environment.id
    assert (
        environment.runtime.state_root / "supervisor/install.json"
    ) in installed.paths
    assert observed_releases == [release_status]


def test_supervision_install_refuses_partial_previous_process_authority_before_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    current_artifact = "sha256:" + "c" * 64
    previous_artifact = "sha256:" + "a" * 64
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": current_artifact,
        "previous_artifact_id": previous_artifact,
    }
    plan["launchable"] = True
    plan["release_status"] = dict(release_status)
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_release_authority_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "artifact_id": previous_artifact,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **kwargs: (
            {
                "ok": False,
                "status": "invalid",
                "message": "active artifact mismatch",
            }
            if kwargs.get("release_status") is None
            else {
                "ok": False,
                "status": "invalid",
                "message": "partial previous authority",
            }
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DeploymentConfigError("unknown or foreign process")
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_installation_binding",
        lambda *_args, **_kwargs: {
            "artifact_id": previous_artifact,
            "plan_digest": "sha256:" + "d" * 64,
            "adapter": plan["adapter"],
            "systemd_scope": plan["systemd_scope"],
            "launcher_python": plan["launcher"]["python"],
            "postgres_image": plan["postgres_image"],
            "service_topology": plan["handoff_topology"]["generations"]["current"],
        },
    )

    with pytest.raises(
        DeploymentConfigError,
        match="unknown or foreign process",
    ):
        install_environment_services(environment, plan)

    assert not (
        environment.runtime.state_root / "supervisor/install.json"
    ).exists()
    assert not (
        environment.runtime.state_root / "supervisor/postgres-password"
    ).exists()


def test_install_refuses_missing_predecessor_binding_before_any_file_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    _stub_receiptless_managers_stopped(monkeypatch)
    original_plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    installed = install_environment_services(environment, original_plan)
    assert installed.paths

    def state_census() -> dict[str, tuple[bytes, int]]:
        return {
            str(path.relative_to(environment.runtime.state_root)): (
                path.read_bytes(),
                stat.S_IMODE(path.stat().st_mode),
            )
            for path in environment.runtime.state_root.rglob("*")
            if path.is_file() and not path.is_symlink()
        }

    before = state_census()
    changed_plan = copy.deepcopy(original_plan)
    changed_plan["release_status"]["artifact_id"] = "sha256:" + "b" * 64
    changed_plan["release_status"]["previous_artifact_id"] = "sha256:" + "a" * 64
    service_artifact = next(
        row
        for row in changed_plan["artifacts"]
        if row["kind"] == "worker_service"
    )
    service_artifact["content"] += "\n# must-not-be-written\n"
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(changed_plan["release_status"]),
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "invalid",
            "message": "active receipt belongs to the predecessor",
        },
    )
    reconciliation_calls: list[str] = []

    def forbidden_reconciliation(*_args, **_kwargs):
        reconciliation_calls.append("restart_inventory")
        (environment.runtime.state_root / "forbidden-rebind").write_text(
            "mutated\n",
            encoding="utf-8",
        )
        return {"ok": True}

    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        forbidden_reconciliation,
    )

    with pytest.raises(
        DeploymentConfigError,
        match="predecessor service installation binding",
    ):
        install_environment_services(environment, changed_plan)

    assert reconciliation_calls == []
    assert state_census() == before


def test_receiptless_install_and_start_refuse_live_manager_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "stopped",
            "message": "all receipts missing",
        },
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(plan["release_status"]),
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    manager_state = {"status": "running", "pid": 424242}

    def manager(*_args, **_kwargs):
        return {
            "ok": True,
            **manager_state,
            "manager_outcome": {
                "adapter": "systemd",
                "load_state": "loaded",
                "result": "success",
                "exec_main_code": 1,
                "exec_main_status": 0,
                "exit_status_observed": True,
            },
        }

    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        manager,
    )

    before = {
        str(path.relative_to(environment.runtime.state_root)): path.read_bytes()
        for path in environment.runtime.state_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    with pytest.raises(DeploymentConfigError, match="receipt-less service mutation"):
        install_environment_services(environment, plan)
    after = {
        str(path.relative_to(environment.runtime.state_root)): path.read_bytes()
        for path in environment.runtime.state_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    assert after == before

    manager_state.update({"status": "stopped", "pid": None})
    manager_outcome = manager()["manager_outcome"]
    manager_outcome.update(
        {
            "load_state": "not-found",
            "exec_main_code": 0,
            "exec_main_status": 0,
        }
    )

    def stopped_manager(*_args, **_kwargs):
        return {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": manager_outcome,
        }

    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        stopped_manager,
    )
    install_environment_services(environment, plan)
    manager_state.update({"status": "running", "pid": 424242})
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        manager,
    )
    plan["release_status"] = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
    }
    plan["launchable"] = True
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    invoked: list[list[str]] = []
    with pytest.raises(DeploymentConfigError, match="receipt-less service mutation"):
        execute_environment_service_action(
            plan,
            action="start",
            environment=environment,
            runner=lambda argv: invoked.append(list(argv))
            or subprocess.CompletedProcess(list(argv), 0, "", ""),
        )
    assert invoked == []


def test_supervision_install_blocks_concurrent_release_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    first_write_started = threading.Event()
    allow_install_write = threading.Event()
    activation_attempted = threading.Event()
    activation_entered = threading.Event()
    failures: list[Exception] = []
    original_atomic_write = environment_services._atomic_write

    def blocking_atomic_write(*args: Any, **kwargs: Any) -> None:
        first_write_started.set()
        assert allow_install_write.wait(timeout=5)
        original_atomic_write(*args, **kwargs)

    monkeypatch.setattr(
        environment_services,
        "_atomic_write",
        blocking_atomic_write,
    )

    def install() -> None:
        try:
            install_environment_services(environment, plan)
        except Exception as exc:
            failures.append(exc)

    def activate() -> None:
        try:
            activation_attempted.set()
            with release_activation.release_activation_lock(
                environment.release.active_pointer
            ):
                activation_entered.set()
        except Exception as exc:
            failures.append(exc)

    install_thread = threading.Thread(target=install)
    install_thread.start()
    assert first_write_started.wait(timeout=5)
    activation_thread = threading.Thread(target=activate)
    activation_thread.start()
    try:
        assert activation_attempted.wait(timeout=5)
        assert not activation_entered.wait(timeout=0.2)
    finally:
        allow_install_write.set()
        install_thread.join(timeout=5)
        activation_thread.join(timeout=5)

    assert not install_thread.is_alive()
    assert not activation_thread.is_alive()
    assert activation_entered.is_set()
    assert failures == []


def test_file_install_rejects_activation_that_wins_before_shared_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    _stub_receiptless_managers_stopped(monkeypatch)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    advanced = {
        **dict(plan["release_status"]),
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": plan["handoff_topology"]["generations"]["current"][
            "definition_digest"
        ],
        "active_pointer_digest": "sha256:" + "c" * 64,
    }

    @contextmanager
    def activation_won(*_args: Any, **_kwargs: Any):
        monkeypatch.setattr(
            environment_services,
            "environment_release_status",
            lambda *_inner_args, **_inner_kwargs: dict(advanced),
        )
        yield

    monkeypatch.setattr(
        environment_services,
        "release_activation_lock",
        activation_won,
    )

    with pytest.raises(DeploymentConfigError, match="active release changed"):
        install_environment_services(environment, plan)

    assert not (
        environment.runtime.state_root / "supervisor/install.json"
    ).exists()


def test_install_action_rejects_activation_between_precheck_and_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    before = dict(plan["release_status"])
    after = {
        **before,
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "d" * 64,
        "definition_digest": "sha256:" + "e" * 64,
        "active_pointer_digest": "sha256:" + "f" * 64,
    }
    release = {"value": before}
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release["value"]),
    )

    @contextmanager
    def activation_won(*_args: Any, **_kwargs: Any):
        release["value"] = after
        yield

    monkeypatch.setattr(
        environment_services,
        "release_activation_lock",
        activation_won,
    )
    observed: list[list[str]] = []

    with pytest.raises(DeploymentConfigError, match="active release changed"):
        execute_environment_service_action(
            plan,
            action="install",
            environment=environment,
            runner=lambda argv: (
                observed.append(list(argv))
                or subprocess.CompletedProcess(list(argv), 0, "", "")
            ),
        )

    assert observed == []


def test_supervision_restart_refuses_invalid_previous_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    current_artifact = "sha256:" + "c" * 64
    previous_artifact = "sha256:" + "a" * 64
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": current_artifact,
        "previous_artifact_id": previous_artifact,
    }
    plan["launchable"] = True
    plan["release_status"] = dict(release_status)
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_release_authority_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "artifact_id": previous_artifact,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **kwargs: (
            {
                "ok": False,
                "status": "invalid",
                "message": "active artifact mismatch",
            }
            if kwargs.get("release_status") is None
            else {
                "ok": False,
                "status": "invalid",
                "message": "partial previous authority",
            }
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DeploymentConfigError("unknown or foreign process")
        ),
    )
    observed_commands: list[list[str]] = []

    with pytest.raises(
        DeploymentConfigError,
        match="unknown or foreign process",
    ):
        execute_environment_service_action(
            plan,
            action="restart",
            environment=environment,
            runner=lambda argv: (
                observed_commands.append(list(argv))
                or subprocess.CompletedProcess(list(argv), 0, "", "")
            ),
        )

    assert observed_commands == []


def test_supervision_restart_refuses_manager_process_behind_dead_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": plan["handoff_topology"]["generations"]["current"][
            "definition_digest"
        ],
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": None,
    }

    monkeypatch.setattr(
        environment_services,
        "environment_service_authority_status",
        lambda _environment, *, service_id, **_kwargs: {
            "schema": "okg.environment_service_authority_status/v1",
            "ok": False,
            "status": "invalid",
            "environment_id": environment.id,
            "service_id": service_id,
            "error": "DeploymentConfigError",
            "reason_code": "service_process_not_alive",
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid(),
        },
    )

    with pytest.raises(
        DeploymentConfigError,
        match="service-manager process without live authority",
    ):
        environment_services._restart_process_inventory(
            environment,
            plan=plan,
            current_release=release_status,
        )


def test_restart_admits_dead_launcher_receipt_before_worker_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": plan["handoff_topology"]["generations"]["current"][
            "definition_digest"
        ],
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": None,
    }
    worker_service = environment.runtime.worker_service
    worker_receipt = (
        environment.runtime.state_root
        / "service-authority"
        / f"{worker_service}.json"
    )
    worker_receipt.parent.mkdir(parents=True)
    worker_receipt.write_text(
        json.dumps(
            {
                "schema": "okg.service_release_authority/v1",
                "pid": 424242,
                "process_start_identity": "failed-before-readiness",
            }
        ),
        encoding="utf-8",
    )
    worker_receipt.chmod(0o600)

    def service_authority(
        _environment: Any,
        *,
        service_id: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        if service_id == worker_service:
            return {
                "schema": "okg.environment_service_authority_status/v1",
                "ok": False,
                "status": "invalid",
                "environment_id": environment.id,
                "service_id": service_id,
                "path": str(worker_receipt),
                "error": "DeploymentConfigError",
                "reason_code": "service_process_not_alive",
            }
        return {
            "schema": "okg.environment_service_authority_status/v1",
            "ok": False,
            "status": "missing",
            "environment_id": environment.id,
            "service_id": service_id,
        }

    monkeypatch.setattr(
        environment_services,
        "environment_service_authority_status",
        service_authority,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_worker_authority_status",
        lambda *_args, **_kwargs: {
            "schema": "okg.environment_worker_authority_status/v1",
            "ok": False,
            "status": "missing",
            "environment_id": environment.id,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda _environment, *, service_id, **_kwargs: {
            "ok": True,
            "status": "failed" if service_id == worker_service else "stopped",
            "pid": None,
            "manager_outcome": {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "exit-code" if service_id == worker_service else "success",
                "exec_main_code": 1,
                "exec_main_status": 3 if service_id == worker_service else 0,
            },
        },
    )

    inventory = environment_services._restart_process_inventory(
        environment,
        plan=plan,
        current_release=release_status,
    )

    assert inventory["services"][worker_service]["state"] == "stopped"
    assert inventory["worker"] == {
        "generation": "current",
        "state": "stopped",
        "identity": {
            "pid": 424242,
            "process_start_identity": "failed-before-readiness",
        },
        "authority": {
            "schema": "okg.service_release_authority/v1",
            "pid": 424242,
            "process_start_identity": "failed-before-readiness",
        },
        "readiness_receipt_missing": True,
    }


def test_restart_refuses_missing_worker_readiness_for_live_launcher(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": plan["handoff_topology"]["generations"]["current"][
            "definition_digest"
        ],
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": None,
    }
    worker_service = environment.runtime.worker_service
    identity = {"pid": 424242, "process_start_identity": "still-live"}
    monkeypatch.setattr(
        environment_services,
        "environment_service_authority_status",
        lambda _environment, *, service_id, **_kwargs: (
            {
                "ok": True,
                "status": "healthy",
                "authority": dict(identity),
                "supervisor": {"ok": True, "status": "running", "pid": 424242},
            }
            if service_id == worker_service
            else {"ok": False, "status": "missing"}
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_worker_authority_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "missing"},
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "stopped", "pid": None},
    )

    with pytest.raises(
        DeploymentConfigError,
        match="worker authority does not match",
    ):
        environment_services._restart_process_inventory(
            environment,
            plan=plan,
            current_release=release_status,
        )


def test_supervision_stop_removes_only_target_authority_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    staging = _load(root, "fixture-staging", values)
    production = _load(root, "fixture-production", values)
    plan = render_environment_services(staging, adapter="systemd", environ=values)
    target_paths = [
        staging.runtime.state_root / "worker-authority.json",
        *(
            staging.runtime.state_root / "service-authority" / f"{service_id}.json"
            for service_id in (staging.runtime.worker_service, staging.ui.service)
        ),
    ]
    foreign = production.runtime.state_root / "worker-authority.json"
    for path in [*target_paths, foreign]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {"ok": True, "artifact_id": "sha256:" + "a" * 64},
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: {"services": {}, "worker": {}},
    )
    monkeypatch.setattr(
        environment_services,
        "_attest_restart_inventory_stopped",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_record_predecessor_stop",
        lambda *_args, **_kwargs: None,
    )

    results = execute_environment_service_action(
        plan,
        action="stop",
        environment=staging,
        runner=lambda argv: subprocess.CompletedProcess(list(argv), 0, "", ""),
    )

    assert all(row["returncode"] == 0 for row in results)
    assert all(not path.exists() for path in target_paths)
    assert foreign.exists()


@pytest.mark.parametrize(
    "retry_snapshot",
    ["database_stopped", "pending", "restart", "restart_pending"],
)
def test_supervision_stop_retry_requires_exact_stopped_database_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    retry_snapshot: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": "sha256:" + "b" * 64,
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": "sha256:" + "d" * 64,
        "previous_definition_digest": "sha256:" + "e" * 64,
        "previous_pointer_digest": "sha256:" + "f" * 64,
    }
    plan["release_status"] = dict(release_status)
    plan["launchable"] = True
    service_ids = tuple(plan["handoff_topology"]["stop_services"])
    generation = environment_services._service_generation_topology(
        environment,
        definition_digest=release_status["definition_digest"],
    )
    plan["handoff_topology"]["generations"] = {
        "current": generation,
        "previous": {
            **generation,
            "definition_digest": release_status["previous_definition_digest"],
        },
    }
    inventory = {
        "target_artifact_id": release_status["artifact_id"],
        "target_definition_digest": release_status["definition_digest"],
        "target_pointer_digest": release_status["active_pointer_digest"],
        "previous_artifact_id": release_status["previous_artifact_id"],
        "previous_definition_digest": release_status[
            "previous_definition_digest"
        ],
        "previous_pointer_digest": release_status["previous_pointer_digest"],
        "plan_digest": plan["plan_digest"],
        "services": {
            service_id: {
                "generation": "absent",
                "state": "stopped",
                "identity": None,
            }
            for service_id in service_ids
        },
        "worker": {"generation": "absent", "state": "stopped", "identity": None},
    }
    record_path = environment_services._predecessor_stop_path(environment)
    database_stopped = False
    database_restart_completed = False
    command_count = 0

    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: (
            {"ok": False, "status": "invalid", "message": "receipts removed"}
            if record_path.exists()
            else {"ok": True, "status": "healthy"}
        ),
    )

    def restart_inventory(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        result = copy.deepcopy(inventory)
        if record_path.exists():
            raw = json.loads(record_path.read_text(encoding="utf-8"))
            result.update(
                {
                    "recovered_from_stop_record": True,
                    "predecessor_processes": raw["predecessor_processes"],
                    "stopped_processes": raw["stopped_processes"],
                }
            )
        return result

    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        restart_inventory,
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "stopped", "pid": None},
    )
    monkeypatch.setattr(
        environment_services,
        "_managed_postgres_target_is_stopped",
        lambda _plan: database_stopped,
    )

    def pending(_environment: Any) -> tuple[dict[str, Any], ...]:
        if not database_stopped:
            if retry_snapshot == "restart_pending" and database_restart_completed:
                return ({"workflow_id": "late-workflow"},)
            return ()
        if retry_snapshot == "pending":
            return ({"workflow_id": "late-workflow"},)
        try:
            raise psycopg.OperationalError("managed postgres is stopped")
        except psycopg.OperationalError as exc:
            raise DeploymentConfigError(
                "DBOS workflow authority snapshot failed before service mutation"
            ) from exc

    monkeypatch.setattr(environment_services, "_pending_dbos_workflows", pending)

    def run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        nonlocal database_restart_completed, database_stopped, command_count
        command_count += 1
        if environment_services._is_database_stop_command(argv):
            database_stopped = True
        elif environment_services._is_database_start_command(argv):
            database_stopped = False
            database_restart_completed = True
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    first = execute_environment_service_action(
        plan,
        action="stop",
        environment=environment,
        runner=run,
    )
    assert all(row["returncode"] == 0 for row in first)
    assert database_stopped is True
    before_retry = command_count

    if retry_snapshot == "database_stopped":
        second = execute_environment_service_action(
            plan,
            action="stop",
            environment=environment,
            runner=run,
        )
        assert all(row["returncode"] == 0 for row in second)
        assert command_count > before_retry
    elif retry_snapshot == "pending":
        with pytest.raises(DeploymentConfigError, match="late-workflow"):
            execute_environment_service_action(
                plan,
                action="stop",
                environment=environment,
                runner=run,
            )
        assert command_count == before_retry
    elif retry_snapshot == "restart":
        monkeypatch.setattr(
            environment_services,
            "_wait_for_service_authority",
            lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
        )
        second = execute_environment_service_action(
            plan,
            action="restart",
            environment=environment,
            runner=run,
        )
        assert all(row["returncode"] == 0 for row in second)
        assert database_stopped is False
        assert command_count > before_retry + 1
    else:
        with pytest.raises(DeploymentConfigError, match="late-workflow"):
            execute_environment_service_action(
                plan,
                action="restart",
                environment=environment,
                runner=run,
            )
        assert database_stopped is False
        assert command_count == before_retry + 1


def test_supervision_stop_detects_pending_workflow_at_host_stop_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": "sha256:" + "b" * 64,
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": None,
        "previous_definition_digest": None,
        "previous_pointer_digest": None,
    }
    plan["release_status"] = dict(release_status)
    inventory = {
        "target_artifact_id": release_status["artifact_id"],
        "target_definition_digest": release_status["definition_digest"],
        "target_pointer_digest": release_status["active_pointer_digest"],
        "previous_artifact_id": None,
        "previous_definition_digest": None,
        "previous_pointer_digest": None,
        "plan_digest": plan["plan_digest"],
        "services": {},
        "worker": {"generation": "current", "state": "running"},
    }
    host_stop_completed = False
    pending_reads = 0
    recorded: list[Mapping[str, Any]] = []
    receipts_removed: list[bool] = []
    observed_commands: list[list[str]] = []

    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(release_status),
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        environment_services,
        "_attest_restart_inventory_stopped",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_record_predecessor_stop",
        lambda _environment, *, inventory, **_kwargs: recorded.append(inventory),
    )
    monkeypatch.setattr(
        environment_services,
        "_remove_environment_authority_receipts",
        lambda *_args, **_kwargs: receipts_removed.append(True),
    )

    def pending(_environment: Any) -> tuple[dict[str, Any], ...]:
        nonlocal pending_reads
        pending_reads += 1
        if pending_reads == 1:
            assert host_stop_completed is False
            return ()
        assert host_stop_completed is True
        return ({"workflow_id": "boundary-race-workflow"},)

    monkeypatch.setattr(environment_services, "_pending_dbos_workflows", pending)

    def run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        nonlocal host_stop_completed
        observed_commands.append(list(argv))
        assert not environment_services._is_database_stop_command(argv)
        host_stop_completed = True
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    with pytest.raises(DeploymentConfigError, match="boundary-race-workflow"):
        execute_environment_service_action(
            plan,
            action="stop",
            environment=environment,
            runner=run,
        )

    assert pending_reads == 2
    assert len(observed_commands) == 1
    assert recorded == [inventory]
    assert receipts_removed == []


def test_launchd_partial_start_restart_retry_preserves_displaced_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="launchd", environ=values)
    current_release = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": "sha256:" + "b" * 64,
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": "sha256:" + "d" * 64,
        "previous_definition_digest": "sha256:" + "e" * 64,
        "previous_pointer_digest": "sha256:" + "f" * 64,
    }
    previous_release = {
        "ok": True,
        "status": "previous",
        "artifact_id": current_release["previous_artifact_id"],
        "definition_digest": current_release["previous_definition_digest"],
    }
    plan["launchable"] = True
    plan["release_status"] = dict(current_release)
    service_ids = tuple(plan["handoff_topology"]["stop_services"])
    plan["handoff_topology"]["generations"] = {
        "current": environment_services._service_generation_topology(
            environment,
            definition_digest=current_release["definition_digest"],
        ),
        "previous": environment_services._service_generation_topology(
            environment,
            definition_digest=current_release["previous_definition_digest"],
        ),
    }
    previous_running = set(service_ids)
    current_running: set[str] = set()
    previous_identities = {
        service_id: {
            "pid": 510_000 + index,
            "process_start_identity": f"previous:{service_id}",
        }
        for index, service_id in enumerate(service_ids)
    }
    current_identities = {
        service_id: {
            "pid": 520_000 + index,
            "process_start_identity": f"current:{service_id}",
        }
        for index, service_id in enumerate(service_ids)
    }
    first_inventory = {
        "target_artifact_id": current_release["artifact_id"],
        "target_definition_digest": current_release["definition_digest"],
        "target_pointer_digest": current_release["active_pointer_digest"],
        "previous_artifact_id": current_release["previous_artifact_id"],
        "previous_definition_digest": current_release[
            "previous_definition_digest"
        ],
        "previous_pointer_digest": current_release["previous_pointer_digest"],
        "plan_digest": plan["plan_digest"],
        "services": {
            service_id: {
                "generation": "previous",
                "state": "running",
                "identity": previous_identities[service_id],
            }
            for service_id in service_ids
        },
        "worker": {
            "generation": "previous",
            "state": "running",
            "identity": previous_identities[environment.runtime.worker_service],
        },
    }
    record_path = environment_services._predecessor_stop_path(environment)
    original_inventory = environment_services._restart_process_inventory
    artifact_paths = {
        str(row["path"]): str(row["service_id"])
        for row in plan["artifacts"]
        if row["kind"].endswith("_service")
    }
    attempt = 1

    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(current_release),
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (dict(previous_release), environment),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: (
            {"ok": True, "status": "healthy"}
            if current_running == set(service_ids)
            else {"ok": False, "status": "invalid", "message": "partial handoff"}
        ),
    )

    def inventory(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if not record_path.exists():
            return copy.deepcopy(first_inventory)
        return original_inventory(*args, **kwargs)

    monkeypatch.setattr(environment_services, "_restart_process_inventory", inventory)
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda _environment, *, service_id, **_kwargs: {
            "ok": True,
            "status": (
                "running"
                if service_id in current_running or service_id in previous_running
                else "stopped"
            ),
            "pid": (
                current_identities[service_id]["pid"]
                if service_id in current_running
                else previous_identities[service_id]["pid"]
                if service_id in previous_running
                else None
            ),
        },
    )

    def service_authority(
        _environment: Any,
        *,
        service_id: str,
        release_status: Mapping[str, Any],
        **_kwargs: Any,
    ) -> dict[str, Any]:
        if (
            release_status.get("artifact_id") == current_release["artifact_id"]
            and service_id in current_running
        ):
            identity = current_identities[service_id]
        elif (
            release_status.get("artifact_id") == previous_release["artifact_id"]
            and service_id in previous_running
        ):
            identity = previous_identities[service_id]
        else:
            return {"ok": False, "status": "missing"}
        return {
            "ok": True,
            "status": "healthy",
            "authority": dict(identity),
            "supervisor": {"ok": True, "status": "running", "pid": identity["pid"]},
        }

    monkeypatch.setattr(
        environment_services,
        "environment_service_authority_status",
        service_authority,
    )

    def worker_authority(
        _environment: Any,
        *,
        expected_pid: int | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        worker_id = environment.runtime.worker_service
        if worker_id not in current_running or expected_pid != current_identities[
            worker_id
        ]["pid"]:
            return {"ok": False, "status": "missing"}
        return {
            "ok": True,
            "status": "healthy",
            "authority": dict(current_identities[worker_id]),
        }

    monkeypatch.setattr(
        environment_services,
        "environment_worker_authority_status",
        worker_authority,
    )
    all_identities = {**previous_identities, **current_identities}
    identity_by_pid = {row["pid"]: row for row in all_identities.values()}
    monkeypatch.setattr(
        environment_services,
        "pid_alive",
        lambda pid: any(
            identity_by_pid[pid] == identities[service_id]
            for identities, running in (
                (previous_identities, previous_running),
                (current_identities, current_running),
            )
            for service_id in running
            if pid in identity_by_pid
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "process_start_identity",
        lambda pid: identity_by_pid.get(pid, {}).get("process_start_identity"),
    )
    monkeypatch.setattr(
        environment_services,
        "_wait_for_service_authority",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    monkeypatch.setattr(
        environment_services,
        "_require_clean_manager_stop_outcome",
        lambda **_kwargs: None,
    )

    def run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        nonlocal attempt
        values = list(argv)
        if "bootout" in values:
            service_id = values[-1].rsplit("/", 1)[-1]
            previous_running.discard(service_id)
            current_running.discard(service_id)
        elif "bootstrap" in values:
            service_id = artifact_paths[values[-1]]
            if attempt == 1 and service_id == environment.ui.service:
                return subprocess.CompletedProcess(values, 1, "", "partial start")
            current_running.add(service_id)
        return subprocess.CompletedProcess(values, 0, "", "")

    first = execute_environment_service_action(
        plan,
        action="restart",
        environment=environment,
        runner=run,
    )
    assert any(row["returncode"] == 1 for row in first)
    assert current_running == {environment.runtime.worker_service}
    assert json.loads(record_path.read_text(encoding="utf-8"))[
        "lifecycle_state"
    ] == "start_attempted"

    attempt = 2
    second = execute_environment_service_action(
        plan,
        action="restart",
        environment=environment,
        runner=run,
    )
    assert all(row["returncode"] == 0 for row in second)
    assert current_running == set(service_ids)
    completed = json.loads(record_path.read_text(encoding="utf-8"))
    assert completed["lifecycle_state"] == "start_attempted"
    assert {row["pid"] for row in completed["predecessor_processes"]} == {
        row["pid"] for row in previous_identities.values()
    }

    later_release = {
        **current_release,
        "artifact_id": "sha256:" + "1" * 64,
        "definition_digest": "sha256:" + "2" * 64,
        "active_pointer_digest": "sha256:" + "3" * 64,
        "previous_artifact_id": current_release["artifact_id"],
        "previous_definition_digest": current_release["definition_digest"],
        "previous_pointer_digest": current_release["active_pointer_digest"],
    }
    later_plan = copy.deepcopy(plan)
    later_plan["plan_digest"] = "sha256:" + "4" * 64
    later_plan["handoff_topology"]["generations"] = {
        "current": environment_services._service_generation_topology(
            environment,
            definition_digest=later_release["definition_digest"],
        ),
        "previous": environment_services._service_generation_topology(
            environment,
            definition_digest=later_release["previous_definition_digest"],
        ),
    }
    monkeypatch.setattr(
        environment_services,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (
            {
                "ok": True,
                "status": "previous",
                "artifact_id": later_release["previous_artifact_id"],
                "definition_digest": later_release[
                    "previous_definition_digest"
                ],
            },
            environment,
        ),
    )
    with pytest.raises(DeploymentConfigError, match="consumed by a later start"):
        original_inventory(
            environment,
            plan=later_plan,
            current_release=later_release,
        )


def test_supervision_runner_launch_error_is_structured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )

    def unavailable(_argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("missing supervisor command")

    results = execute_environment_service_action(
        plan,
        action="logs",
        environment=environment,
        runner=unavailable,
    )

    assert results == [
        {
            "argv": results[0]["argv"],
            "display": results[0]["display"],
            "returncode": 127,
            "stdout": "",
            "stderr": "FileNotFoundError: missing supervisor command",
        }
    ]


def test_supervision_activation_lock_blocks_exclusive_cutover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    artifact_id = "sha256:" + "d" * 64
    plan["launchable"] = True
    plan["release_status"] = {
        "ok": True,
        "status": "active",
        "artifact_id": artifact_id,
    }
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": artifact_id,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    install_environment_services(environment, plan)
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "stopped"},
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: {"previous_artifact_id": None},
    )
    monkeypatch.setattr(
        environment_services,
        "_attest_restart_inventory_stopped",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_record_predecessor_stop",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_invalidate_predecessor_stop_before_start",
        lambda *_args, **_kwargs: None,
    )
    start_waiting = threading.Event()
    allow_start_proof = threading.Event()

    def wait_for_authority(*_args, **_kwargs) -> dict[str, bool]:
        start_waiting.set()
        assert allow_start_proof.wait(timeout=5)
        return {"ok": True}

    monkeypatch.setattr(
        environment_services,
        "_wait_for_service_authority",
        wait_for_authority,
    )
    failures: list[Exception] = []

    def start_services() -> None:
        try:
            execute_environment_service_action(
                plan,
                action="restart",
                environment=environment,
                runner=lambda argv: subprocess.CompletedProcess(list(argv), 0, "", ""),
            )
        except Exception as exc:
            failures.append(exc)

    start_thread = threading.Thread(target=start_services)
    start_thread.start()
    assert start_waiting.wait(timeout=5)

    activation_attempted = threading.Event()
    activation_completed = threading.Event()
    original_lock = release_activation.release_activation_lock

    @contextmanager
    def observed_activation_lock(active_pointer: Path, *, exclusive: bool = True):
        if exclusive:
            activation_attempted.set()
        with original_lock(active_pointer, exclusive=exclusive):
            yield

    monkeypatch.setattr(
        release_activation,
        "release_activation_lock",
        observed_activation_lock,
    )
    fake_release = SimpleNamespace(
        artifact_root=environment.release.artifact_store / "candidate",
        repo_root=tmp_path / "candidate-repo",
        inputs=SimpleNamespace(),
        artifact_id="sha256:" + "e" * 64,
    )
    monkeypatch.setattr(
        release_activation,
        "verify_sealed_release",
        lambda *_args, **_kwargs: fake_release,
    )
    monkeypatch.setattr(
        release_activation,
        "_candidate_environment",
        lambda **_kwargs: (environment, Path("environment.yaml")),
    )
    monkeypatch.setattr(
        release_activation,
        "_validate_candidate_binding",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        release_activation,
        "_active_payload",
        lambda **_kwargs: {
            "schema": "test-active",
            "definition_digest": "sha256:" + "a" * 64,
        },
    )
    monkeypatch.setattr(
        release_activation,
        "_launcher_payload",
        lambda *_args, **_kwargs: {"schema": "test-launcher"},
    )
    monkeypatch.setattr(
        release_activation,
        "load_active_release",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        release_activation,
        "_atomic_write_json",
        lambda *_args, **_kwargs: None,
    )

    def activate() -> None:
        try:
            release_activation.activate_environment_release(
                environment=environment,
                release=fake_release,
                source_repo=tmp_path,
                activated_by="test",
                environ=values,
            )
            activation_completed.set()
        except Exception as exc:
            failures.append(exc)

    activation_thread = threading.Thread(target=activate)
    activation_thread.start()
    try:
        assert activation_attempted.wait(timeout=5)
        assert not activation_completed.wait(timeout=0.2)
    finally:
        allow_start_proof.set()
        start_thread.join(timeout=5)
        activation_thread.join(timeout=5)
    assert not start_thread.is_alive()
    assert not activation_thread.is_alive()
    assert activation_completed.is_set()
    assert failures == []


def test_service_lifecycle_transaction_serializes_cross_thread_cutover(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def first() -> None:
        with environment_services.environment_service_lifecycle_transaction(
            environment
        ):
            first_entered.set()
            assert release_first.wait(timeout=5)

    def second() -> None:
        assert first_entered.wait(timeout=5)
        with environment_services.environment_service_lifecycle_transaction(
            environment
        ):
            second_entered.set()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    try:
        assert first_entered.wait(timeout=5)
        assert not second_entered.wait(timeout=0.2)
    finally:
        release_first.set()
        first_thread.join(timeout=5)
        second_thread.join(timeout=5)
    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert second_entered.is_set()


def test_supervision_restart_plan_cannot_mutate_production_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    staging = _load(root, "fixture-staging", values)
    _stub_receiptless_managers_stopped(monkeypatch)
    production = _load(root, "fixture-production", values)
    production.release.active_pointer.parent.mkdir(parents=True)
    production.release.active_pointer.write_text(
        '{"production":"unchanged"}\n',
        encoding="utf-8",
    )
    production_before = production.release.active_pointer.read_bytes()
    plan = render_environment_services(
        staging,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    artifact_id = "sha256:" + "f" * 64
    plan["launchable"] = True
    plan["release_status"] = {
        "ok": True,
        "status": "active",
        "artifact_id": artifact_id,
    }
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": artifact_id,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    install_environment_services(staging, plan)
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "stopped"},
    )
    monkeypatch.setattr(
        environment_services,
        "_wait_for_service_authority",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    observed: list[list[str]] = []

    def runner(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        observed.append(list(argv))
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    results = execute_environment_service_action(
        plan,
        action="restart",
        runner=runner,
        environment=staging,
    )

    assert results and all(row["returncode"] == 0 for row in results)
    rendered = repr(observed)
    assert staging.runtime.worker_service in rendered
    assert staging.ui.service in rendered
    assert "up', '--detach', 'postgres" in rendered
    for foreign in (
        production.runtime.worker_service,
        production.ui.service,
        production.database.container,
        str(production.runtime.state_root),
    ):
        assert foreign not in rendered
    assert production.release.active_pointer.read_bytes() == production_before


def test_supervision_start_holds_release_identity_through_process_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    staging = _load(root, "fixture-staging", values)
    _stub_receiptless_managers_stopped(monkeypatch)
    plan = render_environment_services(
        staging,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    artifact_id = "sha256:" + "d" * 64
    plan["launchable"] = True
    plan["release_status"] = {
        "ok": True,
        "status": "active",
        "artifact_id": artifact_id,
        "active_pointer": str(staging.release.active_pointer),
    }
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": artifact_id,
            "active_pointer": str(staging.release.active_pointer),
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    install_environment_services(staging, plan)
    monkeypatch.setattr(
        environment_services,
        "environment_service_installation_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "current"},
    )
    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "stopped"},
    )
    observed: list[list[str]] = []

    def runner(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        observed.append(list(argv))
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": "sha256:" + "e" * 64,
        },
    )
    with pytest.raises(DeploymentConfigError, match="active release changed"):
        execute_environment_service_action(
            plan,
            action="restart",
            runner=runner,
            environment=staging,
        )
    assert observed == []

    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": artifact_id,
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_wait_for_service_authority",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    results = execute_environment_service_action(
        plan,
        action="restart",
        runner=runner,
        environment=staging,
    )
    assert observed
    assert results[-1]["process_authority"]["ok"] is True
    assert results[-1]["process_authority"]["status"] == "healthy"


def test_supervision_launchd_uses_environment_file_and_stable_launcher(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-production", values)

    plan = render_environment_services(
        environment,
        adapter="launchd",
        environ=values,
    )

    worker = _artifact(plan, "worker_service")
    assert str(environment.runtime.local_env_file) in worker["content"]
    assert str(plan["launcher"]["path"]) in worker["content"]
    assert "environment_exec.py" in worker["content"]
    assert "/bin/sh" not in worker["content"]
    assert "production-secret" not in worker["content"]
    assert environment.runtime.worker_service in worker["content"]


def test_supervision_tracking_dogfood_adapter_remains_a_separate_surface(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "dogfood",
            "service",
            "render",
            "--deployment",
            "okg-workspace",
            "--adapter",
            "systemd",
            "--repo-root",
            str(tmp_path / "checkout"),
            "--okg-bin",
            str(tmp_path / "checkout/.venv/bin/okg"),
            "--env-file",
            str(tmp_path / "dogfood.env"),
            "--user",
            "okg",
            "--group",
            "okg",
            "--json",
        ]
    )
    assert rc == 0
    dogfood = json.loads(capsys.readouterr().out)
    assert "runtime worker --deployment %i" in dogfood["content"]
    assert "release_launcher.py" not in dogfood["content"]

    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    sealed = render_environment_services(environment, adapter="systemd", environ=values)
    worker = _artifact(sealed, "worker_service")
    assert "runtime worker" not in worker["content"]
    assert "release_launcher.py" in worker["content"]


def test_supervision_launchd_does_not_evaluate_secret_shell_syntax(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    marker = tmp_path / "must-not-run"
    password = f"$(touch {marker})"
    values["TEST_PRODUCTION_DSN"] = (
        f"postgresql://postgres:{quote(password, safe='')}@"
        "127.0.0.1:55434/fixture_production"
    )
    environment = _load(root, "fixture-production", values)
    plan = render_environment_services(
        environment,
        adapter="launchd",
        environ=values,
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": {
                "adapter": "launchd",
                "load_state": "not-found",
                "exit_status_observed": False,
            },
        },
    )
    installed = install_environment_services(environment, plan)

    worker = _artifact(plan, "worker_service")
    assert "$(touch" not in worker["content"]
    assert "/bin/sh" not in worker["content"]
    loaded = load_environment_file(environment.runtime.local_env_file)
    assert loaded["TEST_PRODUCTION_DSN"] == values["TEST_PRODUCTION_DSN"]
    assert loaded["TEST_STATE_ROOT"] == values["TEST_STATE_ROOT"]
    password_file = environment.runtime.state_root / "supervisor/postgres-password"
    assert password_file.read_text(encoding="utf-8") == f"{password}\n"
    assert password_file in installed.paths
    assert not marker.exists()


def test_supervision_environment_composes_owned_webhook_endpoint(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    staging = _load(root, "fixture-staging", values)
    production = _load(root, "fixture-production", values)

    assert staging.manifest.runtime.source_sync["webhook"] == {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 8089,
    }
    assert production.manifest.runtime.source_sync["webhook"] == {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 8088,
    }


def test_supervision_worker_authority_status_validates_live_process_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_root = tmp_path / "artifact"
    executable = artifact_root / "venv/bin/python3"
    package = artifact_root / "venv/lib/site-packages/okg/__init__.py"
    definition_root = artifact_root / "repo/definition"
    for path in (executable, package, definition_root / "definition.yaml"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n", encoding="utf-8")
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "artifact_root": str(artifact_root),
        "definition_digest": environment.definition_digest,
    }
    monkeypatch.setattr(
        release_authority,
        "environment_release_status",
        lambda *_args, **_kwargs: release_status,
    )
    authority = {
        "environment_id": environment.id,
        "tier": environment.tier,
        "deployment": environment.graph_deployment_name,
        "database": environment.database.name,
        "database_identity": environment.database.dbos_identity,
        "artifact_id": release_status["artifact_id"],
        "artifact_root": str(artifact_root),
        "definition_digest": environment.definition_digest,
        "active_pointer": str(environment.release.active_pointer),
        "definition_root": str(definition_root),
        "executable": str(executable),
        "package_file": str(package),
        "isolated": True,
        "arguments": ["-I", "-m", "okg.substrate.runtime.worker"],
        "pycache_prefix": _launch_prefix(environment, "worker"),
        "pid": os.getpid(),
        "process_start_identity": release_authority.process_start_identity(os.getpid()),
        "authority_path": str(environment.runtime.state_root / "worker-authority.json"),
    }
    authority_path = release_authority.write_worker_release_authority(authority)
    monkeypatch.setattr(
        release_authority,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str(executable.resolve()),
            "argv": [
                str(executable),
                *_launched(
                    ["-I", "-m", "okg.substrate.runtime.worker"],
                    _launch_prefix(environment, "worker"),
                ),
            ],
        },
    )

    status = release_authority.environment_worker_authority_status(
        environment,
        environ=values,
    )

    assert status["ok"] is True, json.dumps(status, sort_keys=True)
    assert status["authority"]["pid"] == os.getpid()
    assert stat.S_IMODE(authority_path.stat().st_mode) == 0o600

    monkeypatch.setattr(
        release_authority,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str((tmp_path / "foreign/python3").resolve()),
            "argv": [
                str(tmp_path / "foreign/python3"),
                *_launched(
                    ["-I", "-m", "okg.substrate.runtime.worker"],
                    _launch_prefix(environment, "worker"),
                ),
            ],
        },
    )
    status = release_authority.environment_worker_authority_status(
        environment,
        environ=values,
    )
    assert status["ok"] is False
    assert "executable differs" in status["message"]

    raw = json.loads(authority_path.read_text(encoding="utf-8"))
    raw["package_file"] = str(tmp_path / "foreign/okg/__init__.py")
    authority_path.write_text(json.dumps(raw), encoding="utf-8")
    status = release_authority.environment_worker_authority_status(
        environment,
        environ=values,
    )
    assert status["ok"] is False
    assert "outside the sealed release" in status["message"]


def test_previous_definition_receipts_can_prove_exact_process_death(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_root = tmp_path / "previous-artifact"
    executable = artifact_root / "venv/bin/python3"
    package = artifact_root / "venv/lib/site-packages/okg/__init__.py"
    definition_root = artifact_root / "repo/definition"
    repo_root = artifact_root / "repo"
    for path in (executable, package, definition_root / "definition.yaml"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n", encoding="utf-8")
    previous_definition = "sha256:" + "b" * 64
    assert previous_definition != environment.definition_digest
    release_status = {
        "ok": True,
        "status": "previous",
        "artifact_id": "sha256:" + "a" * 64,
        "artifact_root": str(artifact_root),
        "definition_digest": previous_definition,
    }
    dead_pid = 2_147_400_000
    release_authority.write_worker_release_authority(
        {
            "environment_id": environment.id,
            "tier": environment.tier,
            "deployment": environment.graph_deployment_name,
            "database": environment.database.name,
            "database_identity": environment.database.dbos_identity,
            "artifact_id": release_status["artifact_id"],
            "artifact_root": str(artifact_root),
            "definition_digest": previous_definition,
            "definition_root": str(definition_root),
            "executable": str(executable),
            "package_file": str(package),
            "isolated": True,
            "arguments": [
                "-I",
                "-m",
                "okg.substrate.runtime.worker",
                "--deployment",
                environment.graph_deployment_name,
            ],
            "pid": dead_pid,
            "process_start_identity": "departed",
            "active_pointer": str(environment.release.active_pointer),
            "authority_path": str(
                environment.runtime.state_root / "worker-authority.json"
            ),
        }
    )
    service_id = environment.ui.service
    service_receipt = (
        environment.runtime.state_root / "service-authority" / f"{service_id}.json"
    )
    service_receipt.parent.mkdir(parents=True)
    service_receipt.write_text(
        json.dumps(
            {
                "schema": "okg.service_release_authority/v1",
                "service_id": service_id,
                "environment_id": environment.id,
                "artifact_id": release_status["artifact_id"],
                "artifact_root": str(artifact_root),
                "definition_digest": previous_definition,
                "active_pointer": str(environment.release.active_pointer),
                "interpreter": str(executable),
                "repo_root": str(repo_root),
                "isolated": True,
                "arguments": [
                    "-I",
                    "-m",
                    "okg.viz",
                    "--deployment",
                    environment.graph_deployment_name,
                    "--host",
                    environment.ui.host,
                    "--port",
                    str(environment.ui.port),
                ],
                "pid": dead_pid,
                "process_start_identity": "departed",
            }
        ),
        encoding="utf-8",
    )
    service_receipt.chmod(0o600)
    monkeypatch.setattr(release_authority, "pid_alive", lambda _pid: False)
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)

    worker = release_authority.environment_worker_authority_status(
        environment,
        release_status=release_status,
    )
    service = environment_service_authority_status(
        environment,
        service_id=service_id,
        release_status=release_status,
    )
    aggregate = {
        "schema": "okg.environment_process_authority_status/v1",
        "environment_id": environment.id,
        "declared_services": [service_id],
        "ok": False,
        "status": "invalid",
        "worker": worker,
        "services": {service_id: service},
    }

    assert worker["reason_code"] == "worker_process_not_alive"
    assert service["reason_code"] == "service_process_not_alive"
    assert environment_services._no_process_is_alive(
        aggregate,
        environment_id=environment.id,
        expected_services=(service_id,),
    )


def test_origin_dev_definitionless_live_service_receipt_is_predecessor_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    previous_artifact = "sha256:" + "a" * 64
    previous_definition = "sha256:" + "b" * 64
    current_artifact = "sha256:" + "c" * 64
    previous_root = environment.release.artifact_store / ("a" * 64)
    executable = previous_root / "venv/bin/python3"
    repo_root = previous_root / "repo"
    executable.parent.mkdir(parents=True, exist_ok=True)
    repo_root.mkdir(parents=True, exist_ok=True)
    executable.write_text("fixture\n", encoding="utf-8")
    previous_pointer = {
        "schema": "okg.active_release/v1",
        "artifact_id": previous_artifact,
        "artifact_root": str(previous_root),
        "definition_digest": previous_definition,
    }
    active_pointer = {
        "schema": "okg.active_release/v1",
        "artifact_id": current_artifact,
        "artifact_root": str(environment.release.artifact_store / ("c" * 64)),
        "definition_digest": environment.definition_digest,
        "predecessor": {
            "schema": "okg.release_predecessor/v1",
            "artifact_id": previous_artifact,
            "definition_digest": previous_definition,
            "pointer_digest": release_activation.active_release_pointer_digest(
                previous_pointer
            ),
        },
    }
    for path, payload in (
        (environment.release.previous_pointer, previous_pointer),
        (environment.release.active_pointer, active_pointer),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        path.chmod(0o600)
    current_release = {
        "previous_artifact_id": previous_artifact,
        "previous_artifact_root": str(previous_root),
        "previous_definition_digest": previous_definition,
        "previous_pointer_digest": release_activation.active_release_pointer_digest(
            previous_pointer
        ),
        "active_pointer_digest": release_activation.active_release_pointer_digest(
            active_pointer
        ),
    }
    previous_release = environment_services._previous_release_authority_status(
        environment,
        current_release=current_release,
    )
    service_id = environment.ui.service
    arguments = list(
        environment_services._expected_service_arguments(environment, service_id)
    )
    receipt = (
        environment.runtime.state_root / "service-authority" / f"{service_id}.json"
    )
    receipt.parent.mkdir(parents=True, exist_ok=True)
    origin_dev_payload = {
        "schema": "okg.service_release_authority/v1",
        "service_id": service_id,
        "environment_id": environment.id,
        "artifact_id": previous_artifact,
        "artifact_root": str(previous_root),
        "active_pointer": str(environment.release.active_pointer),
        "interpreter": str(executable),
        "repo_root": str(repo_root),
        "isolated": True,
        "arguments": arguments,
        "pycache_prefix": _launch_prefix(environment, "predecessor"),
        "pid": os.getpid(),
        "process_start_identity": "live-predecessor",
    }
    receipt.write_text(json.dumps(origin_dev_payload), encoding="utf-8")
    receipt.chmod(0o600)
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: True)
    monkeypatch.setattr(
        environment_services,
        "process_start_identity",
        lambda _pid: "live-predecessor",
    )
    monkeypatch.setattr(
        environment_services,
        "process_runtime_identity",
        lambda _pid: {
            "executable": str(executable),
            "argv": [
                str(executable),
                *_launched(arguments, _launch_prefix(environment, "predecessor")),
            ],
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid(),
            "manager_outcome": {"adapter": "systemd"},
        },
    )

    accepted = environment_service_authority_status(
        environment,
        service_id=service_id,
        release_status=previous_release,
    )
    assert accepted["ok"] is True, accepted

    current_status = {
        **previous_release,
        "status": "active",
    }
    refused_current = environment_service_authority_status(
        environment,
        service_id=service_id,
        release_status=current_status,
    )
    assert refused_current["ok"] is False
    assert "definition_digest" in refused_current["message"]

    tampered = copy.deepcopy(active_pointer)
    tampered["predecessor"]["definition_digest"] = "sha256:" + "d" * 64
    environment.release.active_pointer.write_text(
        json.dumps(tampered),
        encoding="utf-8",
    )
    refused_ancestry = environment_service_authority_status(
        environment,
        service_id=service_id,
        release_status=previous_release,
    )
    assert refused_ancestry["ok"] is False
    assert "definition_digest" in refused_ancestry["message"]

    environment.release.active_pointer.write_text(
        json.dumps(active_pointer),
        encoding="utf-8",
    )
    explicit_wrong = {**origin_dev_payload, "definition_digest": "sha256:" + "e" * 64}
    receipt.write_text(json.dumps(explicit_wrong), encoding="utf-8")
    refused_explicit = environment_service_authority_status(
        environment,
        service_id=service_id,
        release_status=previous_release,
    )
    assert refused_explicit["ok"] is False
    assert "definition_digest" in refused_explicit["message"]


@pytest.mark.parametrize(
    "workflow",
    ("staging_clone", "production_rollback"),
)
def test_fenced_activation_workflows_rebind_only_fresh_exact_stopped_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    workflow: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_a = "sha256:" + "a" * 64
    definition_a = "sha256:" + "b" * 64
    pointer_a = "sha256:" + "c" * 64
    artifact_b = "sha256:" + "d" * 64
    definition_b = "sha256:" + "e" * 64
    pointer_b = "sha256:" + "f" * 64
    old_plan_digest = "sha256:" + "1" * 64
    new_plan_digest = "sha256:" + "2" * 64
    service_ids = environment_services._supervised_service_ids(environment)
    old_services = {
        service_id: {
            "generation": "current",
            "state": "stopped",
            "identity": {
                "pid": 430_000 + index,
                "process_start_identity": f"stopped-a:{index}",
            },
        }
        for index, service_id in enumerate(service_ids)
    }
    old_inventory = {
        "target_artifact_id": artifact_a,
        "target_definition_digest": definition_a,
        "target_pointer_digest": pointer_a,
        "previous_artifact_id": (
            artifact_b if workflow == "production_rollback" else None
        ),
        "previous_definition_digest": (
            definition_b if workflow == "production_rollback" else None
        ),
        "previous_pointer_digest": (
            "sha256:" + "3" * 64
            if workflow == "production_rollback"
            else None
        ),
        "plan_digest": old_plan_digest,
        "services": old_services,
        "worker": {
            **old_services[environment.runtime.worker_service],
        },
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    environment_services._record_predecessor_stop(
        environment,
        inventory=old_inventory,
    )
    marker = environment.runtime.state_root / "supervisor/install.json"
    marker.write_text(
        json.dumps(
            {
                "schema": "okg.environment_service_install/v1",
                "environment_id": environment.id,
                "artifact_id": artifact_a,
                "plan_digest": old_plan_digest,
            }
        ),
        encoding="utf-8",
    )
    marker.chmod(0o600)
    if workflow == "staging_clone":
        authority_paths = [
            environment.runtime.state_root / "worker-authority.json",
            *[
                environment.runtime.state_root
                / "service-authority"
                / f"{service_id}.json"
                for service_id in service_ids
            ],
        ]
        for path in authority_paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
        removed = environment_clone._clear_staging_runtime_authority(environment)
        assert set(removed).issuperset(str(path) for path in authority_paths)
        assert environment_services._predecessor_stop_path(environment).exists()
    new_release = {
        "ok": True,
        "status": "active",
        "artifact_id": artifact_b,
        "definition_digest": definition_b,
        "active_pointer_digest": pointer_b,
        "previous_artifact_id": artifact_a,
        "previous_definition_digest": definition_a,
        "previous_pointer_digest": pointer_a,
    }
    generation_current = environment_services._service_generation_topology(
        environment,
        definition_digest=definition_b,
    )
    generation_previous = environment_services._service_generation_topology(
        environment,
        definition_digest=definition_a,
    )
    plan = {
        "adapter": "launchd",
        "systemd_scope": "system",
        "plan_digest": new_plan_digest,
        "handoff_topology": {
            "stop_services": list(service_ids),
            "manager": {"adapter": "launchd", "systemd_scope": "system"},
            "generations": {
                "current": generation_current,
                "previous": generation_previous,
            },
        },
    }
    monkeypatch.setattr(
        environment_services,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (
            {
                "ok": True,
                "status": "previous",
                "artifact_id": artifact_a,
                "definition_digest": definition_a,
            },
            environment,
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "stopped", "pid": None},
    )

    rebound = environment_services._restart_process_inventory(
        environment,
        plan=plan,
        current_release=new_release,
    )

    assert rebound["recovered_from_stop_record"] is True
    assert rebound["target_artifact_id"] == artifact_b
    assert rebound["previous_artifact_id"] == artifact_a
    assert {
        row["generation"] for row in rebound["services"].values()
    } == {"previous"}
    rebound_record = json.loads(
        environment_services._predecessor_stop_path(environment).read_text(
            encoding="utf-8"
        )
    )
    assert rebound_record["lifecycle_state"] == "stopped"
    assert rebound_record["target_artifact_id"] == artifact_b
    assert rebound_record["previous_artifact_id"] == artifact_a

    environment_services._invalidate_predecessor_stop_before_start(
        environment,
        plan=plan,
        release_status=new_release,
    )
    consumed = json.loads(
        environment_services._predecessor_stop_path(environment).read_text(
            encoding="utf-8"
        )
    )
    assert consumed["lifecycle_state"] == "start_attempted"
    monkeypatch.setattr(
        environment_services,
        "_environment_release_status_locked",
        lambda *_args, **_kwargs: dict(new_release),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_process_authority_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    monkeypatch.setattr(
        environment_services,
        "installed_environment_service_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "current",
            "plan_digest": new_plan_digest,
            "plan": plan,
        },
    )
    completed_handoff = environment_release_handoff_status(environment)
    assert completed_handoff["ok"] is True, completed_handoff
    assert completed_handoff["predecessor_stop"]["ok"] is True

    artifact_c = "sha256:" + "4" * 64
    definition_c = "sha256:" + "5" * 64
    pointer_c = "sha256:" + "6" * 64
    later_release = {
        **new_release,
        "artifact_id": artifact_c,
        "definition_digest": definition_c,
        "active_pointer_digest": pointer_c,
        "previous_artifact_id": artifact_b,
        "previous_definition_digest": definition_b,
        "previous_pointer_digest": pointer_b,
    }
    later_plan = copy.deepcopy(plan)
    later_plan["plan_digest"] = "sha256:" + "7" * 64
    later_plan["handoff_topology"]["generations"] = {
        "current": environment_services._service_generation_topology(
            environment,
            definition_digest=definition_c,
        ),
        "previous": generation_current,
    }
    monkeypatch.setattr(
        environment_services,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (
            {
                "ok": True,
                "status": "previous",
                "artifact_id": artifact_b,
                "definition_digest": definition_b,
            },
            environment,
        ),
    )
    with pytest.raises(
        DeploymentConfigError,
        match="consumed by a later start attempt",
    ):
        environment_services._restart_process_inventory(
            environment,
            plan=later_plan,
            current_release=later_release,
        )


def test_handoff_completion_rechecks_recorded_predecessor_incarnation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    previous_pid = 424242
    previous_start = "boot:previous"
    release_status = {
        "ok": True,
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": "sha256:" + "b" * 64,
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": "sha256:" + "d" * 64,
        "previous_definition_digest": "sha256:" + "e" * 64,
        "previous_pointer_digest": "sha256:" + "f" * 64,
    }
    services = {
        service_id: {
            "generation": "previous",
            "state": "stopped",
            "identity": {
                "pid": previous_pid + index,
                "process_start_identity": previous_start + str(index),
            },
        }
        for index, service_id in enumerate(
            environment_services._supervised_service_ids(environment)
        )
    }
    plan_digest = "sha256:" + "1" * 64
    inventory = {
        "target_artifact_id": release_status["artifact_id"],
        "target_definition_digest": release_status["definition_digest"],
        "target_pointer_digest": release_status["active_pointer_digest"],
        "previous_artifact_id": release_status["previous_artifact_id"],
        "previous_definition_digest": release_status[
            "previous_definition_digest"
        ],
        "previous_pointer_digest": release_status["previous_pointer_digest"],
        "plan_digest": plan_digest,
        "services": services,
        "worker": {
            "generation": "previous",
            "state": "stopped",
            "identity": services[environment.runtime.worker_service]["identity"],
        },
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    environment_services._record_predecessor_stop(
        environment,
        inventory=inventory,
    )
    assert environment_services._predecessor_stop_status(
        environment,
        release_status=release_status,
        expected_services=tuple(services),
        expected_plan_digest=plan_digest,
    )["ok"] is True

    second_inventory = copy.deepcopy(inventory)
    for index, row in enumerate(second_inventory["services"].values()):
        row["identity"] = {
            "pid": previous_pid + 100 + index,
            "process_start_identity": f"boot:second:{index}",
        }
    second_inventory["worker"]["identity"] = second_inventory["services"][
        environment.runtime.worker_service
    ]["identity"]
    environment_services._record_predecessor_stop(
        environment,
        inventory=second_inventory,
    )
    advanced = json.loads(
        environment_services._predecessor_stop_path(environment).read_text(
            encoding="utf-8"
        )
    )
    assert {row["pid"] for row in advanced["stopped_processes"]} == {
        previous_pid + 100 + index for index in range(len(services))
    }
    assert previous_pid not in {
        row["pid"] for row in advanced["stopped_processes"]
    }
    record_path = environment_services._predecessor_stop_path(environment)
    before_retry = record_path.read_bytes()
    recovered_inventory = copy.deepcopy(second_inventory)
    recovered_inventory["recovered_from_stop_record"] = True
    recovered_inventory["predecessor_processes"] = advanced[
        "predecessor_processes"
    ]
    recovered_inventory["stopped_processes"] = advanced["stopped_processes"]
    environment_services._record_predecessor_stop(
        environment,
        inventory=recovered_inventory,
    )
    assert record_path.read_bytes() == before_retry

    monkeypatch.setattr(
        environment_services,
        "pid_alive",
        lambda pid: pid == previous_pid + 100,
    )
    monkeypatch.setattr(
        environment_services,
        "process_start_identity",
        lambda pid: "boot:second:0" if pid == previous_pid + 100 else None,
    )
    status = environment_services._predecessor_stop_status(
        environment,
        release_status=release_status,
        expected_services=tuple(services),
        expected_plan_digest=plan_digest,
    )
    assert status["ok"] is False
    assert "still alive" in status["message"]


@pytest.mark.parametrize("corruption", ["role", "processes", "absent_running"])
def test_predecessor_stop_record_refuses_structural_or_relational_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    service_ids = environment_services._supervised_service_ids(environment)
    release_status = {
        "artifact_id": "sha256:" + "a" * 64,
        "definition_digest": "sha256:" + "b" * 64,
        "active_pointer_digest": "sha256:" + "c" * 64,
        "previous_artifact_id": "sha256:" + "d" * 64,
        "previous_definition_digest": "sha256:" + "e" * 64,
        "previous_pointer_digest": "sha256:" + "f" * 64,
    }
    services = {
        service_id: {
            "generation": "previous",
            "state": "running",
            "identity": {
                "pid": 500_000 + index,
                "process_start_identity": f"boot:{index}",
            },
        }
        for index, service_id in enumerate(service_ids)
    }
    inventory = {
        "target_artifact_id": release_status["artifact_id"],
        "target_definition_digest": release_status["definition_digest"],
        "target_pointer_digest": release_status["active_pointer_digest"],
        "previous_artifact_id": release_status["previous_artifact_id"],
        "previous_definition_digest": release_status[
            "previous_definition_digest"
        ],
        "previous_pointer_digest": release_status["previous_pointer_digest"],
        "plan_digest": "sha256:" + "1" * 64,
        "services": services,
        "worker": {
            "generation": "previous",
            "state": "running",
            "identity": services[environment.runtime.worker_service]["identity"],
        },
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    environment_services._record_predecessor_stop(
        environment,
        inventory=inventory,
    )
    path = environment_services._predecessor_stop_path(environment)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if corruption == "role":
        raw["observed_roles"][service_ids[0]] = "stopped"
    elif corruption == "processes":
        raw["stopped_processes"] = []
    else:
        raw["observed_roles"][service_ids[0]] = {
            "generation": "absent",
            "state": "running",
        }
    path.write_text(json.dumps(raw), encoding="utf-8")
    path.chmod(0o600)

    status = environment_services._predecessor_stop_status(
        environment,
        release_status=release_status,
        expected_services=service_ids,
        expected_plan_digest=inventory["plan_digest"],
    )
    assert status["ok"] is False
    assert status["status"] == "invalid"


def test_supervision_process_runtime_identity_reads_live_interpreter_and_argv() -> None:
    # A long-lived child spawned under pytest-xdist MUST be isolated from the
    # worker's process group and control channel (#703): `start_new_session=True`
    # puts it in its own session/process group so no signal delivered to (or by)
    # the child can reach the xdist worker/controller group, and redirecting
    # stdin to DEVNULL detaches it from the worker's inherited fd 0, which under
    # xdist is the execnet channel to the controller. Without both, this
    # `http.server` child sits in the worker's group holding the control channel
    # while `process_runtime_identity` reads it, which is a nondeterministic
    # "worker received keyboard-interrupt" hazard on the parallel fast lane.
    process = subprocess.Popen(
        [sys.executable, "-I", "-m", "http.server", "0"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        identity = release_authority.process_runtime_identity(process.pid)
        assert identity is not None
        assert Path(identity["executable"]).resolve() == Path(sys.executable).resolve()
        assert identity["argv"][1:] == ["-I", "-m", "http.server", "0"]
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


_PS_PATH_PID = 4242


def _force_ps_fallback(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    # Route process_runtime_identity through its `ps` fallback deterministically.
    # On a real Linux host /proc/<pid> resolves for whatever live process holds
    # that pid, which would take the /proc branch and never consult `ps`. Forcing
    # the gate closed for this pid makes the test exercise the `ps` parsing it is
    # about regardless of what the host happens to be running.
    real_exists = Path.exists
    real_is_file = Path.is_file
    proc_prefix = f"/proc/{_PS_PATH_PID}/"

    def exists(self: Path, **kwargs: object) -> bool:
        if str(self).startswith(proc_prefix):
            return False
        return real_exists(self, **kwargs)  # type: ignore[arg-type]

    def is_file(self: Path, **kwargs: object) -> bool:
        if str(self).startswith(proc_prefix):
            return False
        return real_is_file(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "exists", exists)
    monkeypatch.setattr(Path, "is_file", is_file)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(
        release_authority.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=command + "\n"),
    )


@pytest.mark.parametrize(
    "command",
    [
        # A zombie: ps prints the process's status, not its argv.
        pytest.param("[python] <defunct>", id="zombie"),
        # A kernel thread, which never had a userspace command line.
        pytest.param("[kworker/0:1-events]", id="kernel_thread"),
        # A zombie whose comm carries a space still ends in the defunct marker.
        pytest.param("[Web Content] <defunct>", id="spaced_zombie"),
    ],
)
def test_supervision_process_runtime_identity_rejects_defunct_process_output(
    command: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A process with no live command line must read back as having no identity.
    # Parsing ps's status text into argv invents an executable for a process that
    # no longer exists, which makes the authority check report a release mismatch
    # instead of the truth: the supervised process died.
    _force_ps_fallback(monkeypatch, command)

    assert release_authority.process_runtime_identity(_PS_PATH_PID) is None


def test_supervision_process_runtime_identity_still_reads_a_live_ps_command_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The defunct check must not swallow a real command line.
    _force_ps_fallback(
        monkeypatch,
        f"{sys.executable} -I -m okg.substrate.runtime.worker",
    )

    identity = release_authority.process_runtime_identity(_PS_PATH_PID)

    assert identity is not None
    assert identity["argv"][1:] == ["-I", "-m", "okg.substrate.runtime.worker"]


def test_supervision_process_runtime_identity_keeps_a_defunct_argument_mid_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The marker `ps` appends is a TRAILING `<defunct>` token. A live command that
    # merely passes `<defunct>` as an interior argument is a real identity and
    # must survive; rejecting any `<defunct>` token anywhere would drop it.
    _force_ps_fallback(
        monkeypatch,
        f"{sys.executable} --note <defunct> serve",
    )

    identity = release_authority.process_runtime_identity(_PS_PATH_PID)

    assert identity is not None
    assert identity["argv"][1:] == ["--note", "<defunct>", "serve"]


def test_supervision_process_runtime_identity_returns_none_when_proc_access_is_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A /proc entry owned by another user denies access rather than reporting
    # absence, and Path.exists re-raises that. The reader promises `dict | None`,
    # so it must answer None instead of raising through its callers.
    real_exists = Path.exists

    def denied(self: Path, **kwargs: object) -> bool:
        if str(self) == "/proc/4242/exe":
            raise PermissionError(13, "Permission denied")
        return real_exists(self, **kwargs)  # type: ignore[arg-type]

    def refuse(*_args: object, **_kwargs: object) -> SimpleNamespace:
        raise AssertionError("ps must not be consulted for a denied /proc entry")

    monkeypatch.setattr(Path, "exists", denied)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(release_authority.subprocess, "run", refuse)

    # Answering from ps would derive the executable from a spoofable argv[0], so
    # a denied entry must decline outright rather than fall through to it.
    assert release_authority.process_runtime_identity(4242) is None


class _PendingCmdline:
    """A /proc/<pid>/cmdline that reads empty until the kernel publishes argv."""

    def __init__(
        self,
        payload: bytes,
        empty_reads: int,
        on_read: Callable[[], None] | None = None,
    ) -> None:
        self._payload = payload
        self._remaining = empty_reads
        self._on_read = on_read
        self.reads = 0

    def read_bytes(self) -> bytes:
        self.reads += 1
        if self._on_read is not None:
            self._on_read()
        if self._remaining > 0:
            self._remaining -= 1
            return b""
        return self._payload


def test_supervision_process_runtime_identity_retries_mid_exec_empty_cmdline(
    tmp_path: Path,
) -> None:
    # A process that has execed but whose argv the kernel has not published yet
    # reads back as an empty cmdline behind a resolvable exe. That is a pending
    # identity, not an absent one, so the reader must wait for it rather than
    # reporting that a live process has no identity at all.
    executable = tmp_path / "python3"
    executable.write_text("fixture\n", encoding="utf-8")
    command = _PendingCmdline(
        b"\0".join([str(executable).encode(), b"-I", b"-m", b"http.server", b"0"]),
        empty_reads=3,
    )

    identity = release_authority._proc_runtime_identity(
        os.getpid(),
        executable,
        command,
    )

    assert identity is not None
    assert Path(identity["executable"]) == executable.resolve()
    assert identity["argv"][1:] == ["-I", "-m", "http.server", "0"]
    assert command.reads == 4


def test_supervision_process_runtime_identity_stops_waiting_once_the_process_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A process that exits mid-wait drops its mm, so /proc/<pid>/exe stops
    # resolving. The wait must notice on the very next pass instead of polling a
    # pid that is already gone, which only holds while the exe is re-resolved
    # every iteration rather than hoisted out of the loop.
    monkeypatch.setattr(release_authority, "_MID_EXEC_ARGV_DEADLINE_SECONDS", 5.0)
    executable = tmp_path / "python3"
    executable.write_text("fixture\n", encoding="utf-8")
    command = _PendingCmdline(
        b"never published",
        empty_reads=1 << 30,
        on_read=lambda: executable.unlink(missing_ok=True),
    )

    identity = release_authority._proc_runtime_identity(
        os.getpid(),
        executable,
        command,
    )

    assert identity is None
    # One read, then the vanished exe ends the wait. A reader that resolved the
    # exe once up front would keep reading here until the deadline expired.
    assert command.reads == 1


def test_supervision_process_runtime_identity_fails_closed_when_argv_never_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Waiting out the mid-exec window must not soften the verdict: an identity we
    # never observe stays None so callers keep rejecting unprovable authority.
    monkeypatch.setattr(release_authority, "_MID_EXEC_ARGV_DEADLINE_SECONDS", 0.05)
    executable = tmp_path / "python3"
    executable.write_text("fixture\n", encoding="utf-8")
    forever_empty = _PendingCmdline(b"", empty_reads=1 << 30)

    started = time.monotonic()
    assert (
        release_authority._proc_runtime_identity(
            os.getpid(),
            executable,
            forever_empty,
        )
        is None
    )
    assert time.monotonic() - started >= 0.05
    assert forever_empty.reads > 1

    # A pid whose /proc entry is already unreadable never enters the wait at all.
    vanished = tmp_path / "gone"
    reads_before = forever_empty.reads
    assert (
        release_authority._proc_runtime_identity(
            os.getpid(),
            vanished,
            forever_empty,
        )
        is None
    )
    # Asserting the cmdline was never read pins "returned without waiting" without
    # timing the machine, which on a contended runner is its own flake.
    assert forever_empty.reads == reads_before


@pytest.mark.slow
@pytest.mark.skipif(
    not Path("/proc/self/cmdline").is_file(),
    reason="the mid-exec argv window is a /proc behaviour",
)
def test_supervision_process_runtime_identity_survives_repeated_spawns() -> None:
    # The single-child test above only samples the mid-exec window on ~2% of
    # spawns, so it witnesses a regression rarely. Repeating the spawn turns that
    # into a reliable detector: against the pre-fix reader this fails within a few
    # hundred spawns on a contended two-core runner. Marked slow so it runs in the
    # nightly lane rather than the fast lane whose flake it exists to prevent.
    trials = int(os.environ.get("OKG_PROC_IDENTITY_TRIALS", "200"))
    for attempt in range(trials):
        process = subprocess.Popen(
            [sys.executable, "-I", "-m", "http.server", "0"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            identity = release_authority.process_runtime_identity(process.pid)
            assert identity is not None, f"no runtime identity on spawn {attempt}"
            assert identity["argv"][1:] == ["-I", "-m", "http.server", "0"]
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def test_supervision_plan_bound_readback_recovers_legacy_install_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    marker = environment.runtime.state_root / "supervisor/install.json"
    marker.parent.mkdir(parents=True)
    marker.write_text(
        json.dumps(
            {
                "schema": "okg.environment_service_install/v1",
                "environment_id": environment.id,
                "artifact_id": (plan.get("release_status") or {}).get("artifact_id"),
                "plan_digest": plan["plan_digest"],
            }
        ),
        encoding="utf-8",
    )
    marker.chmod(0o600)

    observed: list[list[str]] = []

    def run(
        argv: Sequence[str],
        **_kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        observed.append(list(argv))
        return subprocess.CompletedProcess(
            list(argv),
            0,
            (
                "LoadState=loaded\n"
                f"MainPID={os.getpid()}\n"
                "ActiveState=active\n"
                "SubState=running\n"
                "Job=\n"
                "Result=success\n"
                "ExecMainCode=1\n"
                "ExecMainStatus=0\n"
                f"ExecMainPID={os.getpid()}\n"
                "InvocationID=0123456789abcdef0123456789abcdef\n"
            ),
            "",
        )

    monkeypatch.setattr(environment_services.subprocess, "run", run)

    unbound = environment_services._supervisor_process_status(
        environment,
        service_id=environment.runtime.worker_service,
    )
    bound = environment_services._supervisor_process_status(
        environment,
        service_id=environment.runtime.worker_service,
        plan=plan,
    )

    assert unbound["ok"] is False
    assert unbound["message"] == "installed service adapter is invalid"
    assert bound == {
        "schema": "okg.environment_supervisor_process_status/v1",
        "environment_id": environment.id,
        "service_id": environment.runtime.worker_service,
        "ok": True,
        "status": "running",
        "pid": os.getpid(),
        "manager_outcome": {
            "adapter": "systemd",
            "load_state": "loaded",
            "result": "success",
            "exec_main_code": 1,
            "exec_main_status": 0,
            "exec_main_pid": os.getpid(),
            "invocation_id": "0123456789abcdef0123456789abcdef",
            "exit_status_observed": True,
        },
    }
    assert observed == [
        [
            "systemctl",
            "--user",
            "show",
            "--property=LoadState",
            "--property=MainPID",
            "--property=ActiveState",
            "--property=SubState",
            "--property=Job",
            "--property=Result",
            "--property=ExecMainCode",
            "--property=ExecMainStatus",
            "--property=ExecMainPID",
            "--property=InvocationID",
            f"{environment.runtime.worker_service}.service",
        ]
    ]


@pytest.mark.parametrize(
    "systemd_state",
    [
        "LoadState=loaded\nMainPID=0\nActiveState=activating\n"
        "SubState=auto-restart\nJob=\nResult=success\nExecMainCode=1\n"
        "ExecMainStatus=0\nExecMainPID=0\nInvocationID=\n",
        "LoadState=loaded\nMainPID=4242\nActiveState=deactivating\n"
        "SubState=stop-sigterm\nJob=\nResult=success\nExecMainCode=1\n"
        "ExecMainStatus=0\nExecMainPID=4242\n"
        "InvocationID=0123456789abcdef0123456789abcdef\n",
        "LoadState=loaded\nMainPID=0\nActiveState=inactive\nSubState=dead\n"
        "Job=99\nResult=success\nExecMainCode=1\nExecMainStatus=0\n"
        "ExecMainPID=0\nInvocationID=\n",
    ],
)
def test_systemd_process_readback_refuses_transitional_or_pending_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    systemd_state: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(
        environment,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    monkeypatch.setattr(
        environment_services.subprocess,
        "run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(
            list(argv), 0, systemd_state, ""
        ),
    )

    status = environment_services._supervisor_process_status(
        environment,
        service_id=environment.runtime.worker_service,
        plan=plan,
    )

    assert status["ok"] is False
    assert status["status"] == "invalid"


@pytest.mark.parametrize(
    ("manager_outcome", "error"),
    [
        (
            {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "success",
                "exec_main_code": 1,
                "exec_main_status": 0,
            },
            None,
        ),
        (
            {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "exit-code",
                "exec_main_code": 1,
                "exec_main_status": 6,
            },
            "not a clean process exit",
        ),
        (
            {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "success",
                "exec_main_code": 1,
                "exec_main_status": 75,
            },
            "not a clean process exit",
        ),
    ],
)
def test_restart_boundary_propagates_manager_process_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manager_outcome: dict[str, Any],
    error: str | None,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    inventory = {
        "worker": {
            "state": "running",
            "identity": {"pid": 424242, "process_start_identity": "before"},
        },
        "services": {
            environment.runtime.worker_service: {
                "state": "running",
                "identity": {"pid": 424242, "process_start_identity": "before"},
                "authority": {
                    "pid": 424242,
                    "process_start_identity": "before",
                    "systemd_invocation_id": "0123456789abcdef0123456789abcdef",
                },
                "supervisor": {
                    "manager_outcome": {
                        "adapter": "systemd",
                        "exec_main_pid": 424242,
                        "invocation_id": "0123456789abcdef0123456789abcdef",
                    }
                },
            }
        },
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": {
                "exec_main_pid": 424242,
                "invocation_id": "0123456789abcdef0123456789abcdef",
                **manager_outcome,
            },
        },
    )

    if error is None:
        environment_services._attest_restart_inventory_stopped(
            environment,
            plan=plan,
            inventory=inventory,
        )
    else:
        with pytest.raises(DeploymentConfigError, match=error):
            environment_services._attest_restart_inventory_stopped(
                environment,
                plan=plan,
                inventory=inventory,
            )


def test_restart_boundary_refuses_exact_process_that_failed_before_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    inventory = {
        "worker": {
            "state": "stopped",
            "identity": {"pid": 424242, "process_start_identity": "failed"},
        },
        "services": {
            environment.runtime.worker_service: {
                "state": "stopped",
                "identity": {"pid": 424242, "process_start_identity": "failed"},
                "authority": {
                    "pid": 424242,
                    "process_start_identity": "failed",
                    "systemd_invocation_id": "0123456789abcdef0123456789abcdef",
                },
            }
        },
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "failed",
            "pid": None,
            "manager_outcome": {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "exit-code",
                "exec_main_code": 1,
                "exec_main_status": 3,
            },
        },
    )

    with pytest.raises(DeploymentConfigError, match="not a clean process exit"):
        environment_services._attest_restart_inventory_stopped(
            environment,
            plan=plan,
            inventory=inventory,
        )


def test_systemd_failed_stop_cannot_be_laundered_by_restart_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    invocation_id = "0123456789abcdef0123456789abcdef"
    service_id = environment.runtime.worker_service
    service = {
        "state": "running",
        "identity": {"pid": 424242, "process_start_identity": "before"},
        "authority": {
            "pid": 424242,
            "process_start_identity": "before",
            "systemd_invocation_id": invocation_id,
        },
        "supervisor": {
            "manager_outcome": {
                "adapter": "systemd",
                "exec_main_pid": 424242,
                "invocation_id": invocation_id,
            }
        },
    }
    inventory = {
        "worker": {
            "state": "running",
            "identity": {"pid": 424242, "process_start_identity": "before"},
        },
        "services": {service_id: service},
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "failed",
            "pid": None,
            "manager_outcome": {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "exit-code",
                "exec_main_code": 1,
                "exec_main_status": 6,
                "exec_main_pid": 424242,
                "invocation_id": invocation_id,
            },
        },
    )

    for state in ("running", "stopped"):
        service["state"] = state
        with pytest.raises(DeploymentConfigError, match="not a clean process exit"):
            environment_services._attest_restart_inventory_stopped(
                environment,
                plan=plan,
                inventory=inventory,
            )


def test_systemd_clean_exit_from_replacement_invocation_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    service_id = environment.runtime.worker_service
    inventory = {
        "worker": {
            "state": "running",
            "identity": {"pid": 424242, "process_start_identity": "before"},
        },
        "services": {
            service_id: {
                "state": "running",
                "identity": {"pid": 424242, "process_start_identity": "before"},
                "authority": {
                    "pid": 424242,
                    "process_start_identity": "before",
                    "systemd_invocation_id": "0123456789abcdef0123456789abcdef",
                },
            }
        },
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": {
                "adapter": "systemd",
                "exit_status_observed": True,
                "result": "success",
                "exec_main_code": 1,
                "exec_main_status": 0,
                "exec_main_pid": 525252,
                "invocation_id": "fedcba9876543210fedcba9876543210",
            },
        },
    )

    with pytest.raises(DeploymentConfigError, match="not a clean process exit"):
        environment_services._attest_restart_inventory_stopped(
            environment,
            plan=plan,
            inventory=inventory,
        )


def test_launchd_failed_terminal_receipt_cannot_be_laundered_by_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="launchd", environ=values)
    service_id = environment.runtime.worker_service
    authority = {
        "pid": 424242,
        "process_start_identity": "proc:before",
        "lifecycle_nonce": "n" * 32,
    }
    receipt = environment_services._launchd_supervision_receipt_path(
        environment, service_id
    )
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(
        json.dumps(
            {
                "schema": "okg.launchd_service_supervision/v1",
                "state": "exited",
                "service_id": service_id,
                "lifecycle_nonce": "n" * 32,
                "supervisor_pid": 434343,
                "supervisor_process_start_identity": "proc:wrapper",
                "child_pid": 424242,
                "child_process_start_identity": "proc:before",
                "started_at": "2026-08-27T00:00:00+00:00",
                "exited_at": "2026-08-27T00:00:01+00:00",
                "exit_code": 6,
                "term_signal": None,
            }
        ),
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    service = {
        "state": "running",
        "identity": {"pid": 424242, "process_start_identity": "proc:before"},
        "authority": authority,
    }
    inventory = {
        "worker": {
            "state": "running",
            "identity": {"pid": 424242, "process_start_identity": "proc:before"},
        },
        "services": {service_id: service},
    }
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": {
                "adapter": "launchd",
                "exit_status_observed": False,
            },
        },
    )

    for state in ("running", "stopped"):
        service["state"] = state
        with pytest.raises(DeploymentConfigError, match="clean exact-PID"):
            environment_services._attest_restart_inventory_stopped(
                environment,
                plan=plan,
                inventory=inventory,
            )


def _origin_dev_upgrade_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
    *,
    post_fix_receipts: bool,
) -> SimpleNamespace:
    """An installed predecessor with one live, receipted process per role.

    ``post_fix_receipts`` writes receipts as the #2861 launcher does (with a
    ``pycache_prefix`` and a flagged live argv); otherwise they have the exact
    shape the origin/dev launcher wrote before #2861.
    """
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    original_plan = render_environment_services(
        environment,
        adapter=adapter,
        systemd_scope="user",
        environ=values,
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": (
                {
                    "adapter": "launchd",
                    "load_state": "not-found",
                    "exit_status_observed": False,
                }
                if adapter == "launchd"
                else {
                    "adapter": "systemd",
                    "load_state": "not-found",
                    "result": "success",
                    "exec_main_code": 1,
                    "exec_main_status": 0,
                    "exit_status_observed": True,
                }
            ),
        },
    )
    install_environment_services(environment, original_plan)
    if adapter == "launchd":
        for artifact in original_plan["artifacts"]:
            if not str(artifact.get("kind", "")).endswith("_service"):
                continue
            plist_path = Path(artifact["path"])
            document = plistlib.loads(plist_path.read_bytes())
            argv = list(document["ProgramArguments"])
            assert argv[4] == "--supervision-receipt" and argv[6] == "--"
            document["ProgramArguments"] = [*argv[:4], "--", *argv[7:]]
            plist_path.write_bytes(plistlib.dumps(document, sort_keys=True))
            plist_path.chmod(0o644)
    previous_artifact = "sha256:" + "a" * 64
    current_artifact = "sha256:" + "b" * 64
    previous_root = environment.release.artifact_store / ("a" * 64)
    current_root = environment.release.artifact_store / ("b" * 64)
    previous_interpreter = previous_root / "venv/bin/python3"
    previous_repo = previous_root / "repo"
    previous_interpreter.parent.mkdir(parents=True, exist_ok=True)
    previous_repo.mkdir(parents=True, exist_ok=True)
    previous_interpreter.write_text("origin/dev interpreter\n", encoding="utf-8")
    pointer = {
        "schema": "okg.active_release/v1",
        "artifact_id": previous_artifact,
        "artifact_root": str(previous_root),
        "definition_digest": environment.definition_digest,
        "graph_deployment_name": environment.graph_deployment_name,
    }
    active_pointer = {
        "schema": "okg.active_release/v1",
        "artifact_id": current_artifact,
        "artifact_root": str(current_root),
        "definition_digest": "sha256:" + "c" * 64,
        "predecessor": {
            "schema": "okg.release_predecessor/v1",
            "artifact_id": previous_artifact,
            "definition_digest": environment.definition_digest,
            "pointer_digest": release_activation.active_release_pointer_digest(pointer),
        },
    }
    marker = environment.runtime.state_root / "supervisor/install.json"
    marker.write_text(
        json.dumps(
            {
                "schema": "okg.environment_service_install/v1",
                "environment_id": environment.id,
                "artifact_id": previous_artifact,
                "plan_digest": original_plan["plan_digest"],
                "installed_at": datetime.now(timezone.utc).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    marker.chmod(0o600)
    current_release = {
        "ok": True,
        "status": "active",
        "artifact_id": current_artifact,
        "definition_digest": "sha256:" + "c" * 64,
        "active_pointer_digest": release_activation.active_release_pointer_digest(
            active_pointer
        ),
        "previous_artifact_id": previous_artifact,
        "previous_artifact_root": str(previous_root),
        "previous_definition_digest": environment.definition_digest,
        "previous_pointer_digest": release_activation.active_release_pointer_digest(
            pointer
        ),
    }
    previous_release = {
        "ok": True,
        "status": "previous",
        "environment_id": environment.id,
        "artifact_id": previous_artifact,
        "artifact_root": str(previous_root),
        "definition_digest": environment.definition_digest,
        "pointer_digest": current_release["previous_pointer_digest"],
        "successor_pointer_digest": current_release["active_pointer_digest"],
        "active_pointer": str(environment.release.active_pointer),
        "pointer_path": str(environment.release.previous_pointer),
    }
    receipt_root = environment.runtime.state_root / "service-authority"
    receipt_root.mkdir(parents=True, exist_ok=True)
    previous_services = original_plan["handoff_topology"]["generations"][
        "current"
    ]["services"]
    live_processes: dict[int, tuple[str, list[str]]] = {}
    for index, (service_id, arguments) in enumerate(previous_services.items()):
        pid = 440_000 + index
        start_identity = f"origin-dev:{service_id}"
        receipt = receipt_root / f"{service_id}.json"
        receipt.write_text(
            json.dumps(
                {
                    "schema": "okg.service_release_authority/v1",
                    "verified_at": (
                        datetime.now(timezone.utc) - timedelta(minutes=1)
                    ).isoformat(),
                    "service_id": service_id,
                    "environment_id": environment.id,
                    "artifact_id": previous_artifact,
                    "artifact_root": str(previous_root),
                    "active_pointer": str(environment.release.active_pointer),
                    "interpreter": str(previous_interpreter),
                    "repo_root": str(previous_repo),
                    "isolated": True,
                    "arguments": arguments,
                    **(
                        {"pycache_prefix": _launch_prefix(environment, str(pid))}
                        if post_fix_receipts
                        else {}
                    ),
                    "pid": pid,
                    "process_start_identity": start_identity,
                }
            ),
            encoding="utf-8",
        )
        receipt.chmod(0o600)
        live_processes[pid] = (start_identity, list(arguments))
    monkeypatch.setattr(
        environment_services,
        "load_active_release",
        lambda path, **_kwargs: dict(
            active_pointer
            if Path(path) == environment.release.active_pointer
            else pointer
        ),
    )
    monkeypatch.setattr(
        environment_services,
        "environment_release_status",
        lambda *_args, **_kwargs: dict(current_release),
    )
    monkeypatch.setattr(
        environment_services,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (dict(previous_release), environment),
    )
    monkeypatch.setattr(
        environment_services,
        "pid_alive",
        lambda pid: pid in live_processes,
    )
    monkeypatch.setattr(
        environment_services,
        "process_start_identity",
        lambda pid: live_processes.get(pid, (None, []))[0],
    )
    monkeypatch.setattr(
        environment_services,
        "process_runtime_identity",
        lambda pid: (
            {
                "executable": str(previous_interpreter),
                "argv": [
                    str(previous_interpreter),
                    *(
                        _launched(
                            live_processes[pid][1],
                            _launch_prefix(environment, str(pid)),
                        )
                        if post_fix_receipts
                        else live_processes[pid][1]
                    ),
                ],
            }
            if pid in live_processes
            else None
        ),
    )
    pid_by_service = {
        service_id: pid
        for pid, (_identity, arguments) in live_processes.items()
        for service_id, expected_arguments in previous_services.items()
        if arguments == list(expected_arguments)
    }
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda _environment, *, service_id, **_kwargs: {
            "ok": service_id in pid_by_service,
            "status": "running" if service_id in pid_by_service else "stopped",
            "pid": pid_by_service.get(service_id),
            "manager_outcome": {"adapter": adapter},
        },
    )

    return SimpleNamespace(
        environment=environment,
        values=values,
        marker=marker,
        current_release=current_release,
        previous_release=previous_release,
        receipt_root=receipt_root,
        previous_services=previous_services,
        live_processes=live_processes,
        pid_by_service=pid_by_service,
    )


@pytest.mark.parametrize("adapter", ["systemd", "launchd"])
def test_origin_dev_install_marker_upgrades_through_render_install_and_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
) -> None:
    fixture = _origin_dev_upgrade_fixture(
        tmp_path, monkeypatch, adapter, post_fix_receipts=True
    )
    environment = fixture.environment
    values = fixture.values
    marker = fixture.marker
    current_release = fixture.current_release
    previous_release = fixture.previous_release
    receipt_root = fixture.receipt_root
    previous_services = fixture.previous_services
    for service_id in previous_services:
        authority = environment_service_authority_status(
            environment,
            service_id=service_id,
            release_status=previous_release,
        )
        assert authority["ok"] is True, authority

    plan = render_environment_services(
        environment,
        adapter=adapter,
        systemd_scope="user",
        environ=values,
    )
    inferred = environment_services._previous_manager_binding(
        environment,
        current_release=current_release,
    )
    assert inferred["adapter"] == adapter
    assert inferred["systemd_scope"] == ("user" if adapter == "systemd" else None)
    assert inferred["service_topology"] == plan["handoff_topology"][
        "generations"
    ]["previous"]

    monkeypatch.setattr(
        environment_services,
        "_require_environment_service_install_authority",
        lambda *_args, **_kwargs: None,
    )
    install_environment_services(environment, plan)
    migrated = json.loads(marker.read_text(encoding="utf-8"))
    assert migrated["previous_installation"]["adapter"] == adapter
    assert migrated["previous_installation"]["systemd_scope"] == (
        "user" if adapter == "systemd" else None
    )
    if adapter == "launchd":
        assert set(
            migrated["previous_installation"]["launchd_process_models"].values()
        ) == {"direct_exec"}
        worker_log = environment.runtime.log_root / "worker.log"
        worker_log.write_text(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            + " INFO    okg.substrate.runtime.worker: worker exited cleanly: "
            + f"deployment={environment.graph_deployment_name}\n",
            encoding="utf-8",
        )
        worker_log.chmod(0o644)
        worker_authority = json.loads(
            (
                receipt_root
                / f"{environment.runtime.worker_service}.json"
            ).read_text(encoding="utf-8")
        )
        environment_services._require_clean_manager_stop_outcome(
            environment=environment,
            service_id=environment.runtime.worker_service,
            supervisor={
                "manager_outcome": {
                    "adapter": "launchd",
                    "exit_status_observed": False,
                }
            },
            worker_role=True,
            service_before={"authority": worker_authority},
        )

    monkeypatch.setattr(
        environment_services,
        "_environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "invalid",
            "message": "live predecessor definition",
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_validate_service_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: {"services": {}},
    )
    monkeypatch.setattr(
        environment_services,
        "_attest_restart_inventory_stopped",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_record_predecessor_stop",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        environment_services,
        "_wait_for_service_authority",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    observed: list[list[str]] = []

    def run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        observed.append(list(argv))
        return subprocess.CompletedProcess(list(argv), 0, "", "")

    results = execute_environment_service_action(
        plan,
        action="restart",
        environment=environment,
        runner=run,
    )
    stop_verb = "stop" if adapter == "systemd" else "bootout"
    start_verb = "enable" if adapter == "systemd" else "bootstrap"
    stop_index = next(
        index for index, argv in enumerate(observed) if stop_verb in argv
    )
    start_index = next(
        index for index, argv in enumerate(observed) if start_verb in argv
    )
    assert stop_index < start_index
    assert all(result["returncode"] == 0 for result in results)


@pytest.mark.parametrize("adapter", ["systemd", "launchd"])
def test_live_pre_2861_service_refuses_restart_until_it_is_stopped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
) -> None:
    """#2861 fails closed on receipts without a bytecode cache prefix.

    A service still running from the origin/dev launcher cannot be proven, so
    the restart handoff refuses it as an unknown or foreign process. Once the
    operator stops it, its old receipt only proves a dead process, and the
    handoff accepts that.
    """
    fixture = _origin_dev_upgrade_fixture(
        tmp_path, monkeypatch, adapter, post_fix_receipts=False
    )
    environment = fixture.environment
    for service_id in fixture.previous_services:
        receipt = json.loads(
            (fixture.receipt_root / f"{service_id}.json").read_text(encoding="utf-8")
        )
        assert "pycache_prefix" not in receipt
        authority = environment_service_authority_status(
            environment,
            service_id=service_id,
            release_status=fixture.previous_release,
        )
        assert authority["ok"] is False
        assert "records no bytecode cache prefix" in authority["message"]
    plan = render_environment_services(
        environment,
        adapter=adapter,
        systemd_scope="user",
        environ=fixture.values,
    )

    with pytest.raises(DeploymentConfigError, match="unknown or foreign process"):
        environment_services._restart_process_inventory(
            environment,
            plan=plan,
            current_release=fixture.current_release,
        )

    fixture.live_processes.clear()
    fixture.pid_by_service.clear()
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
            "manager_outcome": (
                {
                    "adapter": "launchd",
                    "load_state": "not-found",
                    "exit_status_observed": False,
                }
                if adapter == "launchd"
                else {
                    "adapter": "systemd",
                    "load_state": "not-found",
                    "result": "success",
                    "exec_main_code": 1,
                    "exec_main_status": 0,
                    "exit_status_observed": True,
                }
            ),
        },
    )
    inventory = environment_services._restart_process_inventory(
        environment,
        plan=plan,
        current_release=fixture.current_release,
    )
    assert set(inventory["services"]) == set(fixture.previous_services)
    assert all(
        row["state"] == "stopped" for row in inventory["services"].values()
    )


def test_supervision_ui_authority_receipt_proves_launcher_process_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_root = tmp_path / "artifact"
    interpreter = artifact_root / "venv/bin/python3"
    repo_root = artifact_root / "repo"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("fixture\n", encoding="utf-8")
    repo_root.mkdir()
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "b" * 64,
        "artifact_root": str(artifact_root),
        "definition_digest": environment.definition_digest,
    }
    monkeypatch.setattr(
        "okg.substrate.environment_services.environment_release_status",
        lambda *_args, **_kwargs: release_status,
    )
    receipt = (
        environment.runtime.state_root
        / "service-authority"
        / f"{environment.ui.service}.json"
    )
    receipt.parent.mkdir(parents=True)
    receipt.write_text(
        json.dumps(
            {
                "schema": "okg.service_release_authority/v1",
                "service_id": environment.ui.service,
                "environment_id": environment.id,
                "artifact_id": release_status["artifact_id"],
                "artifact_root": str(artifact_root),
                "definition_digest": environment.definition_digest,
                "active_pointer": str(environment.release.active_pointer),
                "interpreter": str(interpreter),
                "repo_root": str(repo_root),
                "isolated": True,
                "arguments": [
                    "-I",
                    "-m",
                    "okg.viz",
                    "--deployment",
                    environment.graph_deployment_name,
                    "--host",
                    environment.ui.host,
                    "--port",
                    str(environment.ui.port),
                ],
                "pycache_prefix": _launch_prefix(environment, "ui"),
                "pid": os.getpid(),
                "process_start_identity": release_authority.process_start_identity(
                    os.getpid()
                ),
            }
        ),
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    expected_arguments = environment_services._expected_service_arguments(
        environment,
        environment.ui.service,
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid(),
            "manager_outcome": {"adapter": "systemd"},
        },
    )
    monkeypatch.setattr(
        environment_services,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str(interpreter.resolve()),
            "argv": [str(interpreter), *_launched(expected_arguments, _launch_prefix(environment, "ui"))],
        },
    )

    status = environment_service_authority_status(
        environment,
        service_id=environment.ui.service,
        environ=values,
    )

    assert status["ok"] is True
    assert status["authority"]["arguments"] == list(expected_arguments)

    manager_reads = {"count": 0}

    def changing_manager(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        manager_reads["count"] += 1
        return {
            "ok": True,
            "status": "running",
            "pid": os.getpid() if manager_reads["count"] == 1 else os.getpid() + 1,
            "manager_outcome": {"adapter": "systemd"},
        }

    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        changing_manager,
    )
    changed_manager = environment_service_authority_status(
        environment,
        service_id=environment.ui.service,
        environ=values,
    )
    assert changed_manager["ok"] is False
    assert "changed during authority readback" in changed_manager["message"]
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid(),
            "manager_outcome": {"adapter": "systemd"},
        },
    )

    decoy_arguments = ["-I", "-c", "import time; time.sleep(3600)"]
    decoy = json.loads(receipt.read_text(encoding="utf-8"))
    decoy["arguments"] = decoy_arguments
    receipt.write_text(json.dumps(decoy), encoding="utf-8")
    receipt.chmod(0o600)
    monkeypatch.setattr(
        environment_services,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str(interpreter.resolve()),
            "argv": [
                str(interpreter),
                *_launched(decoy_arguments, _launch_prefix(environment, "ui")),
            ],
        },
    )
    status = environment_service_authority_status(
        environment,
        service_id=environment.ui.service,
        environ=values,
    )
    assert status["ok"] is False
    assert "configured service role" in status["message"]

    decoy["arguments"] = list(expected_arguments)
    receipt.write_text(json.dumps(decoy), encoding="utf-8")
    receipt.chmod(0o600)
    monkeypatch.setattr(
        environment_services,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str(interpreter.resolve()),
            "argv": [str(interpreter), *_launched(expected_arguments, _launch_prefix(environment, "ui"))],
        },
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid() + 1,
            "manager_outcome": {"adapter": "systemd"},
        },
    )
    status = environment_service_authority_status(
        environment,
        service_id=environment.ui.service,
        environ=values,
    )
    assert status["ok"] is False
    assert "service manager main pid" in status["message"]
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid(),
            "manager_outcome": {"adapter": "systemd"},
        },
    )

    monkeypatch.setattr(
        environment_services,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str((tmp_path / "foreign/python3").resolve()),
            "argv": [str(tmp_path / "foreign/python3"), *_launched(expected_arguments, _launch_prefix(environment, "ui"))],
        },
    )
    status = environment_service_authority_status(
        environment,
        service_id=environment.ui.service,
        environ=values,
    )
    assert status["ok"] is False
    assert "executable differs" in status["message"]

    raw = json.loads(receipt.read_text(encoding="utf-8"))
    raw["artifact_id"] = "sha256:" + "c" * 64
    receipt.write_text(json.dumps(raw), encoding="utf-8")
    status = environment_service_authority_status(
        environment,
        service_id=environment.ui.service,
        environ=values,
    )
    assert status["ok"] is False
    assert "artifact_id" in status["message"]


def test_launchd_wrapper_pid_is_independently_bound_to_child_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    service_id = environment.runtime.worker_service
    authority = {
        "pid": 424242,
        "process_start_identity": "proc:child",
        "lifecycle_nonce": "n" * 32,
    }
    receipt = environment_services._launchd_supervision_receipt_path(
        environment, service_id
    )
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(
        json.dumps(
            {
                "schema": "okg.launchd_service_supervision/v1",
                "state": "running",
                "service_id": service_id,
                "lifecycle_nonce": "n" * 32,
                "supervisor_pid": 434343,
                "supervisor_process_start_identity": "proc:wrapper",
                "child_pid": 424242,
                "child_process_start_identity": "proc:child",
                "started_at": "2026-08-27T00:00:00+00:00",
                "exit_code": None,
                "term_signal": None,
            }
        ),
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    monkeypatch.setattr(
        environment_services,
        "process_start_identity",
        lambda pid: "proc:wrapper" if pid == 434343 else None,
    )

    assert environment_services._service_supervisor_matches_authority(
        environment,
        service_id=service_id,
        supervisor={
            "pid": 434343,
            "manager_outcome": {"adapter": "launchd"},
        },
        authority=authority,
    )
    assert not environment_services._service_supervisor_matches_authority(
        environment,
        service_id=service_id,
        supervisor={
            "pid": 444444,
            "manager_outcome": {"adapter": "launchd"},
        },
        authority=authority,
    )


def test_supervision_cli_blocks_colliding_fleet_before_render(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, values = _environment_fixture(tmp_path)
    production_path = root / "fixture-production/environment.yaml"
    production = yaml.safe_load(production_path.read_text(encoding="utf-8"))
    production["runtime"]["worker_service"] = "org.okg.runtime-worker.fixture-staging"
    production_path.write_text(
        yaml.safe_dump(production, sort_keys=False),
        encoding="utf-8",
    )
    _set_values(monkeypatch, values)

    rc = main(
        [
            "--json",
            "environment",
            "service",
            "render",
            "fixture-staging",
            "--environments-root",
            str(root),
            "--adapter",
            "systemd",
        ]
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    collision = payload["resource_isolation"]["collisions"][0]
    assert collision["collision_domain"] == "service_id"
    assert "service_plan" not in payload


def test_supervision_cli_render_is_secret_redacted_and_environment_scoped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, values = _environment_fixture(tmp_path)
    _set_values(monkeypatch, values)

    rc = main(
        [
            "--json",
            "environment",
            "service",
            "render",
            "fixture-production",
            "--environments-root",
            str(root),
            "--adapter",
            "systemd",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["environment"]["id"] == "fixture-production"
    assert payload["service_plan"]["launchable"] is False
    assert "production-secret" not in repr(payload)
    assert "postgresql://" not in repr(payload)


def test_readiness_installs_distinct_trusted_stdio_mcp_registrations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    staging = _load(root, "fixture-staging", values)
    production = _load(root, "fixture-production", values)

    registrations: list[dict] = []
    for environment in (staging, production):
        plan = render_environment_services(
            environment,
            adapter="systemd",
            environ=values,
        )
        install_environment_services(environment, plan)
        artifact = _artifact(plan, "mcp_registration")
        registration = json.loads(Path(artifact["path"]).read_text(encoding="utf-8"))
        registrations.append(registration)
        server = registration["mcpServers"][environment.mcp.registration_id]
        command = [server["command"], *server["args"]]
        assert registration["environment_id"] == environment.id
        assert registration["deployment"] == environment.graph_deployment_name
        assert registration["transport"] == "stdio"
        assert any(Path(part).name == "environment_exec.py" for part in command)
        assert any(Path(part).name == "release_launcher.py" for part in command)
        assert "mcp-serve" in command
        assert environment.graph_deployment_name in command
        assert str(environment.ui.port) not in command

        discovered = discover_registered_servers(config_paths=[Path(artifact["path"])])
        assert len(discovered) == 1
        assert discovered[0].name == environment.mcp.registration_id
        assert discovered[0].deployment == environment.graph_deployment_name

    assert set(registrations[0]["mcpServers"]) != set(registrations[1]["mcpServers"])


def _patch_ready_environment_probes(
    monkeypatch: pytest.MonkeyPatch,
    environment,
    *,
    generation_id: str = "generation:ready",
    nomos_ok: bool = True,
    mcp_release_ok: bool = True,
    mcp_generation_ok: bool = True,
) -> None:
    artifact_root = environment.release.artifact_store / ("a" * 64)
    monkeypatch.setattr(
        environment_readiness,
        "environment_release_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "active",
            "artifact_id": "sha256:" + "a" * 64,
            "artifact_root": str(artifact_root),
            "definition_digest": "sha256:" + "b" * 64,
            "active_pointer_digest": "sha256:" + "c" * 64,
        },
    )
    monkeypatch.setattr(
        environment_readiness,
        "environment_process_authority_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "healthy",
            "worker": {"ok": True},
            "services": {},
        },
    )
    monkeypatch.setattr(
        environment_readiness,
        "environment_release_handoff_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "complete"},
    )
    monkeypatch.setattr(
        environment_readiness,
        "_probe_database_readiness",
        lambda *_args, **_kwargs: {
            "generation": {
                "status": "published",
                "generation_id": generation_id,
            },
            "nomos_read_path": {
                "ok": nomos_ok,
                "status": "pass" if nomos_ok else "failed",
                "denied": [] if nomos_ok else [{"subject": "subject:mcp_client"}],
            },
        },
    )
    monkeypatch.setattr(
        environment_readiness,
        "_probe_operational_readiness",
        lambda *_args, **_kwargs: {
            "checks": [
                {
                    "id": "database.migrations",
                    "status": "pass",
                    "ok": True,
                    "message": "migrations present",
                    "details": {},
                },
                {
                    "id": "queues.bounds",
                    "status": "pass",
                    "ok": True,
                    "message": "queue bounds pass",
                    "details": {},
                },
                {
                    "id": "sources.freshness",
                    "status": "pass",
                    "ok": True,
                    "message": "sources fresh",
                    "details": {},
                },
                {
                    "id": "doctor.okg_workspace",
                    "status": "pass",
                    "ok": True,
                    "message": "doctor passed",
                    "details": {},
                },
            ],
            "database": {"missing_relations": []},
            "queues": {"active_queue_depth": 0, "dlq_count": 0},
            "source_freshness": {"ok": True, "missing": [], "stale": []},
            "doctor": {"ok": True, "status": "pass"},
        },
    )
    monkeypatch.setattr(
        environment_readiness,
        "_probe_ui_http",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "healthy",
            "deployment_name": environment.graph_deployment_name,
            "generation_id": generation_id,
        },
    )
    monkeypatch.setattr(
        environment_readiness,
        "_probe_webhook_owner",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    monkeypatch.setattr(
        environment_readiness,
        "_probe_mcp_registration",
        lambda *_args, **_kwargs: {
            "ok": mcp_release_ok and mcp_generation_ok,
            "status": "healthy" if mcp_release_ok and mcp_generation_ok else "blocked",
            "release_authority_ok": mcp_release_ok,
            "inspect_ok": True,
            "search_ok": True,
            "generation_aligned": mcp_generation_ok,
            "generation_ids": [
                generation_id if mcp_generation_ok else "generation:stale",
                generation_id if mcp_generation_ok else "generation:stale",
            ],
            "surface": {"release_authority": {"status": "complete"}},
            "inspect": {"node_count": 1},
            "search": {"results": []},
        },
    )


def test_readiness_reports_each_environment_boundary_independently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    _patch_ready_environment_probes(monkeypatch, environment)

    report = environment_readiness_report(environment, plan)

    assert report["ok"] is True
    assert report["status"] == "ready"
    assert {row["id"] for row in report["checks"]} == {
        "release.active",
        "processes.authority",
        "services.installation",
        "release.handoff",
        "generation.latest_published",
        "database.migrations",
        "queues.bounds",
        "sources.freshness",
        "doctor.okg_workspace",
        "pact.prerequisites",
        "ui.http",
        "webhook.owner",
        "nomos.read_path",
        "mcp.registration",
        "mcp.release_authority",
        "mcp.inspect",
        "mcp.search",
        "mcp.generation_alignment",
    }
    assert report["blockers"] == []


def test_readiness_refuses_obsolete_service_reappearing_after_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    _patch_ready_environment_probes(monkeypatch, environment)
    monkeypatch.setattr(
        environment_readiness,
        "environment_release_handoff_status",
        lambda *_args, **_kwargs: {
            "ok": False,
            "status": "incomplete",
            "message": "obsolete predecessor service is running",
        },
    )

    report = environment_readiness_report(environment, plan)

    blockers = {row["id"] for row in report["blockers"]}
    assert "release.handoff" in blockers
    assert report["ok"] is False


def test_readiness_healthy_ui_does_not_hide_missing_mcp_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-production", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    _patch_ready_environment_probes(monkeypatch, environment)

    report = environment_readiness_report(environment, plan)
    statuses = {row["id"]: row["status"] for row in report["checks"]}

    assert statuses["ui.http"] == "pass"
    assert statuses["mcp.registration"] == "blocker"
    assert statuses["mcp.inspect"] == "pass"
    assert report["ok"] is False


@pytest.mark.parametrize(
    ("nomos_ok", "release_ok", "generation_ok", "expected_blocker"),
    [
        (False, True, True, "nomos.read_path"),
        (True, False, True, "mcp.release_authority"),
        (True, True, False, "mcp.generation_alignment"),
    ],
)
def test_readiness_names_policy_release_and_generation_blockers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    nomos_ok: bool,
    release_ok: bool,
    generation_ok: bool,
    expected_blocker: str,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    _patch_ready_environment_probes(
        monkeypatch,
        environment,
        nomos_ok=nomos_ok,
        mcp_release_ok=release_ok,
        mcp_generation_ok=generation_ok,
    )

    report = environment_readiness_report(environment, plan)

    assert expected_blocker in {row["id"] for row in report["blockers"]}


def test_readiness_stale_ui_generation_is_independent_alignment_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_receiptless_managers_stopped(monkeypatch)
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    plan = render_environment_services(environment, adapter="systemd", environ=values)
    install_environment_services(environment, plan)
    _patch_ready_environment_probes(monkeypatch, environment)
    monkeypatch.setattr(
        environment_readiness,
        "_probe_ui_http",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "healthy",
            "deployment_name": environment.graph_deployment_name,
            "generation_id": "generation:stale-ui",
        },
    )

    report = environment_readiness_report(environment, plan)

    blockers = {row["id"] for row in report["blockers"]}
    assert "ui.http" not in blockers
    assert "mcp.generation_alignment" in blockers


def test_readiness_ui_health_rejects_foreign_deployment_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "ok": True,
                    "ready": True,
                    "deployment_name": "fixture-production",
                    "generation_id": "generation:foreign",
                }
            ).encode("utf-8")

    def open_ui(url: str, **kwargs):
        assert url.startswith("http://")
        assert kwargs["allow_hosts"] == (environment.ui.host,)
        assert kwargs["same_origin"] == environment.ui.host
        assert kwargs["max_redirects"] == 0
        return Response()

    monkeypatch.setattr(environment_readiness, "open_validated_url", open_ui)

    status = environment_readiness._probe_ui_http(
        environment,
        timeout_seconds=1,
    )

    assert status["ok"] is False
    assert status["status"] == "foreign_or_unready"
    assert status["deployment_matches"] is False


def test_readiness_ui_health_refuses_redirect_without_second_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import urllib.error

    from okg.substrate.library.sources import _url_safety

    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    calls = {"count": 0}

    class RedirectingOpener:
        def open(self, request, timeout=None):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise urllib.error.HTTPError(
                    request.full_url,
                    302,
                    "Found",
                    {"Location": "/redirect-target"},
                    None,
                )
            raise AssertionError("readiness must not follow a redirect")

    monkeypatch.setattr(
        _url_safety.urllib.request,
        "build_opener",
        lambda *_args, **_kwargs: RedirectingOpener(),
    )

    status = environment_readiness._probe_ui_http(
        environment,
        timeout_seconds=1,
    )

    assert calls["count"] == 1
    assert status["ok"] is False
    assert status["status"] == "unavailable"
    assert status["error"] == "UnsafeFetchURL"


def test_readiness_stdio_mcp_probe_checks_inspect_search_root_and_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-production", values)
    artifact_root = environment.release.artifact_store / ("a" * 64)
    generation_id = "generation:published"
    calls: list[tuple[str, dict]] = []

    class FakeMcpClient:
        def __init__(self, command, **kwargs) -> None:
            assert command == ["trusted-mcp"]
            assert "OKG_SERVICE_ID" not in kwargs["env"]

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def call_tool(self, name: str, args: dict, *, meta: dict | None = None) -> dict:
            # The probe is a program: it asks for the JSON result format
            # (issue #3059).
            assert meta == {"okg/result_format": "json"}, meta
            calls.append((name, args))
            if name == "inspect" and args["target"]["kind"] == "surface":
                payload = {
                    "release_authority": {
                        "status": "complete",
                        "environment_id": environment.id,
                        "artifact_id": "sha256:" + "a" * 64,
                        "artifact_root": str(artifact_root),
                        "active_pointer": str(environment.release.active_pointer),
                    }
                }
            elif name == "inspect":
                payload = {
                    "node_count": None,
                    "pinned_generation_id": generation_id,
                }
            else:
                payload = {
                    "results": [],
                    "pinned_generation_id": generation_id,
                }
            return {"structuredContent": {"result": payload}}

    monkeypatch.setattr(
        "okg.substrate.library.sources.mcp_composed.client.McpClient",
        FakeMcpClient,
    )
    monkeypatch.setenv("OKG_SERVICE_ID", environment.runtime.worker_service)

    result = environment_readiness._probe_mcp_registration(
        environment,
        registration={"ok": True, "command": ["trusted-mcp"]},
        expected_release={
            "artifact_id": "sha256:" + "a" * 64,
            "artifact_root": str(artifact_root),
        },
        expected_generation_id=generation_id,
        timeout_seconds=1,
    )

    assert result["ok"] is True
    assert result["release_authority_ok"] is True
    assert result["inspect_ok"] is True
    assert result["search_ok"] is True
    assert result["generation_aligned"] is True
    assert [name for name, _args in calls] == ["inspect", "inspect", "search"]


@pytest.mark.parametrize("failed_probe", ["generation", "nomos"])
def test_readiness_database_probes_fail_independently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_probe: str,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

    monkeypatch.setattr("psycopg.connect", lambda *_args, **_kwargs: Connection())

    from okg.substrate.cli import deployment_readiness

    def generation(*_args, **_kwargs):
        if failed_probe == "generation":
            raise RuntimeError("generation failed")
        return {"status": "published", "generation_id": "generation:ok"}

    def nomos(*_args, **_kwargs):
        if failed_probe == "nomos":
            raise RuntimeError("nomos failed")
        return {"ok": True, "status": "pass"}

    monkeypatch.setattr(deployment_readiness, "_ready_generation_status", generation)
    monkeypatch.setattr(
        deployment_readiness,
        "_ready_nomos_read_probe_status",
        nomos,
    )

    result = environment_readiness._probe_database_readiness(environment)

    if failed_probe == "generation":
        assert result["generation"]["status"] == "unavailable"
        assert result["nomos_read_path"]["status"] == "pass"
    else:
        assert result["generation"]["status"] == "published"
        assert result["nomos_read_path"]["status"] == "unavailable"


def test_readiness_cli_dispatches_environment_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, values = _environment_fixture(tmp_path)
    _set_values(monkeypatch, values)
    monkeypatch.setattr(
        environment_readiness,
        "environment_readiness_report",
        lambda environment, _plan, **_kwargs: {
            "schema": "okg.environment_readiness/v1",
            "ok": True,
            "status": "ready",
            "environment_id": environment.id,
            "checks": [],
            "blockers": [],
        },
    )

    rc = main(
        [
            "--json",
            "environment",
            "ready",
            "fixture-staging",
            "--environments-root",
            str(root),
            "--adapter",
            "systemd",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ready"
    assert payload["environment_id"] == "fixture-staging"
    assert payload["resource_isolation"]["ok"] is True
    evidence_path = (
        Path(values["TEST_STATE_ROOT"])
        / "environments/fixture-staging/readiness/current.json"
    )
    persisted = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert persisted["captured_at"] == payload["captured_at"]
    assert persisted["evidence_path"] == str(evidence_path)
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600


def test_readiness_mcp_surface_reports_sealed_release_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "fixture-staging")
    monkeypatch.setenv("OKG_RELEASE_ARTIFACT_ID", "sha256:" + "a" * 64)
    monkeypatch.setenv("OKG_RELEASE_ROOT", "/srv/okg/artifacts/a")
    monkeypatch.setenv("OKG_ACTIVE_RELEASE_POINTER", "/srv/okg/active.json")

    report = mcp_surface_report(
        deployment_name="fixture-graph-staging",
        instructions="fixture",
    )

    assert report["release_authority"] == {
        "status": "complete",
        "environment_id": "fixture-staging",
        "artifact_id": "sha256:" + "a" * 64,
        "artifact_root": "/srv/okg/artifacts/a",
        "active_pointer": "/srv/okg/active.json",
        "missing": [],
    }


def test_readiness_webhook_health_proves_worker_pid_and_deployment(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    server, _stats = make_webhook_http_server(
        environment.database.dsn,
        deployment=environment.graph_deployment_name,
        registry={},
        host="127.0.0.1",
        port=0,
    )
    port = int(server.server_address[1])
    environment = replace(
        environment,
        webhook=replace(environment.webhook, port=port),
        manifest=replace(
            environment.manifest,
            runtime=replace(
                environment.manifest.runtime,
                source_sync={
                    **environment.manifest.runtime.source_sync,
                    "webhook": {
                        "enabled": True,
                        "host": "127.0.0.1",
                        "port": port,
                    },
                },
            ),
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status = environment_readiness._probe_webhook_owner(
            environment,
            processes={
                "worker": {
                    "ok": True,
                    "authority": {
                        "pid": os.getpid(),
                        "process_start_identity": (
                            release_authority.process_start_identity(os.getpid())
                        ),
                    },
                }
            },
            timeout_seconds=1,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert status["ok"] is True, json.dumps(status, sort_keys=True)
    assert status["owner_matches"] is True
    assert status["observed"]["deployment"] == environment.graph_deployment_name


def test_readiness_webhook_self_attestation_cannot_bypass_socket_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    expected_pid = 424242

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "ok": True,
                    "service": "embedded_webhook",
                    "deployment": environment.graph_deployment_name,
                    "pid": expected_pid,
                }
            ).encode("utf-8")

    def open_webhook(url: str, **kwargs):
        assert url.startswith("http://")
        assert kwargs["allow_hosts"] == (environment.webhook.host,)
        assert kwargs["same_origin"] == environment.webhook.host
        assert kwargs["max_redirects"] == 0
        return Response()

    monkeypatch.setattr(environment_readiness, "open_validated_url", open_webhook)
    monkeypatch.setattr(
        environment_readiness,
        "_pid_owns_tcp_listener",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        environment_readiness,
        "process_start_identity",
        lambda *_args, **_kwargs: "start:expected",
    )

    status = environment_readiness._probe_webhook_owner(
        environment,
        processes={
            "worker": {
                "ok": True,
                "authority": {
                    "pid": expected_pid,
                    "process_start_identity": "start:expected",
                },
            }
        },
        timeout_seconds=1,
    )

    assert status["ok"] is False
    assert status["owner_matches"] is False


def test_readiness_ui_health_requires_supervised_socket_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    expected_pid = 424242

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "ok": True,
                    "ready": True,
                    "deployment_name": environment.graph_deployment_name,
                    "generation_id": "generation:ready",
                    "pid": expected_pid,
                }
            ).encode("utf-8")

    def open_ui(url: str, **kwargs):
        assert url.startswith("http://")
        assert kwargs["allow_hosts"] == (environment.ui.host,)
        assert kwargs["same_origin"] == environment.ui.host
        assert kwargs["max_redirects"] == 0
        return Response()

    monkeypatch.setattr(environment_readiness, "open_validated_url", open_ui)
    monkeypatch.setattr(
        environment_readiness,
        "_pid_owns_tcp_listener",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        environment_readiness,
        "process_start_identity",
        lambda *_args, **_kwargs: "start:expected",
    )
    processes = {
        "services": {
            environment.ui.service: {
                "ok": True,
                "authority": {
                    "pid": expected_pid,
                    "process_start_identity": "start:expected",
                },
            }
        }
    }

    healthy = environment_readiness._probe_ui_http(
        environment,
        processes=processes,
        timeout_seconds=1,
    )
    assert healthy["ok"] is True

    monkeypatch.setattr(
        environment_readiness,
        "_pid_owns_tcp_listener",
        lambda *_args, **_kwargs: False,
    )
    foreign = environment_readiness._probe_ui_http(
        environment,
        processes=processes,
        timeout_seconds=1,
    )
    assert foreign["ok"] is False
    assert foreign["owner_matches"] is False


def test_readiness_macos_socket_owner_is_address_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(environment_readiness.sys, "platform", "darwin")

    def lsof(stdout: str):
        return subprocess.CompletedProcess(
            ["lsof"],
            0,
            stdout,
            "",
        )

    monkeypatch.setattr(
        environment_readiness.subprocess,
        "run",
        lambda *_args, **_kwargs: lsof("p42\nPIPv6\nn[::1]:8088\n"),
    )
    assert not environment_readiness._pid_owns_tcp_listener(
        42,
        "127.0.0.1",
        8088,
    )

    monkeypatch.setattr(
        environment_readiness.subprocess,
        "run",
        lambda *_args, **_kwargs: lsof("p42\nPIPv4\nn127.0.0.1:8088\n"),
    )
    assert environment_readiness._pid_owns_tcp_listener(
        42,
        "127.0.0.1",
        8088,
    )


def test_readiness_linux_socket_owner_is_address_scoped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(environment_readiness.sys, "platform", "linux")
    proc_root = tmp_path / "proc"
    net = proc_root / "net"
    net.mkdir(parents=True)
    (net / "tcp").write_text(
        "sl local_address rem_address st tx_queue tr tm->when retrnsmt uid timeout inode\n"
        "0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 "
        "00:00000000 00000000 1000 0 12345\n",
        encoding="utf-8",
    )
    fd_root = proc_root / "42/fd"
    fd_root.mkdir(parents=True)
    (fd_root / "7").symlink_to("socket:[12345]")
    (fd_root / "8").symlink_to("pipe:[67890]")

    original_readlink = Path.readlink

    def readlink_with_transient_fd(path: Path) -> Path:
        if path.name == "8":
            raise FileNotFoundError(path)
        return original_readlink(path)

    monkeypatch.setattr(Path, "readlink", readlink_with_transient_fd)

    assert environment_readiness._pid_owns_tcp_listener(
        42,
        "127.0.0.1",
        8080,
        proc_root=proc_root,
    )
    assert not environment_readiness._pid_owns_tcp_listener(
        42,
        "127.0.0.2",
        8080,
        proc_root=proc_root,
    )

    def readlink_with_permission_error(path: Path) -> Path:
        if path.name == "7":
            raise PermissionError(path)
        return original_readlink(path)

    monkeypatch.setattr(Path, "readlink", readlink_with_permission_error)
    assert not environment_readiness._pid_owns_tcp_listener(
        42,
        "127.0.0.1",
        8080,
        proc_root=proc_root,
    )
