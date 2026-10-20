from __future__ import annotations

import json
from pathlib import Path
import plistlib

import pytest

from okg.substrate.cli.main import main
from tests.mirror_scope import private_path


@pytest.fixture(autouse=True)
def _no_live_docker(monkeypatch):
    """Keep these tests hermetic: never shell out to a real Docker daemon.

    The compose identity guard probes Docker; the default here reports the
    daemon as unavailable, and the guard tests override this stub.
    """
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._docker_inspect_labels",
        lambda *, kind, name: {
            "probe": "docker_unavailable", "present": None, "labels": {},
        },
    )


def _write_deployment_manifest(
    root: Path,
    deployment: str,
    dsn: str,
) -> None:
    deployment_dir = root / "deployments" / deployment
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        f"name: {deployment}\npostgres:\n  dsn: {dsn}\n",
        encoding="utf-8",
    )


def test_dogfood_service_render_launchd_adapter(capsys) -> None:
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "launchd",
        "--repo-root",
        "/tmp/okg",
        "--okg-bin",
        "/tmp/okg/.venv/bin/okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "okg.dogfood.service_adapter.v1"
    assert payload["adapter"] == "launchd"
    assert payload["filename"] == "org.okg.runtime-worker.okg-workspace.plist"
    assert "org.okg.runtime-worker.okg-workspace" in payload["content"]
    assert "<string>/tmp/okg/.venv/bin/okg</string>" in payload["content"]
    assert "<string>runtime</string>" in payload["content"]
    assert "<string>worker</string>" in payload["content"]
    assert "<string>okg-workspace</string>" in payload["content"]
    assert "<string>${OKG_DSN}</string>" in payload["content"]
    assert "<key>OKG_IVM_INTERVAL_VIEW</key>" in payload["content"]
    plist = plistlib.loads(payload["content"].encode())
    assert (
        plist["EnvironmentVariables"]["OKG_IVM_CONSOLE_COUNT_ROLLUP"] == "1"
    )
    assert plist["ExitTimeOut"] == 120
    assert "<string>1</string>" in payload["content"]
    assert "launchctl bootstrap" in " ".join(payload["install_commands"])


def test_dogfood_service_render_launchd_resolves_bare_uv(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    uv_bin = tmp_path / "uv"
    uv_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    uv_bin.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))

    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "launchd",
        "--repo-root",
        "/tmp/okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert f"<string>{uv_bin}</string>" in payload["content"]
    assert "<string>--module</string>" in payload["content"]
    assert "<string>okg.substrate.cli.main</string>" in payload["content"]
    assert "<string>okg</string>" not in payload["content"]


def test_dogfood_service_install_launchd_requires_concrete_dsn(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    output = tmp_path / "org.okg.runtime-worker.okg-workspace.plist"

    rc = main([
        "dogfood",
        "service",
        "install",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "launchd",
        "--repo-root",
        "/tmp/okg",
        "--output",
        str(output),
        "--apply",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert "does not expand DSN placeholders" in payload["error"]
    assert payload["written"] is None
    assert not output.exists()


def test_dogfood_service_install_resolves_env_dsn_without_echoing_secret(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    output = tmp_path / "org.okg.runtime-worker.okg-workspace.plist"
    concrete_dsn = "postgresql://operator:top-secret@localhost/okg_workspace"
    monkeypatch.setenv("OKG_DEPLOYMENT_APPLY_DSN", concrete_dsn)

    rc = main([
        "dogfood",
        "service",
        "install",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "launchd",
        "--repo-root",
        "/tmp/okg",
        "--dsn",
        "${OKG_DEPLOYMENT_APPLY_DSN}",
        "--output",
        str(output),
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload_text = capsys.readouterr().out
    payload = json.loads(payload_text)
    assert payload["ok"] is True
    assert payload["written"] == str(output)
    assert "content" not in payload
    assert payload["content_sha256"].startswith("sha256:")
    assert concrete_dsn not in payload_text
    plist = plistlib.loads(output.read_bytes())
    assert plist["EnvironmentVariables"]["OKG_DSN"] == concrete_dsn


def test_dogfood_service_render_launchd_uses_deployment_scoped_dsn_env(
    tmp_path: Path,
    capsys,
) -> None:
    _write_deployment_manifest(tmp_path, "cooper", "${COOPER_OKG_DSN}")
    concrete_dsn = "postgresql://operator:secret@localhost/cooper"

    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "cooper",
        "--adapter",
        "launchd",
        "--repo-root",
        str(tmp_path),
        "--dsn",
        concrete_dsn,
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    plist = plistlib.loads(payload["content"].encode())
    assert plist["EnvironmentVariables"]["COOPER_OKG_DSN"] == concrete_dsn
    assert "OKG_DSN" not in plist["EnvironmentVariables"]


def test_dogfood_service_render_launchd_rejects_component_dsn_refs_without_leak(
    tmp_path: Path,
    capsys,
) -> None:
    _write_deployment_manifest(
        tmp_path,
        "cooper",
        "postgresql://${DB_USER}:${DB_PASSWORD}@localhost/cooper",
    )
    concrete_dsn = "postgresql://operator:top-secret@localhost/cooper"

    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "cooper",
        "--adapter",
        "launchd",
        "--repo-root",
        str(tmp_path),
        "--dsn",
        concrete_dsn,
        "--json",
    ])

    captured = capsys.readouterr()
    assert rc == 1
    assert "single whole-value" in captured.err
    assert concrete_dsn not in captured.err
    assert concrete_dsn not in captured.out


def test_dogfood_service_install_launchd_rejects_any_unresolved_dsn_env(
    tmp_path: Path,
    capsys,
) -> None:
    _write_deployment_manifest(tmp_path, "cooper", "${COOPER_OKG_DSN}")
    output = tmp_path / "cooper.plist"

    rc = main([
        "dogfood",
        "service",
        "install",
        "--deployment",
        "cooper",
        "--adapter",
        "launchd",
        "--repo-root",
        str(tmp_path),
        "--dsn",
        "${COOPER_OKG_DSN}",
        "--output",
        str(output),
        "--apply",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["written"] is None
    assert not output.exists()


def test_dogfood_service_install_launchd_accepts_dollar_in_literal_dsn(
    tmp_path: Path,
    capsys,
) -> None:
    _write_deployment_manifest(tmp_path, "cooper", "postgresql://literal/cooper")
    output = tmp_path / "cooper.plist"
    concrete_dsn = "postgresql://operator:pa$word@localhost/cooper"

    rc = main([
        "dogfood",
        "service",
        "install",
        "--deployment",
        "cooper",
        "--adapter",
        "launchd",
        "--repo-root",
        str(tmp_path),
        "--dsn",
        concrete_dsn,
        "--output",
        str(output),
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    plist = plistlib.loads(output.read_bytes())
    assert plist["EnvironmentVariables"]["OKG_DSN"] == concrete_dsn


def test_dogfood_service_render_systemd_adapter(capsys) -> None:
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "systemd",
        "--repo-root",
        "/opt/okg",
        "--okg-bin",
        "/opt/okg/.venv/bin/okg",
        "--env-file",
        "/etc/okg/okg-workspace.env",
        "--user",
        "okg",
        "--group",
        "okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["adapter"] == "systemd"
    assert payload["filename"] == "okg-runtime-worker@.service"
    assert "WorkingDirectory=/opt/okg" in payload["content"]
    assert "EnvironmentFile=/etc/okg/okg-workspace.env" in payload["content"]
    assert 'Environment="OKG_IVM_CONSOLE_COUNT_ROLLUP=1"' in payload["content"]
    assert (
        "ExecStart=/usr/bin/env OKG_IVM_CONSOLE_COUNT_ROLLUP=1 "
        "/opt/okg/.venv/bin/okg runtime worker --deployment %i"
        in payload["content"]
    )
    assert "TimeoutStopSec=120" in payload["content"]
    assert "systemctl enable --now okg-runtime-worker@okg-workspace.service" in (
        " ".join(payload["install_commands"])
    )


def test_dogfood_service_render_uses_the_declared_database(
    tmp_path: Path,
    capsys,
) -> None:
    """The render reads the database the manifest declares (a synthetic one here).

    What happens when no manifest declares one -- the render derives
    ``okg_<name>`` -- is tracked in #2823.
    """
    deployment_dir = tmp_path / "deployments" / "dogfood-demo"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "name: dogfood-demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: dogfood_demo\n",
        encoding="utf-8",
    )
    rc = main([
        "dogfood", "service", "render",
        "--deployment", "dogfood-demo",
        "--adapter", "docker-compose",
        "--repo-root", str(tmp_path),
        "--okg-bin", "uv run okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filename"] == "docker-compose.dogfood-demo.dogfood.yaml"
    assert payload["postgres"]["dedicated"] is True
    assert payload["postgres"]["host_dsn"] == (
        "postgres://postgres:okg@localhost:55434/dogfood_demo"
    )
    assert payload["postgres"]["service_dsn"] == (
        "postgres://postgres:okg@postgres:5432/dogfood_demo"
    )


def test_dogfood_service_render_docker_compose_includes_dedicated_postgres(
    capsys,
) -> None:
    # okg-workspace is this repository's own deployment; the public copy
    # skips this test (tests/mirror_scope.py) and runs the synthetic one above.
    private_path("deployments/okg-workspace/deployment.yaml")
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "docker-compose",
        "--repo-root",
        "/tmp/okg",
        "--okg-bin",
        "uv run okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["adapter"] == "docker-compose"
    assert payload["filename"] == "docker-compose.okg-workspace.dogfood.yaml"
    assert payload["postgres"]["dedicated"] is True
    assert payload["postgres"]["container_name"] == (
        "okg-okg-workspace-dogfood-postgres"
    )
    assert payload["postgres"]["volume_name"] == (
        "okg-okg-workspace-dogfood-postgres-data"
    )
    assert payload["postgres"]["host_port"] == 55434
    assert payload["postgres"]["host_dsn"] == (
        "postgres://postgres:okg@localhost:55434/okg_workspace"
    )
    assert payload["postgres"]["service_dsn"] == (
        "postgres://postgres:okg@postgres:5432/okg_workspace"
    )
    assert payload["postgres"]["max_worker_processes"] == 32
    assert payload["postgres"]["timescaledb_max_background_workers"] == 8
    assert "services:\n  postgres:" in payload["content"]
    assert "container_name: okg-okg-workspace-dogfood-postgres" in (
        payload["content"]
    )
    assert "mem_limit: 6g" in payload["content"]
    assert "shm_size: 2gb" in payload["content"]
    assert '"max_worker_processes=32"' in payload["content"]
    assert '"timescaledb.max_background_workers=8"' in payload["content"]
    assert "depends_on:\n      postgres:\n        condition: service_healthy" in (
        payload["content"]
    )
    assert (
        'OKG_DSN: "postgres://postgres:okg@postgres:5432/okg_workspace"'
        in payload["content"]
    )
    assert "/tmp/okg" not in payload["content"]
    # With no --output the descriptor goes to the deployment's own directory,
    # so every repository path in it climbs two levels back to the root.
    # (Compare the tail: the repo root may resolve through a symlink, such as
    # macOS /tmp -> /private/tmp.)
    assert Path(payload["destination"]).parts[-4:] == (
        "okg",
        "deployments",
        "okg-workspace",
        "docker-compose.okg-workspace.dogfood.yaml",
    )
    assert "context: ../../ops/pg" in payload["content"]
    assert (
        "- ../../ops/pg/initdb:/docker-entrypoint-initdb.d:ro"
        in payload["content"]
    )
    assert "- ../..:/workspace" in payload["content"]
    assert "context: ./ops/pg" not in payload["content"]
    assert "- .:/workspace" not in payload["content"]
    assert 'OKG_IVM_INTERVAL_VIEW: "1"' in payload["content"]
    assert 'OKG_IVM_CONSOLE_COUNT_ROLLUP: "1"' in payload["content"]
    assert "stop_grace_period: 120s" in payload["content"]
    assert "name: okg-okg-workspace-dogfood-postgres-data" in payload["content"]


def test_render_compose_sets_lock_and_memory_sizing(capsys) -> None:
    """A rendered dogfood Postgres must be sized for a large graph DB.

    Regression for fix-dogfood-postgres-lock-and-memory-sizing: the generator
    used to emit only max_connections / max_worker_processes / timescaledb
    workers, leaving shared_buffers at the 128MB initdb default and
    max_locks_per_transaction at 64 (a 6400-slot shared lock table). Under
    large concurrent publishes that lock table exhausted ('out of shared
    memory') and crashed the okg-workspace dogfood DB on 2026-07-23.
    """
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "fixture",
        "--postgres-db",
        "okg_fixture",
        "--adapter",
        "docker-compose",
        "--repo-root",
        "/tmp/okg",
        "--okg-bin",
        "uv run okg",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)

    # Sized defaults surface both in the structured postgres summary and in the
    # rendered compose command that actually starts Postgres.
    assert payload["postgres"]["shared_buffers"] == "512MB"
    # The ingest worker (up to 52 connections) plus several agent-session
    # tool servers (up to 16 each) exhaust the stock 100 connection slots.
    assert payload["postgres"]["max_connections"] == 200
    assert '"max_connections=200"' in payload["content"]
    assert payload["postgres"]["max_locks_per_transaction"] == 1024
    assert payload["postgres"]["maintenance_work_mem"] == "256MB"
    assert '"shared_buffers=512MB"' in payload["content"]
    assert '"max_locks_per_transaction=1024"' in payload["content"]
    assert '"maintenance_work_mem=256MB"' in payload["content"]


def test_render_compose_sizing_flags_flow_through(capsys) -> None:
    """Operator overrides for the sizing knobs reach the rendered command."""
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "fixture",
        "--postgres-db",
        "okg_fixture",
        "--adapter",
        "docker-compose",
        "--repo-root",
        "/tmp/okg",
        "--okg-bin",
        "uv run okg",
        "--postgres-shared-buffers",
        "1GB",
        "--postgres-max-locks-per-transaction",
        "2048",
        "--postgres-maintenance-work-mem",
        "512MB",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["postgres"]["shared_buffers"] == "1GB"
    assert payload["postgres"]["max_locks_per_transaction"] == 2048
    assert payload["postgres"]["maintenance_work_mem"] == "512MB"
    assert '"shared_buffers=1GB"' in payload["content"]
    assert '"max_locks_per_transaction=2048"' in payload["content"]
    assert '"maintenance_work_mem=512MB"' in payload["content"]


def test_render_compose_rejects_lock_knob_below_floor(capsys) -> None:
    """The lock knob must not be sized below the Postgres 64 default.

    A value below the floor would re-introduce the 6400-slot lock table that
    exhausted under concurrent publishes (the crash this change fixes), so the
    generator refuses it loudly rather than rendering an under-provisioned DB.
    """
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "fixture",
        "--postgres-db",
        "okg_fixture",
        "--adapter",
        "docker-compose",
        "--repo-root",
        "/tmp/okg",
        "--okg-bin",
        "uv run okg",
        "--postgres-max-locks-per-transaction",
        "32",
        "--json",
    ])
    assert rc != 0
    captured = capsys.readouterr()
    assert "postgres-max-locks-per-transaction must be >= 64" in (
        captured.out + captured.err
    )


def test_dogfood_service_install_is_dry_run_until_apply(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "worker.service"

    rc = main([
        "dogfood",
        "service",
        "install",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "systemd",
        "--repo-root",
        str(tmp_path),
        "--output",
        str(output),
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] is False
    assert payload["written"] is None
    assert not output.exists()

    rc = main([
        "dogfood",
        "service",
        "install",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "systemd",
        "--repo-root",
        str(tmp_path),
        "--output",
        str(output),
        "--apply",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] is True
    assert payload["written"] == str(output)
    assert output.read_text(encoding="utf-8").startswith("[Unit]\n")


def _render_compose(root, capsys, *, deployment="fixture", port=None):
    import yaml

    # The fixture deployments have no manifest, so the database is named
    # explicitly: a render never derives one from the deployment name.
    argv = [
        "dogfood", "service", "render", "--deployment", deployment,
        "--adapter", "docker-compose", "--repo-root", str(root),
        "--postgres-db", f"okg_{deployment}", "--json",
    ]
    if port is not None:
        argv += ["--postgres-host-port", str(port)]
    assert main(argv) == 0
    payload = json.loads(capsys.readouterr().out)
    return payload, yaml.safe_load(payload["content"])


def test_compose_render_refuses_a_deployment_that_declares_no_database(
    tmp_path, capsys, monkeypatch,
):
    """No manifest declares a database and none is passed: refuse, never derive.

    The render used to fall back to ``okg_<deployment>`` and report success,
    so a mistyped deployment (or one this checkout does not carry, as in the
    public copy) produced a descriptor for a database nobody declared.
    """
    monkeypatch.setattr(
        "okg.substrate.deployment_authority._declared_database", lambda _name: None
    )
    output = tmp_path / "compose.yaml"

    rc = main([
        "dogfood", "service", "install", "--deployment", "undeclared",
        "--adapter", "docker-compose", "--repo-root", str(tmp_path),
        "--output", str(output), "--apply", "--json",
    ])
    captured = capsys.readouterr()
    error = json.loads(captured.err or captured.out)["error"]

    assert rc != 0
    assert error["message"] == "no database is declared for deployment 'undeclared'"
    assert error["data"]["manifest_path"].endswith(
        "deployments/undeclared/deployment.yaml"
    )
    assert "--postgres-db" in error["fix_hint"]
    assert "okg_undeclared" not in captured.out + captured.err
    assert not output.exists()


def test_compose_render_uses_the_database_the_manifest_declares(tmp_path, capsys):
    manifest = tmp_path / "deployments" / "declared" / "deployment.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        "name: declared\npostgres:\n  database: declared_graph\n", encoding="utf-8"
    )

    assert main([
        "dogfood", "service", "render", "--deployment", "declared",
        "--adapter", "docker-compose", "--repo-root", str(tmp_path), "--json",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert 'POSTGRES_DB: "declared_graph"' in payload["content"]


def test_compose_project_is_keyed_on_stack_data_identity(tmp_path, capsys):
    """The project label identifies the STACK, not the checkout that rendered it."""
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    for root in (first_root, second_root):
        root.mkdir()

    _, first = _render_compose(first_root, capsys)
    _, second = _render_compose(second_root, capsys)
    _, other_deployment = _render_compose(first_root, capsys, deployment="other")
    _, other_port = _render_compose(first_root, capsys, port=55999)

    # Two checkouts of one deployment share one volume, one container name and
    # one host port: they are one stack and must render one project, or
    # `down`/`restart` silently match nothing from the second checkout.
    assert first["name"] == second["name"]
    # The colliding legacy label from issue #1602 is never rendered again.
    assert first["name"] != "okg-dogfood-fixture"
    assert first["name"].startswith("okg-dogfood-fixture-")
    # Different data => different project.
    assert other_deployment["name"] != first["name"]
    assert other_port["name"] != first["name"]
    # No absolute path may leak into a tracked descriptor.
    assert str(tmp_path) not in json.dumps(first)

    # Compose adopts by project/service, including stopped containers. A
    # foreign deployment must never enter the selected set for our Postgres.
    containers = [
        {"id": str(index), "labels": {
            "com.docker.compose.project": config["name"],
            "com.docker.compose.service": "postgres",
        }}
        for index, config in enumerate([first, other_deployment])
    ]
    selected = [row["id"] for row in containers if row["labels"] == {
        "com.docker.compose.project": first["name"],
        "com.docker.compose.service": "postgres",
    }]
    assert selected == ["0"]


def _stub_docker(monkeypatch, *, container_labels=None, volume_labels=None):
    def _inspect(*, kind, name):
        labels = container_labels if kind == "container" else volume_labels
        if labels is None:
            return {"probe": "ok", "present": False, "labels": {}}
        return {"probe": "ok", "present": True, "labels": labels}

    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._docker_inspect_labels", _inspect,
    )


def test_compose_render_refuses_a_foreign_project_label(tmp_path, capsys, monkeypatch):
    """A stale/foreign project label on our container refuses the mutating verbs."""
    _stub_docker(monkeypatch, container_labels={
        "com.docker.compose.project": "okg-dogfood-fixture",
        "com.docker.compose.service": "postgres",
    })

    rc = main([
        "dogfood", "service", "render", "--deployment", "fixture",
        "--adapter", "docker-compose", "--repo-root", str(tmp_path),
        "--postgres-db", "okg_fixture", "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.err or captured.out)

    assert rc == 1
    assert payload["ok"] is False
    assert payload["status"] == "compose_project_label_conflict"
    assert "okg-dogfood-fixture" in payload["error"]
    assert payload["control_commands"] == {}
    assert payload["install_commands"] == []
    conflict = payload["compose_identity"]["conflicts"][0]
    assert conflict["kind"] == "container"
    assert conflict["name"] == "okg-fixture-dogfood-postgres"
    assert conflict["observed_project"] == "okg-dogfood-fixture"
    assert payload["reconcile_commands"][0] == (
        "docker rm -f okg-fixture-dogfood-postgres"
    )
    assert payload["reconcile_commands"][-1].endswith("up -d --no-deps postgres")


def test_compose_install_refuses_before_writing_under_a_conflict(
    tmp_path, capsys, monkeypatch,
):
    """`install --apply` refuses too: it precedes `up` and publishes authority."""
    _stub_docker(monkeypatch, volume_labels={
        "com.docker.compose.project": "some-other-stack",
    })
    output = tmp_path / "compose.yaml"

    rc = main([
        "dogfood", "service", "install", "--deployment", "fixture",
        "--adapter", "docker-compose", "--repo-root", str(tmp_path),
        "--postgres-db", "okg_fixture",
        "--output", str(output), "--apply", "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.err or captured.out)

    assert rc == 1
    assert payload["status"] == "compose_project_label_conflict"
    assert payload["written"] is None
    assert not output.exists()
    assert payload["compose_identity"]["conflicts"][0]["kind"] == "volume"
    assert "docker volume inspect" in payload["reconcile_commands"][0]


def test_compose_identity_probe_reports_absent_docker(tmp_path, capsys, monkeypatch):
    """Absence of Docker is reported, never reported as a clean stack."""
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._docker_inspect_labels",
        lambda *, kind, name: {
            "probe": "docker_unavailable", "present": None, "labels": {},
        },
    )

    payload, _ = _render_compose(tmp_path, capsys)

    assert payload["compose_identity"]["ok"] is True
    assert payload["compose_identity"]["container"]["probe"] == "docker_unavailable"
    assert payload["compose_identity"]["conflicts"] == []
