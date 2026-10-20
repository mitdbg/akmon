"""Tests for substrate-wide lifecycle verbs `okg start/stop/
restart` (per `dead-simple-install`).

Strategy: stub `docker compose` subprocess calls; verify the
verbs read `deployment.yaml.docker.{compose_path, project_name}`
correctly and translate to the right docker compose invocations.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest import mock

import pytest
import yaml

from okg.substrate.cli import lifecycle


def _make_deployment(
    repo_root: Path, name: str, *, docker_block: dict | None = None,
) -> Path:
    """Build a minimal deployments/<name>/deployment.yaml."""
    dep_dir = repo_root / "deployments" / name
    dep_dir.mkdir(parents=True, exist_ok=True)
    payload: dict = {"name": name}
    if docker_block is not None:
        payload["docker"] = docker_block
    (dep_dir / "deployment.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
    )
    return dep_dir


@pytest.fixture
def temp_repo(tmp_path: Path, monkeypatch) -> Path:
    (tmp_path / "src" / "okg").mkdir(parents=True)
    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle._find_repo_root",
        lambda: tmp_path,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-state"))
    return tmp_path


@pytest.fixture
def stub_docker(monkeypatch):
    """Stub `subprocess.run` for docker calls. Returns a list
    that records every call so tests can assert."""
    calls: list[list[str]] = []

    def fake_run(cmd, *args, **kwargs):
        calls.append(list(cmd))
        return mock.MagicMock(returncode=0)

    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle.subprocess.run", fake_run,
    )
    # Make `shutil.which("docker")` succeed.
    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle.shutil.which",
        lambda _: "/usr/local/bin/docker",
    )
    return calls


# ---------- happy path ----------


def test_start_invokes_compose_up_with_correct_project(
    temp_repo: Path, stub_docker, capsys,
) -> None:
    _make_deployment(
        temp_repo, "demo",
        docker_block={
            "compose_path": "/path/to/compose.yaml",
            "project_name": "okg-demo",
            "host_port": 32773,
        },
    )
    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_start(args, as_json=True)
    assert rc == 0
    assert len(stub_docker) == 1
    cmd = stub_docker[0]
    assert cmd[:2] == ["docker", "compose"]
    assert "-p" in cmd
    assert "okg-demo" in cmd
    assert "up" in cmd
    assert "-d" in cmd
    assert "--wait" in cmd


def test_start_reads_user_state_docker_block_before_repo_fallback(
    tmp_path: Path, monkeypatch, stub_docker,
) -> None:
    """Lifecycle verbs SHALL use the resolved user-state deployment,
    not a direct repo/deployments/<name> construction."""
    repo_root = tmp_path / "repo"
    user_home = tmp_path / "user-home"
    (repo_root / "src" / "okg").mkdir(parents=True)
    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle._find_repo_root",
        lambda: repo_root,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))

    _make_deployment(
        user_home, "demo",
        docker_block={
            "compose_path": "/user-state/compose.yaml",
            "project_name": "okg-demo-user",
        },
    )
    _make_deployment(
        repo_root, "demo",
        docker_block={
            "compose_path": "/repo/compose.yaml",
            "project_name": "okg-demo-repo",
        },
    )

    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_start(args, as_json=True)

    assert rc == 0
    cmd = stub_docker[0]
    assert "/user-state/compose.yaml" in cmd
    assert "okg-demo-user" in cmd
    assert "/repo/compose.yaml" not in cmd
    assert "okg-demo-repo" not in cmd


def test_stop_invokes_compose_stop(
    temp_repo: Path, stub_docker,
) -> None:
    _make_deployment(
        temp_repo, "demo",
        docker_block={
            "compose_path": "/path/to/compose.yaml",
            "project_name": "okg-demo",
        },
    )
    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_stop(args, as_json=True)
    assert rc == 0
    assert stub_docker[0][-1] == "stop"


def test_restart_invokes_compose_up_force_recreate(
    temp_repo: Path, stub_docker,
) -> None:
    _make_deployment(
        temp_repo, "demo",
        docker_block={
            "compose_path": "/path/to/compose.yaml",
            "project_name": "okg-demo",
        },
    )
    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_restart(args, as_json=True)
    assert rc == 0
    assert "up" in stub_docker[0]
    assert "--force-recreate" in stub_docker[0]


# ---------- error paths ----------


def test_lifecycle_errors_when_no_docker_block(
    temp_repo: Path, stub_docker, capsys,
) -> None:
    """A deployment without a `docker:` block SHALL fail
    cleanly. No docker compose invocation."""
    _make_deployment(temp_repo, "demo", docker_block=None)
    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_start(args, as_json=True)
    assert rc == 1
    assert len(stub_docker) == 0  # docker not invoked
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["status"] == "no_docker_block"
    assert "demo" in payload["error"]


def test_lifecycle_errors_when_docker_not_on_path(
    temp_repo: Path, monkeypatch, capsys,
) -> None:
    _make_deployment(
        temp_repo, "demo",
        docker_block={
            "compose_path": "/x", "project_name": "okg-demo",
        },
    )
    # docker absent
    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle.shutil.which",
        lambda _: None,
    )
    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_start(args, as_json=True)
    assert rc == 1
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["status"] == "docker_not_on_path"


def test_lifecycle_propagates_docker_compose_nonzero_rc(
    temp_repo: Path, monkeypatch, capsys,
) -> None:
    _make_deployment(
        temp_repo, "demo",
        docker_block={
            "compose_path": "/x", "project_name": "okg-demo",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle.shutil.which",
        lambda _: "/usr/local/bin/docker",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.lifecycle.subprocess.run",
        lambda *a, **kw: mock.MagicMock(returncode=42),
    )
    args = argparse.Namespace(deployment="demo")
    rc = lifecycle.cmd_start(args, as_json=True)
    assert rc == 42
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["status"] == "start_failed"
    assert payload["returncode"] == 42


# ---------- idempotency ----------


def test_start_is_idempotent_via_docker_compose_up(
    temp_repo: Path, stub_docker,
) -> None:
    """`docker compose up -d --wait` is idempotent and reconciles
    the compose config. Two consecutive `okg start` calls SHALL
    both succeed."""
    _make_deployment(
        temp_repo, "demo",
        docker_block={
            "compose_path": "/x", "project_name": "okg-demo",
        },
    )
    args = argparse.Namespace(deployment="demo")
    assert lifecycle.cmd_start(args, as_json=True) == 0
    assert lifecycle.cmd_start(args, as_json=True) == 0
    assert len(stub_docker) == 2
