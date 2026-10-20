"""Tests for the `_build_okg_pg17_image` helper.

Per `streamline-academic-pi-install` Phase 3: `pi-bootstrap` SHALL
auto-build the substrate's Postgres image when absent rather than
refusing.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest import mock


from okg.substrate.cli.profile_cli_loader import get_profile_cli

_build_okg_pg17_image = (
    get_profile_cli("academic-researcher").install._build_okg_pg17_image
)


def test_build_invokes_docker_compose_up_build(tmp_path: Path) -> None:
    """The helper SHALL invoke `docker compose up -d --build` in
    the `ops/pg/` directory.
    """
    # Synthesize a fake repo with the expected compose.yaml.
    (tmp_path / "ops/pg").mkdir(parents=True)
    (tmp_path / "ops/pg/compose.yaml").write_text("services: {}\n")

    with mock.patch("subprocess.run") as fake_run:
        fake_run.return_value = mock.Mock(returncode=0)
        result = _build_okg_pg17_image(tmp_path)

    assert result is None
    fake_run.assert_called_once()
    call_args, call_kwargs = fake_run.call_args
    assert call_args[0] == [
        "docker", "compose", "up", "-d", "--build",
    ]
    assert call_kwargs.get("cwd") == tmp_path / "ops/pg"
    assert call_kwargs.get("check") is True


def test_build_returns_message_when_compose_yaml_missing(
    tmp_path: Path,
) -> None:
    """If `ops/pg/compose.yaml` is missing, the helper SHALL NOT
    invoke docker and SHALL return a structured failure message.
    """
    with mock.patch("subprocess.run") as fake_run:
        result = _build_okg_pg17_image(tmp_path)
    assert result is not None
    assert "compose.yaml" in result
    fake_run.assert_not_called()


def test_build_returns_message_on_docker_failure(
    tmp_path: Path,
) -> None:
    """A non-zero exit from `docker compose up` SHALL surface a
    failure message that points operators at the manual build
    command (so they can see the full output).
    """
    (tmp_path / "ops/pg").mkdir(parents=True)
    (tmp_path / "ops/pg/compose.yaml").write_text("services: {}\n")

    def _raise(*args, **kwargs):
        raise subprocess.CalledProcessError(2, args[0])

    with mock.patch("subprocess.run", side_effect=_raise):
        result = _build_okg_pg17_image(tmp_path)
    assert result is not None
    assert "rc=2" in result
    assert "docker compose up -d --build" in result


def test_build_accepts_shipped_docker_compose_yaml(
    tmp_path: Path,
) -> None:
    """Regression: the substrate ships `ops/pg/docker-compose.yaml`,
    not `ops/pg/compose.yaml`. A fresh first-install (no pre-built
    `okg-pg17:local`) SHALL find the shipped build context and invoke
    docker, instead of aborting with "compose.yaml not found".

    Before the fix the guard checked only `compose.yaml`, so this
    case bailed without ever calling docker — the cold-install bug.
    """
    (tmp_path / "ops/pg").mkdir(parents=True)
    (tmp_path / "ops/pg/docker-compose.yaml").write_text("services: {}\n")

    with mock.patch("subprocess.run") as fake_run:
        fake_run.return_value = mock.Mock(returncode=0)
        result = _build_okg_pg17_image(tmp_path)

    assert result is None
    fake_run.assert_called_once()
    call_args, _ = fake_run.call_args
    assert call_args[0] == ["docker", "compose", "up", "-d", "--build"]
