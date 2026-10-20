"""Docker-preflight triage in the academic-researcher installer.

Per `fix-docker-preflight-misdiagnosis`: `docker info` failing does
NOT mean the daemon is down. On Linux the common first-install
failure is the operator missing from the `docker` group (permission
denied on the socket) — observed live on a PI's first install,
2026-06-09. The preflight must say so instead of "daemon not
running", which sends the operator to restart a daemon that is
already up.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from unittest import mock

from okg.substrate.cli.profile_cli_loader import get_profile_cli

_install = get_profile_cli("academic-researcher").install


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        deployment_name="demo",
        orcid=None,
        use_local_postgres=None,
        postgres_port=None,
        postgres_dsn=None,
        reset=False,
    )


def _run_preflight_with_stderr(stderr: bytes, capsys) -> dict:
    err = subprocess.CalledProcessError(
        1, ["docker", "info"], output=b"", stderr=stderr,
    )
    with (
        mock.patch.object(
            _install.shutil, "which", return_value="/usr/bin/docker",
        ),
        mock.patch.object(
            _install.subprocess, "run", side_effect=err,
        ),
    ):
        rc = _install._provision_postgres_and_scaffold(
            _args(), as_json=True,
        )
    assert rc != 0
    return json.loads(capsys.readouterr().out)


def test_permission_denied_diagnosed_as_socket_access(capsys) -> None:
    out = _run_preflight_with_stderr(
        b"permission denied while trying to connect to the Docker "
        b"daemon socket at unix:///var/run/docker.sock",
        capsys,
    )
    msg = out["error"]
    assert "usermod -aG docker" in msg
    assert "log out" in msg.lower()
    assert "not running" not in msg.lower()


def test_missing_compose_plugin_diagnosed_at_preflight(capsys) -> None:
    """Compose v2 ships separately from the docker CLI on most
    distros; without the preflight check the install dies mid-build
    with the bare CLI's "unknown shorthand flag: 'd' in -d"."""
    responses = iter([
        mock.Mock(returncode=0),  # docker info — daemon fine
        mock.Mock(returncode=1, stderr=b"", stdout=b""),  # compose probe
    ])
    with (
        mock.patch.object(
            _install.shutil, "which", return_value="/usr/bin/docker",
        ),
        mock.patch.object(
            _install.subprocess, "run",
            side_effect=lambda *a, **k: next(responses),
        ),
    ):
        rc = _install._provision_postgres_and_scaffold(
            _args(), as_json=True,
        )
    assert rc != 0
    msg = json.loads(capsys.readouterr().out)["error"]
    assert "Compose v2 plugin is missing" in msg
    assert "docker-compose-v2" in msg
    assert "docker-compose-plugin" in msg


def test_daemon_unreachable_gets_platform_hint(capsys) -> None:
    out = _run_preflight_with_stderr(
        b"Cannot connect to the Docker daemon at "
        b"unix:///var/run/docker.sock. Is the docker daemon running?",
        capsys,
    )
    msg = out["error"]
    assert "daemon not reachable" in msg
    # Platform-appropriate guidance, never the wrong platform's only.
    assert ("systemctl enable --now docker" in msg) or (
        "Docker Desktop" in msg
    )
