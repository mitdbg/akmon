"""Deployment readiness doctor command selection."""
from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

from okg.substrate.cli import deployment as deployment_cli
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli import doctor as doctor_cli


def test_doctor_loads_readiness_dsn_from_owner_only_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "doctor.dsn"
    path.write_text("postgres://operator:secret@staging/graph", encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setenv("OKG_READY_DOCTOR_DSN_FILE", str(path))
    args = SimpleNamespace(dsn=None)

    doctor_cli._load_ready_doctor_dsn(args)

    assert args.dsn == "postgres://operator:secret@staging/graph"
    assert "OKG_READY_DOCTOR_DSN_FILE" not in doctor_cli.os.environ


def test_ready_doctor_uses_plain_deployment_doctor_without_custom_check(
    monkeypatch,
) -> None:
    seen: dict[str, list[str]] = {}

    monkeypatch.setattr(
        "okg.substrate.doctor.load_deployment_doctor_checks",
        lambda deployment: [],
    )

    def _run(command, **kwargs):
        seen["command"] = command
        seen["dsn"] = Path(
            kwargs["env"]["OKG_READY_DOCTOR_DSN_FILE"]
        ).read_text(encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout='{"ok": true}', stderr="")

    monkeypatch.setattr(subprocess, "run", _run)

    status = deployment_cli._ready_doctor_status(
        "cern-twiki",
        dsn="postgres://secret@staging/graph",
        skip=False,
    )

    assert status["ok"] is True
    # Issue #2301: the running okg with -P, whatever the prefix length.
    prefix = running_okg_argv()
    assert seen["command"][: len(prefix)] == prefix
    assert seen["command"][len(prefix):] == [
        "doctor",
        "--deployment",
        "cern-twiki",
        "--json",
    ]
    assert seen["dsn"] == "postgres://secret@staging/graph"
    assert "secret" not in " ".join(status["command"])


def test_ready_doctor_uses_same_named_custom_check_when_registered(
    monkeypatch,
) -> None:
    seen: dict[str, list[str]] = {}

    monkeypatch.setattr(
        "okg.substrate.doctor.load_deployment_doctor_checks",
        lambda deployment: [SimpleNamespace(name="okg-workspace")],
    )

    def _run(command, **kwargs):
        seen["command"] = command
        seen["dsn"] = Path(
            kwargs["env"]["OKG_READY_DOCTOR_DSN_FILE"]
        ).read_text(encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout='{"ok": true}', stderr="")

    monkeypatch.setattr(subprocess, "run", _run)

    status = deployment_cli._ready_doctor_status(
        "okg-workspace",
        dsn="postgres://secret@staging/graph",
        skip=False,
    )

    assert status["ok"] is True
    prefix = running_okg_argv()
    assert seen["command"][: len(prefix)] == prefix
    # `--check` goes right after `doctor`, not at a fixed index.
    assert seen["command"].index("--check") == len(prefix) + 1
    assert seen["command"][len(prefix):] == [
        "doctor",
        "--check",
        "okg-workspace",
        "--deployment",
        "okg-workspace",
        "--json",
    ]
    assert seen["dsn"] == "postgres://secret@staging/graph"
    assert "secret" not in " ".join(status["command"])
