"""`okg --version` reports the installed package version (issue #2275).

The top-level parser had only `--help` and `--json`, and no test pinned the
top-level flag set, so `okg --version` exited 2. These tests pin the flag
through the real entry point, in-process and as a subprocess.
"""
from __future__ import annotations

import importlib.metadata
import subprocess
import sys

import pytest

from okg.substrate.cli.main import main


def test_version_flag_prints_installed_metadata_version(capsys) -> None:
    expected = importlib.metadata.version("okg")
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert captured.out == f"okg {expected}\n"
    assert captured.err == ""


def test_version_flag_subprocess_exits_zero() -> None:
    expected = importlib.metadata.version("okg")
    result = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main", "--version"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"okg {expected}"


def test_version_flag_refuses_when_metadata_missing(monkeypatch, capsys) -> None:
    def _missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", _missing)
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "version unknown" in captured.err
