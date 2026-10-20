"""Exact consumed-snapshot identity for InterfaceSource."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    tokens_equal,
)
from okg.substrate.library.sources.code_graph_interface import InterfaceSource


def _git_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "OKG Test"],
        check=True,
    )
    app = repo / "app.py"
    app.write_text(
        "from fastapi import FastAPI\n"
        "app = FastAPI()\n"
        "@app.get('/before')\n"
        "def before():\n"
        "    return {'ok': True}\n"
    )
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "fixture"],
        check=True,
    )
    return repo, app


def test_interface_returns_exact_global_git_snapshot(tmp_path: Path) -> None:
    repo, app = _git_repo(tmp_path)
    app.write_text(
        "from fastapi import FastAPI\n"
        "app = FastAPI()\n"
        "@app.get('/after')\n"
        "def after():\n"
        "    return {'ok': True}\n"
    )
    source = InterfaceSource(repo="demo", repo_root=repo)

    run = source.run("run-exact", mode="scope_complete")
    facts = list(run.facts)

    assert source.probe_short_circuit_safe is True
    assert any(fact.attrs.get("route") == "/after" for fact in facts)
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.applied_raw_token is not None
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert tokens_equal(
        run.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_interface_mutation_during_capture_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, app = _git_repo(tmp_path)
    source = InterfaceSource(repo="demo", repo_root=repo)
    original_read_bytes = Path.read_bytes
    mutated = False

    def _read_then_mutate(path: Path) -> bytes:
        nonlocal mutated
        raw_bytes = original_read_bytes(path)
        if path == app and not mutated:
            mutated = True
            app.write_text("@changed.post('/race')\ndef race():\n    return None\n")
        return raw_bytes

    monkeypatch.setattr(Path, "read_bytes", _read_then_mutate)

    with pytest.raises(ChangeProbeError, match="input changed during snapshot"):
        source.run("run-race", mode="scope_complete")

    assert mutated is True


def test_interface_read_error_does_not_acknowledge_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, app = _git_repo(tmp_path)
    source = InterfaceSource(repo="demo", repo_root=repo)
    original_read_bytes = Path.read_bytes

    def _unreadable(path: Path) -> bytes:
        if path == app:
            raise PermissionError("sensitive operating-system detail")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", _unreadable)

    with pytest.raises(ChangeProbeError, match="could not read app.py") as exc_info:
        source.run("run-error", mode="scope_complete")

    assert "sensitive operating-system detail" not in str(exc_info.value)


def test_interface_non_git_direct_run_is_conservative(tmp_path: Path) -> None:
    (tmp_path / "app.py").write_text(
        "@app.get('/direct')\ndef direct():\n    return None\n"
    )
    source = InterfaceSource(repo="demo", repo_root=tmp_path)

    run = source.run("run-non-git", mode="scope_complete")

    assert source.probe_short_circuit_safe is False
    assert list(run.facts)
    assert run.applied_raw_token is None
    assert run.applied_token_scope == "unavailable"
    assert run.applied_token_authority == "unavailable"
