"""Exact consumed-snapshot identity for PackageManifestSource."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    tokens_equal,
)
from okg.substrate.library.sources.code_graph_manifest import (
    PackageManifestSource,
)


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
    manifest = repo / "pyproject.toml"
    manifest.write_text(
        "[project]\nname = \"demo\"\ndependencies = [\"pydantic>=2\"]\n"
    )
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "fixture"],
        check=True,
    )
    return repo, manifest


def test_package_manifest_returns_exact_global_git_snapshot(
    tmp_path: Path,
) -> None:
    repo, manifest = _git_repo(tmp_path)
    manifest.write_text(
        "[project]\nname = \"demo\"\ndependencies = [\"pydantic>=2\", \"mcp\"]\n"
    )
    source = PackageManifestSource(repo="demo", repo_root=repo)

    run = source.run("run-exact", mode="scope_complete")
    facts = list(run.facts)

    assert facts
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.applied_raw_token is not None
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert tokens_equal(
        run.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_package_manifest_mutation_during_capture_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, manifest = _git_repo(tmp_path)
    source = PackageManifestSource(repo="demo", repo_root=repo)
    original_read_bytes = Path.read_bytes
    mutated = False

    def _read_then_mutate(path: Path) -> bytes:
        nonlocal mutated
        raw_bytes = original_read_bytes(path)
        if path == manifest and not mutated:
            mutated = True
            manifest.write_text(
                "[project]\nname = \"changed\"\ndependencies = [\"mcp\"]\n"
            )
        return raw_bytes

    monkeypatch.setattr(Path, "read_bytes", _read_then_mutate)

    with pytest.raises(ChangeProbeError, match="changed during snapshot"):
        source.run("run-race", mode="scope_complete")

    assert mutated is True


def test_package_manifest_read_error_does_not_acknowledge_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, manifest = _git_repo(tmp_path)
    source = PackageManifestSource(repo="demo", repo_root=repo)
    original_read_bytes = Path.read_bytes

    def _unreadable(path: Path) -> bytes:
        if path == manifest:
            raise PermissionError("sensitive operating-system detail")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", _unreadable)

    with pytest.raises(ChangeProbeError, match="could not be read") as exc_info:
        source.run("run-error", mode="scope_complete")

    assert "sensitive operating-system detail" not in str(exc_info.value)


def test_package_manifest_non_git_direct_run_is_conservative(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = \"demo\"\n"
    )
    source = PackageManifestSource(repo="demo", repo_root=tmp_path)

    run = source.run("run-non-git", mode="scope_complete")

    assert source.probe_short_circuit_safe is False
    assert list(run.facts)
    assert run.applied_raw_token is None
    assert run.applied_token_scope == "unavailable"
    assert run.applied_token_authority == "unavailable"
