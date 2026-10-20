"""Runner proof for authoritative replay modes of exact-snapshot adapters."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.code_graph_manifest import (
    PackageManifestSource,
)
from okg.substrate.library.sources.github import GitHubSource


pytestmark = pytest.mark.postgres


def _git_manifest_repo(tmp_path: Path) -> Path:
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
    (repo / "pyproject.toml").write_text(
        "[project]\nname = \"demo\"\ndependencies = [\"pydantic>=2\"]\n"
    )
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "fixture"],
        check=True,
    )
    return repo


def _assert_replay_cursor(result) -> None:
    assert result.completed_scope is True
    assert result.next_cursor is not None
    assert result.next_cursor.get("record_set") is not None
    assert result.next_cursor.get("change_probe_token") is not None


def test_package_profile_first_cursor_run_proves_scope_complete_replay(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    repo = _git_manifest_repo(tmp_path)
    source = PackageManifestSource(repo="demo", repo_root=repo)

    result = SourceRunner(
        clean_runtime,
        source_name="test.package-exact-replay",
    ).run(source, mode="cursor")

    _assert_replay_cursor(result)


def test_github_profile_first_cursor_run_proves_reconcile_replay(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "issues.json"
    fixture.write_text(json.dumps([
        {
            "number": 1,
            "title": "issue",
            "state": "open",
            "updated_at": "2026-07-25T00:00:00Z",
        }
    ]))
    source = GitHubSource(repo="owner/repo", fixture_path=fixture)

    result = SourceRunner(
        clean_runtime,
        source_name="test.github-exact-replay",
    ).run(source, mode="cursor")

    _assert_replay_cursor(result)
