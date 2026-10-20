"""Runtime change-probe contract for CodeLspResolutionSource (#323)."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    run_probe,
    tokens_equal,
)
from okg.substrate.library.sources.code_graph_lsp import (
    CodeLspResolutionSource,
)
from okg.substrate.library.sources.git_scope_probe import GitScopeProbe


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "probe@example.com")
    _git(root, "config", "user.name", "Probe Test")
    (root / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "notes.md").write_text("not in read scope\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "initial")
    return root


def _source(repo_root: Path, **overrides: object) -> CodeLspResolutionSource:
    params: dict[str, object] = {
        "repo": "demo",
        "repo_root": repo_root,
        "server_command": ["python", "-m", "demo_lsp"],
        "server_name": "demo-lsp",
        "server_version": "1.0",
        "language_id": "python",
        "include_exts": (".py",),
        "exclude_globs": ("vendor/**",),
        "max_files": 100,
        "max_bytes_per_file": 10_000,
        "dependency_fanout_limit": 10,
        "reverse_dependency_fanout_limit": 11,
        "timeout_s": 2.5,
        "max_lsp_requests": 50,
        "definition_request_concurrency": 4,
        "hot_path_max_lsp_requests": 20,
        "initialization_options": {"feature": {"enabled": True}},
    }
    params.update(overrides)
    return CodeLspResolutionSource(**params)


def _cursor(token: dict[str, object]) -> dict[str, object]:
    return {CURSOR_TOKEN_KEY: token}


def test_declares_and_builds_git_scope_probe(repo: Path) -> None:
    source = _source(repo)

    assert source.change_probe_kind == "git_scope"
    assert isinstance(source.change_probe, GitScopeProbe)


def test_mtime_churn_does_not_bust_token(repo: Path) -> None:
    source = _source(repo)
    before = source.change_probe.build_token()

    os.utime(repo / "app.py", (2_000_000_000, 2_000_000_000))

    assert tokens_equal(before, source.change_probe.build_token())


def test_edit_add_and_delete_each_bust_token(repo: Path) -> None:
    source = _source(repo)
    baseline = source.change_probe.build_token()

    (repo / "app.py").write_text("VALUE = 2\n", encoding="utf-8")
    assert not tokens_equal(baseline, source.change_probe.build_token())

    (repo / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "new.py").write_text("NEW = True\n", encoding="utf-8")
    assert not tokens_equal(baseline, source.change_probe.build_token())

    (repo / "new.py").unlink()
    (repo / "app.py").unlink()
    assert not tokens_equal(baseline, source.change_probe.build_token())


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("repo", "other"),
        ("server_command", ["python", "-m", "other_lsp"]),
        ("server_name", "other-lsp"),
        ("server_version", "2.0"),
        ("language_id", "unknown-language"),
        ("include_exts", (".py", ".pyi")),
        ("exclude_globs", ("vendor/**", "generated/**")),
        ("max_files", 101),
        ("max_bytes_per_file", 10_001),
        ("dependency_fanout_limit", 12),
        ("reverse_dependency_fanout_limit", 13),
        ("timeout_s", 3.5),
        ("max_lsp_requests", 51),
        ("definition_request_concurrency", 5),
        ("hot_path_max_lsp_requests", 21),
        ("initialization_options", {"feature": {"enabled": False}}),
    ],
)
def test_fact_determining_config_change_busts_token(
    repo: Path,
    field: str,
    changed_value: object,
) -> None:
    baseline = _source(repo).change_probe.build_token()
    changed = _source(repo, **{field: changed_value}).change_probe.build_token()

    assert not tokens_equal(baseline, changed), field


def test_untracked_non_source_file_does_not_bust_read_scope(repo: Path) -> None:
    source = _source(repo)
    baseline = source.change_probe.build_token()

    (repo / "local.txt").write_text("outside LSP read scope\n", encoding="utf-8")

    assert tokens_equal(baseline, source.change_probe.build_token())


def test_probe_error_fails_toward_full_read(tmp_path: Path) -> None:
    plain = tmp_path / "not-a-git-repo"
    plain.mkdir()
    source = _source(plain)

    verdict = run_probe(
        source.change_probe,
        cursor=_cursor({"invalid": "prior-token"}),
        sync_scope=None,
    )

    assert verdict.changed is True
    assert verdict.reason.startswith("probe_error:")
