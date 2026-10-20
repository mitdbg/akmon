"""PACT `source-supplied-search-citations` — citation URL/label builders.

Pure-function coverage for `_source_urls`: remote normalization, the
supported-host dialects, immutable-ref file URLs, label construction,
and the git helpers (HEAD sha, dirty paths). Adapter emission is covered
in `test_git_source_citations.py`; the search/fencing contract in
`tests/substrate/mcp/test_search_citations.py`.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources._source_urls import (
    citation_label,
    dirty_paths,
    head_commit_sha,
    repo_file_url,
    web_base_url,
)


def test_web_base_url_normalizes_common_remote_shapes() -> None:
    assert (
        web_base_url("https://gitlab.example.org/CMSProductionReprocessing/unified.git")
        == "https://gitlab.example.org/CMSProductionReprocessing/unified"
    )
    assert (
        web_base_url("https://github.com/dmwm/WMCore.git")
        == "https://github.com/dmwm/WMCore"
    )
    assert (
        web_base_url("git@gitlab.example.org:ai/it-puppet-hostgroup-vocms.git")
        == "https://gitlab.example.org/ai/it-puppet-hostgroup-vocms"
    )
    assert (
        web_base_url("ssh://git@gitlab.example.org:7999/group/repo.git")
        == "https://gitlab.example.org/group/repo"
    )
    # Credentials stripped; trailing slash tolerated.
    assert (
        web_base_url("https://user:token@github.example.com/org/repo.git")
        == "https://github.example.com/org/repo"
    )
    assert web_base_url("https://github.com/org/repo/") == (
        "https://github.com/org/repo"
    )
    assert web_base_url("http://gitlab.example.com/g/r.git") == (
        "http://gitlab.example.com/g/r"
    )


def test_web_base_url_keeps_explicit_https_port() -> None:
    assert web_base_url("https://gitlab.example.com:8443/g/r.git") == (
        "https://gitlab.example.com:8443/g/r"
    )
    # ssh ports say nothing about the web port — dropped.
    assert web_base_url("ssh://git@gitlab.example.org:7999/g/r.git") == (
        "https://gitlab.example.org/g/r"
    )


def test_web_base_url_rejects_local_and_alias_remotes() -> None:
    assert web_base_url(None) is None
    assert web_base_url("") is None
    assert web_base_url("file:///data/repos/x") is None
    assert web_base_url("/data/repos/x") is None
    # Dotless scp-style hosts are ssh-config aliases — the web host is
    # unknowable, so refuse rather than fabricate.
    assert web_base_url("cerngit:cms/repo.git") is None
    assert web_base_url("localhost:foo") is None
    # Single-segment paths are not group/repo shaped.
    assert web_base_url("https://gitlab.example.org/onlygroup") is None
    assert web_base_url("C:\\repos\\x") is None


def test_repo_file_url_supported_hosts_use_their_dialect() -> None:
    sha = "0" * 40
    assert repo_file_url(
        "https://gitlab.example.org/g/r.git", sha, "docs/a.md",
    ) == f"https://gitlab.example.org/g/r/-/blob/{sha}/docs/a.md"
    assert repo_file_url(
        "https://gitlab.com/g/r.git", sha, "docs/a.md",
    ) == f"https://gitlab.com/g/r/-/blob/{sha}/docs/a.md"
    assert repo_file_url(
        "https://github.com/dmwm/CMSRucio.git", sha, "scripts/x",
    ) == f"https://github.com/dmwm/CMSRucio/blob/{sha}/scripts/x"


def test_repo_file_url_unsupported_host_is_label_only() -> None:
    sha = "0" * 40
    # Unknown host: no guessed URL.
    assert repo_file_url("https://forge.example.org/g/r", sha, "a.md") is None


def test_repo_file_url_preserves_scheme_and_encodes_file_path() -> None:
    sha = "0" * 40
    assert repo_file_url(
        "http://gitlab.example.com/g/r.git",
        sha,
        "docs/a b#c?.md",
    ) == (
        f"http://gitlab.example.com/g/r/-/blob/{sha}/"
        "docs/a%20b%23c%3F.md"
    )


def test_repo_file_url_requires_all_parts() -> None:
    assert repo_file_url(None, "sha", "a.md") is None
    assert repo_file_url("https://github.com/o/r", None, "a.md") is None
    assert repo_file_url("https://github.com/o/r", "sha", "") is None
    assert repo_file_url("/local/path", "sha", "a.md") is None


def test_citation_label_is_repo_slash_path() -> None:
    assert citation_label("unified", "src/app.py") == "unified/src/app.py"
    assert citation_label("r", "/a.md") == "r/a.md"
    assert citation_label("", "a.md") is None
    assert citation_label("r", "") is None


def _init_repo(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(root), "init", "-b", "main"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "config", "user.email", "t@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(root), "config", "user.name", "T"], check=True,
    )


def _commit_all(root: Path) -> None:
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-m", "c", "--no-gpg-sign"],
        check=True,
    )


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "a.md").write_text("# A\n\nbody\n")
    _commit_all(root)
    return root


def test_head_commit_sha_and_dirty_paths(git_repo: Path, tmp_path: Path) -> None:
    sha = head_commit_sha(git_repo)
    assert sha and len(sha) == 40
    assert dirty_paths(git_repo) == frozenset()
    (git_repo / "a.md").write_text("# A\n\nchanged\n")
    (git_repo / "new.txt").write_text("untracked\n")
    assert dirty_paths(git_repo) == frozenset({"a.md", "new.txt"})
    # Non-git dir: no citable revision, unprovable cleanliness.
    assert head_commit_sha(tmp_path / "nowhere") is None
    assert dirty_paths(tmp_path / "nowhere") is None
