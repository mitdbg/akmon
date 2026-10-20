"""GitScopeProbe cohort migration (#323, git-scope-probe-file-cohort).

Each migrated git-backed source must expose a sound, churn-immune GitScopeProbe
and declare change_probe_kind='git_scope'. Verified against a real temp git repo.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.code_graph_dataflow import CodeDataFlowSource
from okg.substrate.library.sources.code_graph_interface import InterfaceSource
from okg.substrate.library.sources.code_graph_manifest import PackageManifestSource
from okg.substrate.library.sources.code_graph_structure import CodeStructureSource
from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

COHORT = [CodeDataFlowSource, InterfaceSource, CodeStructureSource, PackageManifestSource]


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "repo"
    r.mkdir()
    subprocess.run(["git", "-C", str(r), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
    (r / "app.py").write_text("import os\nX = os.getenv('A')\n")
    (r / "pyproject.toml").write_text("[project]\nname='demo'\n")
    subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(r), "commit", "-q", "-m", "i"], check=True)
    return r


@pytest.mark.parametrize("cls", COHORT, ids=[c.__name__ for c in COHORT])
def test_cohort_declares_git_scope_probe(cls, repo):
    assert getattr(cls, "change_probe_kind", None) == "git_scope"
    src = cls(repo="demo", repo_root=str(repo))
    assert isinstance(src.change_probe, GitScopeProbe)


@pytest.mark.parametrize("cls", COHORT, ids=[c.__name__ for c in COHORT])
def test_cohort_probe_is_churn_immune(cls, repo):
    src = cls(repo="demo", repo_root=str(repo))
    t0 = src.change_probe.build_token()
    # mtime churn: touch every tracked file, content identical.
    for name in ("app.py", "pyproject.toml"):
        os.utime(repo / name, (2_000_000_000, 2_000_000_000))
    t1 = src.change_probe.build_token()
    assert tokens_equal(t0, t1), f"{cls.__name__} probe must be churn-immune"


@pytest.mark.parametrize("cls", COHORT, ids=[c.__name__ for c in COHORT])
def test_cohort_probe_detects_content_edit(cls, repo):
    src = cls(repo="demo", repo_root=str(repo))
    t0 = src.change_probe.build_token()
    # Edit a file each source's scope covers: app.py (.py) for the AST sources,
    # pyproject.toml for the manifest source. Edit both to cover all.
    (repo / "app.py").write_text("import os\nX = os.getenv('A')\nY = os.getenv('B')\n")
    (repo / "pyproject.toml").write_text("[project]\nname='demo2'\n")
    t1 = src.change_probe.build_token()
    assert not tokens_equal(t0, t1), f"{cls.__name__} probe must detect a content edit"
