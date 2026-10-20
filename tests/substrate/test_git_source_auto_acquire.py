"""git-files owns checkout acquisition; code-structure inherits it.

Adapter-level (DB-free) proof against a local fixture remote: the git-files
source with manage_checkout clones a missing repo_root then indexes it; an
existing checkout is fast-forward-pulled; a non-fast-forward degrades to
acquire_stale while still indexing the existing tree; a failed clone degrades to
acquire_failed and emits zero facts (no crash); and a code-structure source
pointed at the same repo_root with manage_checkout=false does NOT acquire.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from okg.substrate.library.sources.code_graph_structure import CodeStructureSource
from okg.substrate.library.sources.git_graph_files import GitFilesSource


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def _commit(repo: Path, name: str, body: str) -> None:
    (repo / name).write_text(body, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", name)


def _init_remote(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q")
    _commit(path, "mod.py", "def foo():\n    return 1\n")
    return path


def _facts(source, mode: str = "cursor") -> int:
    run = source.run("r1", mode=mode)
    return len(list(run.facts))


def test_owner_clones_missing_then_indexes(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    src = GitFilesSource(
        repo="x", repo_root=dest, url=str(remote), manage_checkout=True,
        include_untracked=True,
    )

    assert not dest.exists()
    health = src.ensure_checkout()
    assert health is None  # clean clone -> no degraded health
    assert (dest / ".git").exists()
    assert _facts(src) > 0  # the cloned repo is indexed


def test_owner_fast_forward_pull(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    src = GitFilesSource(repo="x", repo_root=dest, url=str(remote), manage_checkout=True)
    src.ensure_checkout()  # initial clone

    _commit(remote, "extra.py", "def bar():\n    return 2\n")
    health = src.ensure_checkout()  # fast-forward
    assert health is None
    assert (dest / "extra.py").exists()


def test_non_fast_forward_degrades_stale_but_still_indexes(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    src = GitFilesSource(
        repo="x", repo_root=dest, url=str(remote), manage_checkout=True,
        include_untracked=True,
    )
    src.ensure_checkout()

    _commit(dest, "local.py", "def local():\n    return 0\n")  # local-only commit
    _commit(remote, "c.py", "def c():\n    return 3\n")  # remote advances -> diverged

    health = src.ensure_checkout()
    assert health is not None
    assert health.status == "acquire_stale"
    # The existing (stale) tree is still indexed rather than crashing.
    assert _facts(src) > 0


def test_failed_clone_degrades_and_emits_zero_facts(tmp_path: Path) -> None:
    dest = tmp_path / "dest"
    src = GitFilesSource(
        repo="x", repo_root=dest, url=str(tmp_path / "no-such.git"),
        manage_checkout=True,
    )
    health = src.ensure_checkout()
    assert health is not None
    assert health.status == "acquire_failed"
    assert not (dest / ".git").exists()

    run = src.run("r1", mode="cursor")
    assert len(list(run.facts)) == 0  # no crash, zero facts
    assert run.health is not None and run.health.status == "acquire_failed"


def test_code_structure_inherits_owner_checkout(tmp_path: Path) -> None:
    remote = _init_remote(tmp_path / "remote")
    dest = tmp_path / "dest"
    # Owner (git-files) materialises the checkout.
    GitFilesSource(
        repo="x", repo_root=dest, url=str(remote), manage_checkout=True,
    ).ensure_checkout()
    assert (dest / ".git").exists()

    # code-structure is a non-owner: manage_checkout defaults false -> ensure
    # does not pull, and it indexes the inherited checkout normally.
    cs = CodeStructureSource(repo="x", repo_root=dest)
    assert cs.ensure_checkout() is None
    assert _facts(cs) > 0  # code symbols from mod.py
