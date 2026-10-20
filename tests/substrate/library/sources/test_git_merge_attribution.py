"""Git ingest credits a file change to the commit that made it (issue #3468).

A merge diffed against its first parent "adds" or "modifies" every file its
branch changed. These tests build small repositories with no-fast-forward
merges and check that the file nodes name the authored commits, that the
merge's own `affects` edges say `via_merge`, and that graphs written by the
old code are brought up to date by an ordinary run.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from okg.substrate.library.sources._git_graph_files_emit import (
    FILE_ATTRS_VERSION,
    FILE_ATTRS_VERSION_KEY,
    OWED_RETRACTION_PATHS_KEY,
)
from okg.substrate.library.sources._git_graph_files_emit import (
    GitFilesSource as DomainGitFilesSource,
)
from okg.substrate.library.sources._git_graph_history_emit import (
    MESSAGE_SCHEMA_VERSION,
)
from okg.substrate.library.sources._git_helpers import in_proc_path_history
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.git_graph_files import GitFilesSource
from okg.substrate.library.sources.git_graph_history import GitHistorySource

_T0 = 1_700_000_000


class _Repo:
    """A repository whose every commit gets an explicit, distinct date."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True)
        self.git("init", "-q", "-b", "main")

    def git(self, *args: str, date: int | None = None, check: bool = True) -> str:
        env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.test",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.test",
        }
        if date is not None:
            env["GIT_AUTHOR_DATE"] = f"@{date} +0000"
            env["GIT_COMMITTER_DATE"] = f"@{date} +0000"
        return subprocess.run(
            ["git", *args],
            cwd=self.root,
            env=env,
            check=check,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def commit(self, files: dict[str, str | None], message: str, *, at: int) -> str:
        for rel, content in files.items():
            if content is None:
                self.git("rm", "-q", rel)
                continue
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            self.git("add", rel)
        self.git("commit", "-q", "-m", message, date=_T0 + at)
        return self.git("rev-parse", "HEAD")

    def merge(self, branch: str, *, at: int) -> str:
        self.git("merge", "-q", "--no-ff", "-m", f"Merge {branch}", branch, date=_T0 + at)
        return self.git("rev-parse", "HEAD")


def _iso(at: int) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(_T0 + at, tz=timezone.utc).isoformat()


def _branch_repo(tmp_path: Path) -> tuple[_Repo, dict[str, str]]:
    """main: base; topic: add then edit a script; main: other work; merge."""
    repo = _Repo(tmp_path / "repo")
    shas = {"base": repo.commit({"README.md": "hello\n"}, "Base", at=0)}
    repo.git("checkout", "-q", "-b", "topic")
    shas["add"] = repo.commit({"tool.py": "print(1)\n"}, "Add tool", at=100)
    shas["edit"] = repo.commit({"tool.py": "print(2)\n"}, "Edit tool", at=200)
    repo.git("checkout", "-q", "main")
    shas["main"] = repo.commit({"other.txt": "x\n"}, "Main work", at=300)
    shas["merge"] = repo.merge("topic", at=400)
    return repo, shas


def _file_attrs(run) -> dict[str, dict]:  # noqa: ANN001
    return {
        f.node_id: f.attrs
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "source_file" and f.op == "I"
    }


def test_file_names_the_authored_commits_not_the_merge(tmp_path: Path) -> None:
    repo, shas = _branch_repo(tmp_path)

    attrs = _file_attrs(GitFilesSource(repo="r", repo_root=repo.root).run("run-1"))

    tool = attrs["file:r:tool.py"]
    assert tool["last_modified_commit_sha"] == shas["edit"]
    assert tool["last_modified_at"] == _iso(200)
    assert tool["created_commit_sha"] == shas["add"]
    assert tool["created_at"] == _iso(100)
    readme = attrs["file:r:README.md"]
    # Added by the root commit and never changed: created and last modified
    # by the same commit, and created_at no longer merely copies last_modified.
    assert readme["created_commit_sha"] == readme["last_modified_commit_sha"] == shas["base"]
    other = attrs["file:r:other.txt"]
    assert other["created_commit_sha"] == other["last_modified_commit_sha"] == shas["main"]


def test_mainline_and_branch_edits_are_compared_by_date(tmp_path: Path) -> None:
    """A file edited on the branch after the mainline edit names the branch
    commit; the topological walk alone could list the mainline edit first."""
    repo = _Repo(tmp_path / "repo")
    lines = [f"line {i}\n" for i in range(30)]
    repo.commit({"f.txt": "".join(lines)}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    branch_lines = list(lines)
    branch_lines[28] = "branch edit\n"
    branch_edit = repo.commit({"f.txt": "".join(branch_lines)}, "Branch edit", at=300)
    repo.git("checkout", "-q", "main")
    main_lines = list(lines)
    main_lines[1] = "main edit\n"
    repo.commit({"f.txt": "".join(main_lines)}, "Main edit", at=200)
    repo.merge("topic", at=400)

    history = in_proc_path_history(repo.root, ["f.txt"])

    assert history["f.txt"].last_modified_sha == branch_edit


def test_a_newer_mainline_edit_beats_an_older_branch_edit(tmp_path: Path) -> None:
    """The mirror case: without ordering by date the walk would reach the
    merge's second-parent line first and name the older branch edit."""
    repo = _Repo(tmp_path / "repo")
    lines = [f"line {i}\n" for i in range(30)]
    repo.commit({"f.txt": "".join(lines)}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    branch_lines = list(lines)
    branch_lines[28] = "branch edit\n"
    repo.commit({"f.txt": "".join(branch_lines)}, "Branch edit", at=200)
    repo.git("checkout", "-q", "main")
    main_lines = list(lines)
    main_lines[1] = "main edit\n"
    main_edit = repo.commit({"f.txt": "".join(main_lines)}, "Main edit", at=300)
    repo.merge("topic", at=400)

    history = in_proc_path_history(repo.root, ["f.txt"])

    assert history["f.txt"].last_modified_sha == main_edit


def test_a_delete_a_merge_undid_is_not_the_last_change(tmp_path: Path) -> None:
    """A branch deletes a file the mainline keeps; the merge keeps it. The
    file still exists, so the delete is not its last change."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"keep.txt": "v1\n", "a.txt": "a\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    repo.commit({"keep.txt": None}, "Drop keep", at=300)
    repo.git("checkout", "-q", "main")
    edit = repo.commit({"keep.txt": "v2\n"}, "Edit keep", at=200)
    repo.git("merge", "-q", "--no-ff", "--no-commit", "topic", check=False)
    repo.git("checkout", "-q", edit, "--", "keep.txt")
    repo.git("add", "keep.txt")
    repo.git("commit", "-q", "-m", "Merge topic keeping keep.txt", date=_T0 + 400)

    history = in_proc_path_history(repo.root, ["keep.txt"])

    assert history["keep.txt"].last_modified_sha == edit


def test_a_path_only_a_merge_added_names_the_merge(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    repo.commit({"b.txt": "b\n"}, "Topic", at=100)
    repo.git("checkout", "-q", "main")
    repo.commit({"c.txt": "c\n"}, "Main", at=200)
    repo.git("merge", "-q", "--no-ff", "--no-commit", "topic")
    (repo.root / "resolution.txt").write_text("made in the merge\n", encoding="utf-8")
    repo.git("add", "resolution.txt")
    repo.git("commit", "-q", "-m", "Merge topic", date=_T0 + 300)
    merge = repo.git("rev-parse", "HEAD")

    history = in_proc_path_history(repo.root, ["resolution.txt", "b.txt"])

    assert history["resolution.txt"].last_modified_sha == merge
    assert history["resolution.txt"].created_sha == merge
    assert history["b.txt"].created_sha != merge


def test_merge_edges_say_via_merge_and_authored_edges_do_not(tmp_path: Path) -> None:
    repo, shas = _branch_repo(tmp_path)

    run = GitHistorySource(repo="r", repo_root=repo.root).run("run-1", mode="cursor")
    affects = {
        (e.src, e.dst): e.attrs
        for e in run.facts
        if isinstance(e, EdgeFact) and e.edge_type == "affects"
    }

    merge_edge = affects[(f"commit:r:{shas['merge']}", "file:r:tool.py")]
    assert merge_edge["via_merge"] is True
    assert merge_edge["change_type"] == "added"
    for key in ("add", "edit", "base", "main"):
        for (src, _dst), attrs in affects.items():
            if src == f"commit:r:{shas[key]}":
                assert "via_merge" not in attrs
    merge_edges = [k for k in affects if k[0] == f"commit:r:{shas['merge']}"]
    assert all(affects[k]["via_merge"] is True for k in merge_edges)
    assert run.next_cursor["message_schema_version"] == MESSAGE_SCHEMA_VERSION
    # The field version this test covers; later fields only raise it.
    assert MESSAGE_SCHEMA_VERSION >= 4


def test_history_cursor_from_the_old_code_rewalks_once(tmp_path: Path) -> None:
    repo, shas = _branch_repo(tmp_path)
    seeded = GitHistorySource(repo="r", repo_root=repo.root).run("run-0", mode="cursor")
    list(seeded.facts)
    old_cursor = {**seeded.next_cursor, "message_schema_version": 2}

    rewalk = GitHistorySource(repo="r", repo_root=repo.root).run(
        "run-1", mode="cursor", cursor=old_cursor
    )
    edges = [
        e for e in rewalk.facts
        if isinstance(e, EdgeFact) and e.edge_type == "affects"
        and e.src == f"commit:r:{shas['merge']}"
    ]
    assert edges and all(e.attrs["via_merge"] is True for e in edges)

    again = GitHistorySource(repo="r", repo_root=repo.root).run(
        "run-2", mode="cursor", cursor=rewalk.next_cursor
    )
    assert list(again.facts) == []


def _old_cursor(cursor: dict) -> dict:
    """A cursor as the code before issue #3468 wrote it."""
    return {k: v for k, v in cursor.items() if k != FILE_ATTRS_VERSION_KEY}


def test_files_cursor_from_the_old_code_rereads_every_file_once(tmp_path: Path) -> None:
    """A routine cursor run over an unchanged tree, from a cursor the old code
    wrote, re-emits every current file with the new attributes; the next
    unchanged run emits nothing."""
    repo, shas = _branch_repo(tmp_path)
    seeded = GitFilesSource(repo="r", repo_root=repo.root).run("run-0")
    list(seeded.facts)
    assert seeded.next_cursor[FILE_ATTRS_VERSION_KEY] == FILE_ATTRS_VERSION

    current = GitFilesSource(repo="r", repo_root=repo.root).run(
        "run-1", cursor=seeded.next_cursor
    )
    assert _file_attrs(current) == {}

    reread = GitFilesSource(repo="r", repo_root=repo.root).run(
        "run-1", cursor=_old_cursor(seeded.next_cursor)
    )
    emitted = _file_attrs(reread)
    assert set(emitted) == {"file:r:README.md", "file:r:tool.py", "file:r:other.txt"}
    assert emitted["file:r:tool.py"]["created_commit_sha"] == shas["add"]
    assert reread.next_cursor[FILE_ATTRS_VERSION_KEY] == FILE_ATTRS_VERSION

    again = GitFilesSource(repo="r", repo_root=repo.root).run(
        "run-2", cursor=reread.next_cursor
    )
    assert _file_attrs(again) == {}


def test_files_reread_still_finds_a_file_deleted_since(tmp_path: Path) -> None:
    """The re-read takes deletions from the saved tree: a cursor run owes the
    deleted path (it cannot retract in cursor mode), and the crawl reports it
    stale."""
    repo, _shas = _branch_repo(tmp_path)
    seeded = GitFilesSource(repo="r", repo_root=repo.root).run("run-0")
    list(seeded.facts)
    repo.commit({"other.txt": None}, "Drop other", at=500)

    reread = GitFilesSource(repo="r", repo_root=repo.root).run(
        "run-1", cursor=_old_cursor(seeded.next_cursor)
    )
    assert set(_file_attrs(reread)) == {"file:r:README.md", "file:r:tool.py"}
    assert reread.next_cursor[OWED_RETRACTION_PATHS_KEY] == ["other.txt"]
    domain = DomainGitFilesSource(
        repo_root=repo.root, repo="r", state=_old_cursor(seeded.next_cursor)
    )
    list(domain.nodes())
    assert domain.stale_node_ids == ["file:r:other.txt"]


def test_a_scoped_run_keeps_the_old_attributes_version(tmp_path: Path) -> None:
    """A scoped run reads only its paths, so the next unscoped run still
    re-reads every file."""
    repo, _shas = _branch_repo(tmp_path)
    seeded = GitFilesSource(repo="r", repo_root=repo.root).run("run-0")
    list(seeded.facts)

    scoped = GitFilesSource(repo="r", repo_root=repo.root).run(
        "run-1",
        mode="scope_incomplete",
        cursor=_old_cursor(seeded.next_cursor),
        sync_scope={"scope_keys": ["tool.py"]},
    )
    list(scoped.facts)
    assert FILE_ATTRS_VERSION_KEY not in scoped.next_cursor

    unscoped = GitFilesSource(repo="r", repo_root=repo.root).run(
        "run-2", cursor=scoped.next_cursor
    )
    assert set(_file_attrs(unscoped)) == {
        "file:r:README.md", "file:r:tool.py", "file:r:other.txt"
    }
    assert unscoped.next_cursor[FILE_ATTRS_VERSION_KEY] == FILE_ATTRS_VERSION
