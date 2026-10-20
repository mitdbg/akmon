"""A deleted file's history record says when and by which commit (issue #3468).

Git history emits a `source_file` record for every path a commit touched, so
its `affects` edges have an endpoint. For a path the head tree no longer
holds, that record now carries `deleted_at` and `deleted_commit_sha`. These
tests build real repositories with plain deletes, deletes merged from a
branch, renames, re-adds, merge resolutions and fast-forwards.
"""

from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources._git_graph_history_emit import (
    MESSAGE_SCHEMA_VERSION,
)
from okg.substrate.library.sources._git_helpers import in_proc_path_deletions
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.git_graph_history import GitHistorySource

from .test_git_merge_attribution import _iso, _Repo


def _files(run) -> dict[str, dict]:  # noqa: ANN001
    return {
        f.node_id.removeprefix("file:r:"): f.attrs
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "source_file" and f.op == "I"
    }


def _history(repo: _Repo, cursor: dict | None = None):  # noqa: ANN202
    run = GitHistorySource(repo="r", repo_root=repo.root).run(
        "run", mode="cursor", cursor=cursor
    )
    facts = list(run.facts)
    run.facts = facts
    return run


def _deleted(attrs: dict) -> tuple[str | None, str | None]:
    return attrs.get("deleted_commit_sha"), attrs.get("deleted_at")


def test_a_plain_delete_names_its_commit(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"keep.txt": "k\n", "gone.txt": "g\n"}, "Base", at=0)
    repo.commit({"gone.txt": "g2\n"}, "Edit gone", at=100)
    drop = repo.commit({"gone.txt": None}, "Drop gone", at=200)

    files = _files(_history(repo))

    assert _deleted(files["gone.txt"]) == (drop, _iso(200))
    assert files["gone.txt"]["current_in_worktree"] is False
    assert _deleted(files["keep.txt"]) == (None, None)


def test_a_delete_merged_from_a_branch_names_the_branch_commit(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n", "old.py": "x\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    drop = repo.commit({"old.py": None}, "Drop old", at=100)
    repo.git("checkout", "-q", "main")
    repo.commit({"a.txt": "a2\n"}, "Main work", at=200)
    merge = repo.merge("topic", at=300)

    run = _history(repo)

    assert _deleted(_files(run)["old.py"]) == (drop, _iso(100))
    merge_edge = next(
        e
        for e in run.facts
        if isinstance(e, EdgeFact)
        and e.edge_type == "affects"
        and e.src == f"commit:r:{merge}"
        and e.dst == "file:r:old.py"
    )
    assert merge_edge.attrs["change_type"] == "deleted"
    assert merge_edge.attrs["via_merge"] is True


def test_a_rename_deletes_the_old_path_only(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"src/old_name.py": "body = 1\n" * 20}, "Base", at=0)
    repo.git("mv", "src/old_name.py", "src/new_name.py")
    repo.git("commit", "-q", "-m", "Rename", date=1_700_000_000 + 100)
    move = repo.git("rev-parse", "HEAD")

    files = _files(_history(repo))

    assert _deleted(files["src/old_name.py"]) == (move, _iso(100))
    assert _deleted(files["src/new_name.py"]) == (None, None)


def test_a_path_deleted_then_added_again_is_not_deleted(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"f.txt": "1\n"}, "Add", at=0)
    repo.commit({"f.txt": None}, "Drop", at=100)
    repo.commit({"f.txt": "2\n"}, "Add again", at=200)

    files = _files(_history(repo))

    assert _deleted(files["f.txt"]) == (None, None)


def test_a_path_deleted_twice_names_the_last_delete(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"f.txt": "1\n", "keep": "k\n"}, "Add", at=0)
    repo.commit({"f.txt": None}, "Drop", at=100)
    repo.commit({"f.txt": "2\n"}, "Add again", at=200)
    second = repo.commit({"f.txt": None}, "Drop again", at=300)

    files = _files(_history(repo))

    assert _deleted(files["f.txt"]) == (second, _iso(300))


def test_a_merge_that_resolves_by_deleting_names_the_merge(tmp_path: Path) -> None:
    """The branch deleted the file, the mainline then edited it, and the merge
    resolved the conflict by deleting it: the newest authored change is the
    edit, so the merge removed the file."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"f.txt": "1\n", "keep": "k\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    repo.commit({"f.txt": None}, "Drop on branch", at=100)
    repo.git("checkout", "-q", "main")
    repo.commit({"f.txt": "2\n"}, "Edit on main", at=200)
    repo.git(
        "merge", "-q", "--no-ff", "-m", "Merge topic", "topic",
        date=1_700_000_000 + 300, check=False,
    )
    repo.git("rm", "-q", "f.txt")
    repo.git("commit", "-q", "--no-edit", date=1_700_000_000 + 300)
    merge = repo.git("rev-parse", "HEAD")

    files = _files(_history(repo))

    assert _deleted(files["f.txt"]) == (merge, _iso(300))


def test_a_newer_branch_edit_dropped_by_the_merge_names_the_mainline_delete(
    tmp_path: Path,
) -> None:
    """The mainline deleted the file, a branch edited it later, and the merge
    kept the mainline side: the merge changes nothing on its first parent, so
    the mainline delete is still the one."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"f.txt": "1\n", "keep": "k\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    repo.git("checkout", "-q", "main")
    drop = repo.commit({"f.txt": None}, "Drop on main", at=100)
    repo.git("checkout", "-q", "topic")
    repo.commit({"f.txt": "2\n"}, "Edit on branch", at=200)
    repo.git("checkout", "-q", "main")
    repo.git(
        "merge", "-q", "--no-ff", "-m", "Merge topic", "topic",
        date=1_700_000_000 + 300, check=False,
    )
    repo.git("rm", "-q", "f.txt")
    repo.git("commit", "-q", "--no-edit", date=1_700_000_000 + 300)

    files = _files(_history(repo))

    assert _deleted(files["f.txt"]) == (drop, _iso(100))


def test_a_path_only_on_another_branch_is_not_deleted(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    repo.commit({"wip.py": "x\n"}, "Work in progress", at=100)
    repo.git("checkout", "-q", "main")

    files = _files(_history(repo))

    assert _deleted(files["wip.py"]) == (None, None)


def test_a_fast_forward_marks_a_delete_with_no_new_commit(tmp_path: Path) -> None:
    """The branch's delete was walked while the file still existed on the
    head; fast-forwarding the head brings no new commit but removes the file."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n", "gone.txt": "g\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    drop = repo.commit({"gone.txt": None}, "Drop gone", at=100)
    repo.git("checkout", "-q", "main")

    first = _history(repo)
    assert _deleted(_files(first)["gone.txt"]) == (None, None)

    repo.git("merge", "-q", "--ff-only", "topic")
    second = _history(repo, cursor=first.next_cursor)

    assert not any(
        isinstance(f, NodeFact) and f.subtype == "git_commit" for f in second.facts
    )
    assert _deleted(_files(second)["gone.txt"]) == (drop, _iso(100))
    assert second.next_cursor["head_sha"] == drop

    third = _history(repo, cursor=second.next_cursor)
    assert third.facts == []


def test_a_checkout_of_another_branch_is_not_skipped(tmp_path: Path) -> None:
    """Refs unchanged, HEAD moved: the run re-reads the paths that differ."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n", "gone.txt": "g\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    drop = repo.commit({"gone.txt": None}, "Drop gone", at=100)
    repo.git("checkout", "-q", "main")
    first = _history(repo)

    repo.git("checkout", "-q", "topic")
    second = _history(repo, cursor=first.next_cursor)

    assert _deleted(_files(second)["gone.txt"]) == (drop, _iso(100))


def test_an_incremental_delete_is_marked(tmp_path: Path) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n", "gone.txt": "g\n"}, "Base", at=0)
    first = _history(repo)
    drop = repo.commit({"gone.txt": None}, "Drop gone", at=100)

    second = _history(repo, cursor=first.next_cursor)

    assert set(_files(second)) == {"gone.txt"}
    assert _deleted(_files(second)["gone.txt"]) == (drop, _iso(100))


def test_a_cursor_from_the_previous_version_rewalks_and_marks_deletes(
    tmp_path: Path,
) -> None:
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n", "gone.txt": "g\n"}, "Base", at=0)
    drop = repo.commit({"gone.txt": None}, "Drop gone", at=100)
    seeded = _history(repo)
    old = {
        k: v for k, v in seeded.next_cursor.items() if k != "head_sha"
    } | {"message_schema_version": 3}

    rewalk = _history(repo, cursor=old)

    assert _deleted(_files(rewalk)["gone.txt"]) == (drop, _iso(100))
    assert rewalk.next_cursor["message_schema_version"] == MESSAGE_SCHEMA_VERSION
    # The field version this test covers; later fields only raise it.
    assert MESSAGE_SCHEMA_VERSION >= 4
    assert _history(repo, cursor=rewalk.next_cursor).facts == []


def test_one_pass_per_path_and_one_full_pass_agree(tmp_path: Path) -> None:
    """Up to eight paths each get their own git log; more take one full diff.
    Both must give the same answers."""
    repo = _Repo(tmp_path / "repo")
    names = [f"d{i}/f{i}.txt" for i in range(12)]
    repo.commit({n: f"{n}\n" for n in names} | {"keep": "k\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    for i, n in enumerate(names[:6]):
        repo.commit({n: None}, f"Drop {n}", at=10 + i)
    repo.git("checkout", "-q", "main")
    for i, n in enumerate(names[6:]):
        repo.commit({n: None}, f"Drop {n}", at=100 + i)
    repo.merge("topic", at=200)

    full = in_proc_path_deletions(repo.root, names + ["keep"])
    single = {}
    for n in names:
        single |= in_proc_path_deletions(repo.root, [n])

    assert set(full) == set(names)
    assert full == single


def test_a_merged_branch_that_added_and_removed_a_file_marks_it(tmp_path: Path) -> None:
    """The branch was walked on its own ref first. Its merge changes nothing
    on the first parent for the file, so only the head move finds it."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "topic")
    repo.commit({"tmp.txt": "t\n"}, "Add tmp", at=100)
    drop = repo.commit({"tmp.txt": None}, "Drop tmp", at=200)
    repo.git("checkout", "-q", "main")
    repo.commit({"a.txt": "a2\n"}, "Main work", at=300)
    first = _history(repo)
    assert _deleted(_files(first)["tmp.txt"]) == (None, None)

    repo.merge("topic", at=400)
    second = _history(repo, cursor=first.next_cursor)

    assert _deleted(_files(second)["tmp.txt"]) == (drop, _iso(200))
    assert _deleted(_files(_history(repo))["tmp.txt"]) == (drop, _iso(200))


def test_a_submodule_the_head_holds_is_not_deleted(tmp_path: Path) -> None:
    sub = _Repo(tmp_path / "sub")
    sub_sha = sub.commit({"x": "x\n"}, "Sub", at=0)
    repo = _Repo(tmp_path / "repo")
    repo.commit({"a.txt": "a\n"}, "Base", at=10)

    def link(at: int) -> None:
        repo.git("update-index", "--add", "--cacheinfo", f"160000,{sub_sha},m")
        repo.git("commit", "-q", "-m", "Link m", date=1_700_000_000 + at)

    link(100)
    repo.git("rm", "-q", "--cached", "m")
    repo.git("commit", "-q", "-m", "Unlink m", date=1_700_000_000 + 200)
    link(300)

    assert in_proc_path_deletions(repo.root, ["m"]) == {}


def test_a_merge_that_added_a_path_back_moves_the_delete_to_a_later_merge(
    tmp_path: Path,
) -> None:
    """A delete, then a merge whose resolution adds the file back, then a
    merge that deletes it: the later merge removed it, not the old delete."""
    repo = _Repo(tmp_path / "repo")
    repo.commit({"f.txt": "1\n", "keep": "k\n"}, "Base", at=0)
    repo.git("checkout", "-q", "-b", "keepf")
    repo.commit({"f.txt": "kept\n"}, "Edit on branch", at=50)
    repo.git("checkout", "-q", "main")
    repo.commit({"f.txt": None}, "Drop on main", at=100)
    # Merge keepf, resolving the modify/delete conflict by keeping the file.
    repo.git(
        "merge", "-q", "--no-ff", "-m", "Merge keepf", "keepf",
        date=1_700_000_000 + 200, check=False,
    )
    repo.git("add", "f.txt")
    repo.git("commit", "-q", "--no-edit", date=1_700_000_000 + 200)
    # Merge a side branch, removing the file in the merge itself.
    repo.git("checkout", "-q", "-b", "side")
    repo.commit({"other": "o\n"}, "Side work", at=250)
    repo.git("checkout", "-q", "main")
    repo.commit({"keep": "k2\n"}, "Main work", at=260)
    repo.git(
        "merge", "-q", "--no-ff", "--no-commit", "side",
        date=1_700_000_000 + 300,
    )
    repo.git("rm", "-q", "f.txt")
    repo.git("commit", "-q", "-m", "Merge side", date=1_700_000_000 + 300)
    assert len(repo.git("rev-list", "--parents", "-n1", "HEAD").split()) == 3
    last_merge = repo.git("rev-parse", "HEAD")

    assert in_proc_path_deletions(repo.root, ["f.txt"])["f.txt"].deleted_sha == last_merge
