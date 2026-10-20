"""A file an incomplete git-files run finds gone is retracted by a later
complete run (issue #3301).

Only a complete run (``reconcile``, unscoped ``scope_complete``) emits
retractions, yet an incomplete unscoped run (``cursor``, unscoped
``scope_incomplete``) still advances the cursor tree. The deletion it saw is
then out of every later tree diff, and the complete run carries the deleted
file's prior record forward. Each test drives real git, models the graph by
applying every run's facts in order with the record set the runner would
keep, and ends with the graph equal to the working tree the source reads.
The runner-level proof on PostgreSQL is in
``tests/substrate/test_source_runner_record_set.py``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write(repo: Path, files: dict[str, str]) -> None:
    for rel, text in files.items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "OKG Test")
    _write(repo, {"keep.cc": "keep\n", "other.cc": "other\n"})
    _commit(repo, "A")
    return repo


def _source(repo: Path, *, include_untracked: bool = False):  # noqa: ANN202
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    return GitFilesSource(
        repo="r", repo_root=repo, include_untracked=include_untracked,
    )


def _tree_paths(repo: Path) -> set[str]:
    return set(_git(repo, "ls-tree", "-r", "--name-only", "HEAD").splitlines())


def _fresh_repository_handles() -> None:
    """Start each run with fresh repository handles, as a new process would.

    This process's cached handle keeps the index it first read, so it lists
    a file a later commit removed and misses one a later commit added
    (issue #3291, a separate open bug fixed by PR #3300).
    """
    from okg.substrate.library.sources import _git_helpers

    _git_helpers._repo_tls.cache = {}


class _Graph:
    """Source-file nodes after applying each run's facts in order, with the
    record set SourceRunner would keep in the cursor."""

    def __init__(
        self,
        *,
        include_untracked: bool = False,
        fresh_handles: bool = True,
    ) -> None:
        self.nodes: dict[str, str] = {}
        self.cursor: dict = {}
        self.include_untracked = include_untracked
        self.fresh_handles = fresh_handles
        self.last_run = None

    def apply(self, run) -> None:  # noqa: ANN001
        from okg.substrate.library.sources.base import RecordSet

        self.last_run = run
        for fact in run.facts:
            if getattr(fact, "subtype", None) != "source_file":
                continue
            path = fact.node_id.removeprefix("file:r:")
            if fact.op == "R":
                self.nodes.pop(path, None)
            else:
                self.nodes[path] = fact.attrs["git_blob_sha"]
        cursor = dict(run.next_cursor)
        prior_keys = (
            set(RecordSet.from_cursor(self.cursor["record_set"]).records)
            if "record_set" in self.cursor else set()
        )
        if run.record_set is not None:
            if run.record_set_replacement_keys is not None:
                prior = RecordSet.from_cursor(self.cursor["record_set"])
                keys = set(run.record_set_replacement_keys)
                records = {
                    k: v for k, v in prior.records.items() if k not in keys
                }
                records.update(run.record_set.records)
                record_set = RecordSet(records=records)
            else:
                record_set = run.record_set
            cursor["record_set"] = record_set.to_cursor()
            if run.completed_scope:
                # The runner's record-set diff retracts every prior record
                # the complete run no longer lists.
                for key in prior_keys - set(record_set.records):
                    self.nodes.pop(key, None)
        elif "record_set" in self.cursor:
            # An incomplete run keeps the prior authority (SourceRunner).
            cursor["record_set"] = self.cursor["record_set"]
        self.cursor = cursor

    def run(self, repo: Path, mode: str, keys: list[str] | None = None) -> None:
        if self.fresh_handles:
            _fresh_repository_handles()
        self.apply(
            _source(repo, include_untracked=self.include_untracked).run(
                f"{mode}-{keys}",
                mode=mode,
                cursor=self.cursor,
                sync_scope={"scope_keys": keys} if keys else None,
            )
        )

    def record_keys(self) -> set[str]:
        from okg.substrate.library.sources.base import RecordSet

        return {
            key
            for key in RecordSet.from_cursor(self.cursor["record_set"]).records
            if not key.startswith("__branch__:")
        }


def _start(
    tmp_path: Path,
    *,
    include_untracked: bool = False,
    fresh_handles: bool = True,
):  # noqa: ANN202
    repo = _repo(tmp_path)
    graph = _Graph(
        include_untracked=include_untracked, fresh_handles=fresh_handles,
    )
    graph.run(repo, "scope_complete")
    assert set(graph.nodes) == _tree_paths(repo)
    assert "record_set" in graph.cursor
    return repo, graph


INCOMPLETE_MODES = ("cursor", "scope_incomplete")
COMPLETE_MODES = ("reconcile", "scope_complete")


@pytest.mark.parametrize("complete_mode", COMPLETE_MODES)
@pytest.mark.parametrize("incomplete_mode", INCOMPLETE_MODES)
def test_file_deleted_before_incomplete_run_is_retracted(
    tmp_path: Path, incomplete_mode: str, complete_mode: str,
) -> None:
    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")

    graph.run(repo, incomplete_mode)
    assert "keep.cc" in graph.nodes  # an incomplete run cannot retract
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]

    graph.run(repo, complete_mode)
    assert set(graph.nodes) == _tree_paths(repo) == {"other.cc"}
    assert graph.record_keys() == {"other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


@pytest.mark.parametrize("complete_mode", COMPLETE_MODES)
def test_owed_retraction_survives_repeated_incomplete_runs(
    tmp_path: Path, complete_mode: str,
) -> None:
    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, "cursor")
    graph.run(repo, "scope_incomplete")
    graph.run(repo, "cursor")
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]

    graph.run(repo, complete_mode)
    assert set(graph.nodes) == {"other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


@pytest.mark.parametrize("complete_mode", COMPLETE_MODES)
def test_file_readded_before_complete_run_is_read_not_retracted(
    tmp_path: Path, complete_mode: str,
) -> None:
    # This row keeps this process's cached repository handle (issue #3291):
    # the owed existence check reads the current tree by its sha, so a stale
    # cached index cannot change its answer.
    repo, graph = _start(tmp_path, fresh_handles=False)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, "cursor")
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]

    _write(repo, {"keep.cc": "keep v2\n"})
    _commit(repo, "C adds keep.cc back")
    graph.run(repo, complete_mode)
    assert set(graph.nodes) == {"keep.cc", "other.cc"}
    blob = _git(repo, "rev-parse", "HEAD:keep.cc")
    assert graph.nodes["keep.cc"] == blob
    assert graph.record_keys() == {"keep.cc", "other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


def test_readded_file_settles_its_owed_entry_on_an_incomplete_run(
    tmp_path: Path,
) -> None:
    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, "cursor")
    _write(repo, {"keep.cc": "keep v2\n"})
    _commit(repo, "C adds keep.cc back")

    graph.run(repo, "cursor")
    assert graph.nodes["keep.cc"] == _git(repo, "rev-parse", "HEAD:keep.cc")
    assert "owed_retraction_paths" not in graph.cursor


@pytest.mark.parametrize("complete_mode", COMPLETE_MODES)
def test_untracked_file_removed_before_incomplete_run_is_retracted(
    tmp_path: Path, complete_mode: str,
) -> None:
    repo, graph = _start(tmp_path, include_untracked=True)
    _write(repo, {"scratch.cc": "scratch\n"})
    graph.run(repo, complete_mode)
    assert "scratch.cc" in graph.nodes
    assert "scratch.cc" in graph.record_keys()

    (repo / "scratch.cc").unlink()
    graph.run(repo, "cursor")
    assert "scratch.cc" in graph.nodes
    assert graph.cursor.get("owed_retraction_paths") == ["scratch.cc"]

    graph.run(repo, complete_mode)
    assert set(graph.nodes) == {"keep.cc", "other.cc"}
    assert "scratch.cc" not in graph.record_keys()
    assert "owed_retraction_paths" not in graph.cursor


@pytest.mark.parametrize("complete_mode", COMPLETE_MODES)
def test_control_delete_then_complete_run_retracts(
    tmp_path: Path, complete_mode: str,
) -> None:
    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, complete_mode)
    assert set(graph.nodes) == {"other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


def test_path_scoped_run_keeps_owed_paths_outside_its_scope(
    tmp_path: Path,
) -> None:
    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, "cursor")
    _write(repo, {"other.cc": "other v2\n"})
    _commit(repo, "C edits other.cc")

    graph.run(repo, "scope_incomplete", ["other.cc"])
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]
    graph.run(repo, "scope_complete", ["other.cc"])
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]

    graph.run(repo, "reconcile")
    assert set(graph.nodes) == {"other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


def test_path_scoped_complete_run_settles_the_owed_path_it_retracts(
    tmp_path: Path,
) -> None:
    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, "cursor")

    graph.run(repo, "scope_complete", ["keep.cc"])
    assert "keep.cc" not in graph.record_keys()
    assert "owed_retraction_paths" not in graph.cursor


def test_incomplete_run_past_the_owed_limit_holds_the_cursor_until_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit

    monkeypatch.setattr(
        _git_graph_files_emit, "OWED_RETRACTION_PATHS_LIMIT", 1,
    )
    repo, graph = _start(tmp_path)
    _write(repo, {"new.cc": "new\n"})
    _commit(repo, "B adds new.cc")
    graph.run(repo, "cursor")  # new.cc is first read by an incomplete run
    assert "new.cc" in graph.nodes
    held = dict(graph.cursor)

    _git(repo, "rm", "-q", "keep.cc", "new.cc")
    _commit(repo, "C deletes keep.cc and new.cc")
    for _ in range(2):
        graph.run(repo, "cursor")
        # Two owed paths pass the limit of one. The run neither drops nor
        # truncates them: it reports itself partial, emits nothing, and
        # keeps the prior cursor, so a complete run still finds both.
        assert graph.last_run.health.status == "partial"
        assert "owed retractions" in graph.last_run.health.reason
        assert graph.cursor == held

    graph.run(repo, "reconcile")
    assert set(graph.nodes) == _tree_paths(repo) == {"other.cc"}
    assert graph.record_keys() == {"other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


def test_probe_reports_a_change_while_retractions_are_owed(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY

    repo, graph = _start(tmp_path)
    _git(repo, "rm", "-q", "keep.cc")
    _commit(repo, "B deletes keep.cc")
    graph.run(repo, "cursor")

    probe = _source(repo).change_probe
    token = probe.build_token()
    settled = {CURSOR_TOKEN_KEY: token}
    assert probe.probe(cursor=settled, sync_scope=None).changed is False
    owed = {**settled, "owed_retraction_paths": ["keep.cc"]}
    verdict = probe.probe(cursor=owed, sync_scope=None)
    assert verdict.changed is True
    assert verdict.reason == "owed_retractions_pending"


def _replace_keep_with_directory(repo: Path) -> None:
    _git(repo, "rm", "-q", "keep.cc")
    _write(repo, {"keep.cc/a.cc": "a\n"})
    _commit(repo, "B replaces file keep.cc with directory keep.cc/")


def _replace_keep_with_gitlink(repo: Path) -> None:
    head = _git(repo, "rev-parse", "HEAD")
    _git(repo, "rm", "-q", "keep.cc")
    _git(
        repo, "update-index", "--add", "--cacheinfo", f"160000,{head},keep.cc",
    )
    _git(repo, "commit", "-q", "-m", "B replaces file keep.cc with a gitlink")


@pytest.mark.parametrize("complete_mode", COMPLETE_MODES)
def test_owed_path_that_became_a_directory_is_retracted_without_a_crash(
    tmp_path: Path, complete_mode: str,
) -> None:
    repo, graph = _start(tmp_path)
    _replace_keep_with_directory(repo)
    graph.run(repo, "cursor")
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]
    assert "keep.cc/a.cc" in graph.nodes

    # An owed path that names a tree now is gone as a file, not a crash.
    graph.run(repo, "cursor")
    graph.run(repo, "scope_incomplete")
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]
    graph.run(repo, complete_mode)
    assert set(graph.nodes) == _tree_paths(repo) == {"keep.cc/a.cc", "other.cc"}
    # keep.cc/a.cc was read by the incomplete run, whose record set the
    # runner does not keep, so only the retracted path is checked here.
    assert "keep.cc" not in graph.record_keys()
    assert "owed_retraction_paths" not in graph.cursor


def test_owed_path_that_became_a_gitlink_is_retracted(tmp_path: Path) -> None:
    repo, graph = _start(tmp_path)
    _replace_keep_with_gitlink(repo)
    graph.run(repo, "cursor")
    assert graph.cursor.get("owed_retraction_paths") == ["keep.cc"]

    graph.run(repo, "cursor")
    graph.run(repo, "reconcile")
    assert set(graph.nodes) == {"other.cc"}
    assert "owed_retraction_paths" not in graph.cursor


def test_owed_path_under_a_directory_that_became_a_file_is_retracted(
    tmp_path: Path,
) -> None:
    repo, graph = _start(tmp_path)
    _write(repo, {"pkg/a.cc": "a\n"})
    _commit(repo, "B adds pkg/a.cc")
    graph.run(repo, "reconcile")
    assert "pkg/a.cc" in graph.nodes

    _git(repo, "rm", "-q", "pkg/a.cc")
    _write(repo, {"pkg": "now a file\n"})
    _commit(repo, "C replaces directory pkg/ with file pkg")
    graph.run(repo, "cursor")
    assert graph.cursor.get("owed_retraction_paths") == ["pkg/a.cc"]

    graph.run(repo, "cursor")
    graph.run(repo, "reconcile")
    assert set(graph.nodes) == _tree_paths(repo) == {"keep.cc", "other.cc", "pkg"}
    assert "owed_retraction_paths" not in graph.cursor


def test_whole_tree_complete_run_retracts_owed_paths_explicitly(
    tmp_path: Path,
) -> None:
    """A complete run that reads the whole tree (a cursor with no record set,
    as the external-repos factory hands its git-files sub-run) must still
    retract owed paths itself: a file first read by an incomplete run is in
    no record set the runner keeps, so the runner's diff cannot."""
    from okg.substrate.library.sources.base import (
        source_state_without_runner_keys,
    )

    repo, graph = _start(tmp_path)
    _write(repo, {"new.cc": "new\n"})
    _commit(repo, "B adds new.cc")
    graph.run(repo, "cursor")
    _git(repo, "rm", "-q", "new.cc")
    _commit(repo, "C deletes new.cc")
    graph.run(repo, "cursor")
    assert graph.cursor.get("owed_retraction_paths") == ["new.cc"]

    _fresh_repository_handles()
    run = _source(repo).run(
        "whole-tree",
        mode="scope_complete",
        cursor=source_state_without_runner_keys(graph.cursor),
    )
    retracted = {
        fact.node_id
        for fact in run.facts
        if getattr(fact, "subtype", None) == "source_file" and fact.op == "R"
    }
    assert run.completed_scope is True
    assert retracted == {"file:r:new.cc"}
    assert "owed_retraction_paths" not in run.next_cursor
