"""The external repos factory completes full runs through the real runner.

Issue #2939: `ExternalRepoFactory` declared a record set with no edges, so the
runner's coverage proof found the factory's own `member_of` edges (and every
edge its per-repo git-files, git-history and code-structure sources emit) in
the graph but not in the declaration, and refused the run:
`record_set_coverage ... stale_edges=N class=stale`. No completed-scope run --
full sync, replay, `--reset-cursor` -- could publish.

These tests drive the real factory over real git repositories through the
default `SourceRunner` and its Chronos graph store in `scope_complete` mode.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import threading
from pathlib import Path

import psycopg
import pygit2
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources import _git_graph_files_emit, _git_helpers
from okg.substrate.library.sources.base import SourceHealth, SourceRun
from okg.substrate.library.sources.external_repos import ExternalRepoFactory
from okg.substrate.library.sources.git_graph_files import GitFilesSource
from okg.substrate.library.sources.git_graph_history import GitHistorySource
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

SOURCE = "test.external-repos"


@pytest.fixture(autouse=True)
def _no_background_git_maintenance(monkeypatch: pytest.MonkeyPatch) -> None:
    # A fixture `git commit` may start auto-maintenance in the background,
    # and that process can still be writing under .git/objects while a
    # damage helper deletes the directory: rmtree then fails with
    # FileNotFoundError (substrate-db, train 27e). Explicit `git gc` and
    # `git repack` calls in the fixtures still run.
    monkeypatch.setenv("GIT_CONFIG_COUNT", "2")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "maintenance.auto")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "false")
    monkeypatch.setenv("GIT_CONFIG_KEY_1", "gc.auto")
    monkeypatch.setenv("GIT_CONFIG_VALUE_1", "0")


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(repo: Path, files: dict[str, str]) -> None:
    repo.mkdir(parents=True)
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(repo)],
        check=True,
    )
    _git(repo, "config", "user.email", "fixture@example.invalid")
    _git(repo, "config", "user.name", "fixture")
    for rel, content in files.items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    _git(repo, "add", "-A")
    _git(repo, "commit", "--quiet", "-m", "seed")


def _write_config(cfg: Path, repos: list[tuple[str, Path]]) -> None:
    lines = ["repos:"] if repos else ["repos: []"]
    for name, local_path in repos:
        lines.append(f"  - name: {name}")
        lines.append(f"    url: https://example.invalid/{name}.git")
        lines.append(f"    local_path: {local_path}")
    cfg.write_text("\n".join(lines) + "\n")


def _live(dsn: str) -> tuple[set[str], set[tuple[str, str, str]]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = ChronosGraphStore.from_connection(conn).checkout()
        nodes = {
            str(row["node_id"])
            for row in session.query(
                "SELECT node_id FROM okg.graph_nodes WHERE source = :source",
                {"source": SOURCE},
            )
        }
        edges = {
            (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
            for row in session.query(
                "SELECT src, edge_type, dst FROM okg.graph_edges "
                "WHERE source = :source",
                {"source": SOURCE},
            )
        }
    return nodes, edges


def _touches(ids: set[str], edge: tuple[str, str, str]) -> bool:
    return edge[0] in ids or edge[2] in ids


@pytest.fixture
def two_repos(tmp_path: Path) -> tuple[Path, Path, Path]:
    alpha = tmp_path / "alpha"
    beta = tmp_path / "beta"
    _init_repo(alpha, {
        "src/keep.py": "def keep():\n    return 1\n",
        "src/drop.py": (
            "class Dropped:\n"
            "    def run(self):\n"
            "        return 2\n"
        ),
        "README.md": "alpha repo\n",
    })
    _init_repo(beta, {
        "src/beta.py": "def beta():\n    return 3\n",
    })
    cfg = tmp_path / "external_repos.yaml"
    _write_config(cfg, [("alpha", alpha), ("beta", beta)])
    return cfg, alpha, beta


@pytest.fixture
def runner(clean_runtime: str, monkeypatch: pytest.MonkeyPatch) -> SourceRunner:
    # Every run below must be a real read. The change-probe gate would
    # otherwise short-circuit the no-change run before the coverage proof.
    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    return SourceRunner(clean_runtime, source_name=SOURCE, write_fact_log=True)


def test_scope_complete_run_publishes_and_declares_every_emitted_edge(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
) -> None:
    cfg, _alpha, _beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))

    # Before #2939 this raised `record_set_coverage ... class=stale`.
    first = runner.run(factory, mode="scope_complete")
    nodes, edges = _live(clean_runtime)

    assert first.retractions_node == 0
    assert first.retractions_edge == 0
    assert {"external_repo:alpha", "external_repo:beta"} <= nodes
    member_of = {edge for edge in edges if edge[1] == "member_of"}
    assert ("file:alpha:src/keep.py", "member_of", "external_repo:alpha") in member_of
    assert ("file:beta:src/beta.py", "member_of", "external_repo:beta") in member_of
    # The sub-sources' own edges publish too, not just the factory's.
    assert {edge[1] for edge in edges} - {"member_of"}

    # The record set keys stay one per repository, and together they declare
    # exactly the live source-owned edges.
    record_set = first.next_cursor["record_set"]["records"]
    assert set(record_set) == {"external_repo:alpha", "external_repo:beta"}
    declared_edges = {
        (edge["src"], edge["edge_type"], edge["dst"])
        for record in record_set.values()
        for edge in record["edges"]
    }
    assert declared_edges == edges
    # Each declared edge is its identity alone. The attrs and the per-run
    # revision (which embeds the run id) quadrupled the stored record set on
    # a large repository; coverage and retraction compare identities only.
    for record in record_set.values():
        for edge in record["edges"]:
            assert edge["attrs"] == {}
            assert edge["source_record_id"] == {}
            assert edge["source_revision"] == {}


def test_second_run_with_no_change_retracts_nothing(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
) -> None:
    cfg, _alpha, _beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    runner.run(factory, mode="scope_complete")
    before = _live(clean_runtime)

    second = runner.run(factory, mode="scope_complete")

    assert second.retractions_node == 0
    assert second.retractions_edge == 0
    assert _live(clean_runtime) == before


def test_removing_a_file_retracts_exactly_that_files_rows(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
) -> None:
    cfg, alpha, _beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    runner.run(factory, mode="scope_complete")
    nodes_before, edges_before = _live(clean_runtime)
    dropped_file = "file:alpha:src/drop.py"
    dropped_symbols = {
        node_id for node_id in nodes_before
        if node_id.startswith("symbol:alpha:src/drop.py:")
    }
    assert dropped_file in nodes_before
    assert dropped_symbols

    _git(alpha, "rm", "--quiet", "src/drop.py")
    _git(alpha, "commit", "--quiet", "-m", "drop")
    second = runner.run(factory, mode="scope_complete")
    nodes_after, edges_after = _live(clean_runtime)

    gone_nodes = nodes_before - nodes_after
    # The deletion is a new commit, so the branch head moves from the old
    # commit to the new one. That is the commit's row, not the file's.
    old_head = {
        edge for edge in edges_before - edges_after
        if edge[:2] == ("branch:alpha:main", "references")
    }
    assert len(old_head) == 1
    gone_edges = edges_before - edges_after - old_head
    # The file's tree rows go: its symbols, their edges, and its place on
    # the branch. Git history still names the file, so the file node itself
    # stays as history's record of it.
    assert gone_nodes == dropped_symbols
    assert (dropped_file, "member_of", "branch:alpha:main") in gone_edges
    assert all(
        _touches(dropped_symbols | {dropped_file}, edge) for edge in gone_edges
    ), gone_edges
    assert second.retractions_node == len(gone_nodes)
    assert second.retractions_edge == len(gone_edges) + len(old_head)
    # Nothing else moved: the kept file, the other repo, the prior history.
    assert nodes_before - dropped_symbols <= nodes_after
    assert edges_before - gone_edges - old_head <= edges_after
    assert (
        "file:alpha:src/keep.py", "member_of", "external_repo:alpha"
    ) in edges_after


def test_removing_a_repo_from_config_retracts_that_repos_rows(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
) -> None:
    cfg, alpha, _beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    runner.run(factory, mode="scope_complete")
    nodes_before, edges_before = _live(clean_runtime)
    beta_nodes = {n for n in nodes_before if n == "external_repo:beta" or ":beta:" in n}
    assert "file:beta:src/beta.py" in beta_nodes

    _write_config(cfg, [("alpha", alpha)])
    runner.run(factory, mode="scope_complete")
    nodes_after, edges_after = _live(clean_runtime)

    assert not (beta_nodes & nodes_after)
    assert not {edge for edge in edges_after if _touches(beta_nodes, edge)}
    # alpha is untouched.
    assert {n for n in nodes_before if n not in beta_nodes} <= nodes_after
    assert {
        edge for edge in edges_before if not _touches(beta_nodes, edge)
    } <= edges_after


def _registered_crawl_runner(dsn: str) -> SourceRunner:
    """A runner bound the way deployments bind the factory."""
    return SourceRunner(
        dsn,
        source_name=SOURCE,
        write_fact_log=True,
        executable_profile="discovery_crawl",
        registry_name=SOURCE,
        ownership_id=SOURCE,
        registry_binding_fingerprint="sha256:external-repos-failed-repo",
    )


def test_repo_with_no_commits_is_empty_not_failed(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    """A freshly created repository with no commits does not block full runs.

    Its HEAD cannot be read because it points at a branch that does not exist
    yet. That is a verified empty result, not a read failure, so a registered
    crawl runner publishes the full run instead of refusing it.
    """
    cfg, alpha, beta = two_repos
    gamma = tmp_path / "gamma"
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(gamma)],
        check=True,
    )
    _write_config(cfg, [("alpha", alpha), ("beta", beta), ("gamma", gamma)])
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    registered = _registered_crawl_runner(clean_runtime)

    first = registered.run(factory, mode="scope_complete")
    nodes, edges = _live(clean_runtime)

    assert first.completed_scope is True
    assert first.source_health is None or first.source_health.status == "ok"
    assert first.retractions_node == 0
    assert first.retractions_edge == 0
    assert {
        "external_repo:alpha", "external_repo:beta", "external_repo:gamma",
    } <= nodes
    assert "file:alpha:src/keep.py" in nodes
    assert not {n for n in nodes if ":gamma:" in n}

    second = registered.run(factory, mode="scope_complete")
    assert second.retractions_node == 0
    assert second.retractions_edge == 0
    assert _live(clean_runtime) == (nodes, edges)


def _git_init(repo: Path) -> None:
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(repo)],
        check=True,
    )


_ONE_COMMIT = {"src/a.py": "def a():\n    return 1\n"}
_MISSING_SHA = "f" * 40


# Each builder damages or creates a repository under `base` and returns the
# path git-files should read. None of them is a repository that never had
# commits, so git-files must report every one as partial, never as empty.
# The comment on each names the check in `head_is_verified_unborn` that is
# the ONLY one rejecting it, where one is; that makes each check's mutant
# fail at least one case.
def _refs_deleted(base: Path) -> Path:
    # Loose commit, no refs, empty pack dir: only the commit-object check.
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    (repo / ".git" / "refs" / "heads" / "main").unlink()
    return repo


def _ref_renamed_to_lock(base: Path) -> Path:
    # A crash left main.lock and no main: only the commit-object check.
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    heads = repo / ".git" / "refs" / "heads"
    (heads / "main").rename(heads / "main.lock")
    return repo


def _refs_only_under_remotes(base: Path) -> Path:
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    remotes = repo / ".git" / "refs" / "remotes" / "origin"
    remotes.mkdir(parents=True)
    (repo / ".git" / "refs" / "heads" / "main").rename(remotes / "main")
    return repo


def _head_to_missing_branch(base: Path) -> Path:
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    _git(repo, "symbolic-ref", "HEAD", "refs/heads/gone")
    return repo


def _orphan_worktree(base: Path) -> Path:
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    worktree = base / "orphan"
    _git(repo, "worktree", "add", "--quiet", "--detach", str(worktree))
    _git(worktree, "checkout", "--quiet", "--orphan", "orph")
    return worktree


def _garbage_head(base: Path) -> Path:
    # libgit2 opens it but HEAD is neither unborn nor readable.
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    (repo / ".git" / "HEAD").write_text("garbage\n")
    return repo


def _gitdir_missing(base: Path) -> Path:
    # A .git file points at a directory that is gone, so libgit2 cannot open
    # the repository at all: only the "an error is never empty" rule.
    repo = base / "repo"
    repo.mkdir()
    (repo / ".git").write_text(f"gitdir: {base / 'gone'}\n")
    (repo / "a.py").write_text("x = 1\n")
    return repo


def _killed_fetch_partial_pack(base: Path) -> Path:
    # An interrupted fetch left only a tmp_pack: only the pack-dir check.
    repo = base / "repo"
    _git_init(repo)
    (repo / ".git" / "objects" / "pack" / "tmp_pack_interrupted").write_bytes(
        b"PACK\x00\x00\x00\x02",
    )
    return repo


def _killed_fetch_full_pack(base: Path) -> Path:
    # The pack landed but the refs were never written.
    source = base / "source"
    _init_repo(source, _ONE_COMMIT)
    _git(source, "repack", "-a", "-d", "-q")
    repo = base / "repo"
    _git_init(repo)
    for pack_file in (source / ".git" / "objects" / "pack").iterdir():
        (repo / ".git" / "objects" / "pack" / pack_file.name).write_bytes(
            pack_file.read_bytes(),
        )
    return repo


def _dangling_ref_no_objects(base: Path) -> Path:
    # A ref exists but no object does: only the no-refs check.
    repo = base / "repo"
    _git_init(repo)
    (repo / ".git" / "refs" / "heads" / "other").write_text(_MISSING_SHA + "\n")
    return repo


def _detached_missing_no_objects(base: Path) -> Path:
    # HEAD is detached at a missing commit, nothing else exists: only the
    # unborn-HEAD check.
    repo = base / "repo"
    _git_init(repo)
    (repo / ".git" / "HEAD").write_text(_MISSING_SHA + "\n")
    return repo


def _drop_all_refs(repo: Path) -> None:
    git_dir = repo / ".git"
    for path in (git_dir / "refs" / "heads").iterdir():
        path.unlink()
    for name in ("refs/remotes", "logs"):
        shutil.rmtree(git_dir / name, ignore_errors=True)
    (git_dir / "packed-refs").unlink(missing_ok=True)


def _broken_child_in_unborn_parent(base: Path) -> Path:
    # The child's own .git lost its HEAD, so a search upward would answer
    # for the fresh parent: only the no-search open and the root check.
    parent = base / "parent"
    _git_init(parent)
    child = parent / "child"
    _init_repo(child, _ONE_COMMIT)
    (child / ".git" / "HEAD").unlink()
    return child


def _alternate_store_moved(base: Path) -> Path:
    # A --shared clone lost its refs and its alternate object store moved;
    # libgit2 skips the missing store silently: only the objects/info check.
    source = base / "source"
    _init_repo(source, _ONE_COMMIT)
    _git(source, "gc", "--quiet")
    repo = base / "repo"
    subprocess.run(
        ["git", "clone", "--quiet", "--shared", str(source), str(repo)],
        check=True,
    )
    _drop_all_refs(repo)
    source.rename(base / "source-moved")
    return repo


def _commit_graph_left(base: Path) -> Path:
    # Refs and objects are gone but the commit-graph remains: only the
    # objects/info check.
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    _git(repo, "gc", "--quiet")
    _git(repo, "commit-graph", "write", "--reachable")
    _drop_all_refs(repo)
    objects = repo / ".git" / "objects"
    for path in (objects / "pack").iterdir():
        path.unlink()
    for path in objects.iterdir():
        if len(path.name) == 2:
            shutil.rmtree(path)
    return repo


def _worktree_with_shared_tmp_pack(base: Path) -> Path:
    # A linked worktree of a fresh repository whose SHARED object store holds
    # an interrupted fetch's tmp_pack: the pack check must read the common
    # git directory, not the worktree's own.
    main = base / "main"
    _git_init(main)
    admin = main / ".git" / "worktrees" / "wt"
    admin.mkdir(parents=True)
    worktree = base / "wt"
    worktree.mkdir()
    (admin / "HEAD").write_text("ref: refs/heads/wt\n")
    (admin / "commondir").write_text("../..\n")
    (admin / "gitdir").write_text(f"{worktree / '.git'}\n")
    (worktree / ".git").write_text(f"gitdir: {admin}\n")
    (main / ".git" / "objects" / "pack" / "tmp_pack_interrupted").write_bytes(
        b"PACK\x00\x00\x00\x02",
    )
    return worktree


_PARENT_FILES = {"PARENT_ONLY.md": "parent\n", "p/x.py": "x = 1\n"}


def _damage_child_git(child: Path, damage: str) -> None:
    """Damage a child repository's own .git so it no longer opens."""
    git_dir = child / ".git"
    if damage == "head_deleted":
        (git_dir / "HEAD").unlink()
    elif damage == "git_emptied":
        shutil.rmtree(git_dir)
        git_dir.mkdir()
    elif damage == "objects_removed":
        shutil.rmtree(git_dir / "objects")
    else:  # pragma: no cover - a typo in a caller
        raise AssertionError(damage)


def _child_in_committed_parent(base: Path, damage: str) -> Path:
    # A parent WITH commits holds a child whose own .git is damaged. A
    # parent-directory search opens the parent and reads its HEAD, so the
    # parent's files would be emitted under the child's name: only the exact
    # root open (`open_repository_at`) rejects it.
    parent = base / "parent"
    _init_repo(parent, _PARENT_FILES)
    child = parent / "child"
    _init_repo(child, _ONE_COMMIT)
    _damage_child_git(child, damage)
    return child


def _reflog_only_left(base: Path) -> Path:
    # Objects, packs, refs and index are gone; the reflog still records the
    # commit: only the reflog check.
    repo = base / "repo"
    _init_repo(repo, _ONE_COMMIT)
    git_dir = repo / ".git"
    (git_dir / "refs" / "heads" / "main").unlink()
    (git_dir / "index").unlink()
    objects = git_dir / "objects"
    for path in objects.iterdir():
        if len(path.name) == 2:
            shutil.rmtree(path)
    assert (git_dir / "logs" / "HEAD").is_file()
    return repo


def _shallow_only_left(base: Path) -> Path:
    # A shallow clone lost its objects, packs, refs, index and reflogs; the
    # shallow file still lists its boundary commit: only the shallow check.
    source = base / "source"
    _init_repo(source, _ONE_COMMIT)
    repo = base / "repo"
    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1",
         f"file://{source}", str(repo)],
        check=True,
    )
    _drop_all_refs(repo)
    git_dir = repo / ".git"
    (git_dir / "index").unlink(missing_ok=True)
    (git_dir / "logs").mkdir(exist_ok=True)
    objects = git_dir / "objects"
    for path in (objects / "pack").iterdir():
        path.unlink()
    for path in objects.iterdir():
        if len(path.name) == 2:
            shutil.rmtree(path)
    assert (git_dir / "shallow").read_text().strip()
    return repo


_DAMAGED_REPOS = {
    "child_head_deleted_in_committed_parent": (
        lambda base: _child_in_committed_parent(base, "head_deleted")
    ),
    "child_git_emptied_in_committed_parent": (
        lambda base: _child_in_committed_parent(base, "git_emptied")
    ),
    "child_objects_removed_in_committed_parent": (
        lambda base: _child_in_committed_parent(base, "objects_removed")
    ),
    "reflog_only_left": _reflog_only_left,
    "shallow_only_left": _shallow_only_left,
    "broken_child_in_unborn_parent": _broken_child_in_unborn_parent,
    "alternate_store_moved": _alternate_store_moved,
    "commit_graph_left": _commit_graph_left,
    "worktree_with_shared_tmp_pack": _worktree_with_shared_tmp_pack,
    "refs_deleted": _refs_deleted,
    "ref_renamed_to_lock": _ref_renamed_to_lock,
    "refs_only_under_remotes": _refs_only_under_remotes,
    "head_to_missing_branch": _head_to_missing_branch,
    "orphan_worktree": _orphan_worktree,
    "garbage_head": _garbage_head,
    "gitdir_missing": _gitdir_missing,
    "killed_fetch_partial_pack": _killed_fetch_partial_pack,
    "killed_fetch_full_pack": _killed_fetch_full_pack,
    "dangling_ref_no_objects": _dangling_ref_no_objects,
    "detached_missing_no_objects": _detached_missing_no_objects,
}


@pytest.mark.parametrize("case", sorted(_DAMAGED_REPOS))
def test_git_files_damaged_repo_stays_partial(tmp_path: Path, case: str) -> None:
    """A repository that has, or had, commits is never read as empty."""
    root = _DAMAGED_REPOS[case](tmp_path)
    source = GitFilesSource(repo="damaged", repo_root=str(root))

    run = source.run("run-1", mode="scope_complete", cursor=None)

    assert list(run.facts) == []
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.health is not None, case
    assert run.health.status == "partial", case
    assert "damaged" in (run.health.reason or "")


@pytest.mark.parametrize("shape", ["plain", "staged", "linked_worktree"])
def test_git_files_repo_that_never_had_commits_is_empty(
    tmp_path: Path,
    shape: str,
) -> None:
    """A fresh repository -- with staged files, or a linked worktree of one --
    reads as an empty tree."""
    if shape == "linked_worktree":
        repo = _worktree_with_shared_tmp_pack(tmp_path)
        for path in (tmp_path / "main" / ".git" / "objects" / "pack").iterdir():
            path.unlink()
    else:
        repo = tmp_path / "fresh"
        _git_init(repo)
    if shape == "staged":
        (repo / "a.py").write_text("x = 1\n")
        _git(repo, "add", "a.py")
    assert _git_graph_files_emit.head_is_verified_unborn(repo)
    source = GitFilesSource(repo="fresh", repo_root=str(repo))

    run = source.run("run-1", mode="scope_complete", cursor=None)

    assert list(run.facts) == []
    assert run.health is None


def test_git_files_fresh_repo_that_had_commits_stays_partial(
    tmp_path: Path,
) -> None:
    """A prior cursor that saw a HEAD forbids reading the repository as empty."""
    repo = tmp_path / "reinit"
    _git_init(repo)
    source = GitFilesSource(repo="reinit", repo_root=str(repo))
    prior = {"head_sha": "a" * 40, "tree_sha": "b" * 40}

    run = source.run("run-1", mode="scope_complete", cursor=prior)

    assert list(run.facts) == []
    assert run.completed_scope is False
    assert run.next_cursor == prior
    assert run.health is not None
    assert run.health.status == "partial"
    assert "had commits before, now has no readable HEAD" in (
        run.health.reason or ""
    )


def _damage_in_place(repo: Path, case: str) -> None:
    """Damage an already-published repository at its own path."""
    git_dir = repo / ".git"
    if case == "refs_deleted":
        (git_dir / "refs" / "heads" / "main").unlink()
    elif case == "ref_renamed_to_lock":
        heads = git_dir / "refs" / "heads"
        (heads / "main").rename(heads / "main.lock")
    elif case == "refs_only_under_remotes":
        remotes = git_dir / "refs" / "remotes" / "origin"
        remotes.mkdir(parents=True)
        (git_dir / "refs" / "heads" / "main").rename(remotes / "main")
    elif case == "head_to_missing_branch":
        _git(repo, "symbolic-ref", "HEAD", "refs/heads/gone")
    elif case == "garbage_head":
        (git_dir / "HEAD").write_text("garbage\n")
    elif case == "reinitialized":
        # Every commit is gone and the repository is fresh again: it passes
        # every object-level check, so only the prior cursor refuses it.
        shutil.rmtree(git_dir)
        _git_init(repo)
    else:  # pragma: no cover - a typo in the parametrize list
        raise AssertionError(case)


@pytest.mark.parametrize(
    "case",
    [
        "refs_deleted",
        "ref_renamed_to_lock",
        "refs_only_under_remotes",
        "head_to_missing_branch",
        "garbage_head",
        "reinitialized",
    ],
)
def test_damaged_published_repo_keeps_its_rows(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
    case: str,
) -> None:
    """A published repository that loses its HEAD keeps every row.

    Round 3 read a repository with deleted or lock-renamed refs as "never
    had commits", skipped its sub-sources, and a full run retracted every
    row but the repo node.
    """
    cfg, _alpha, beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    runner.run(factory, mode="scope_complete")
    before = _live(clean_runtime)

    _damage_in_place(beta, case)
    partial = runner.run(factory, mode="scope_complete")

    assert _live(clean_runtime) == before
    assert partial.retractions_node == 0
    assert partial.retractions_edge == 0
    assert partial.source_health is not None
    assert partial.source_health.status == "partial"
    assert "beta" in (partial.source_health.reason or "")
    if case == "reinitialized":
        assert "had commits before, now has no readable HEAD" in (
            partial.source_health.reason or ""
        )


def _fresh_process_repo_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop every thread's cached repository handle, as a new process would.

    A warm handle opened before the damage keeps reading the old repository,
    which hides a read that would go to the parent in a new process.
    """
    monkeypatch.setattr(_git_helpers, "_repo_tls", threading.local())


@pytest.mark.parametrize(
    "damage", ["head_deleted", "git_emptied", "objects_removed"],
)
def test_damaged_nested_repo_is_not_read_as_its_parent(
    clean_runtime: str,
    runner: SourceRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    """A published repository inside a parent with commits, damaged in place,
    keeps its rows and is never read as the parent.

    Round 5: git-files opened the damaged child with a parent search, read
    the PARENT's HEAD, published the parent's files as the child's and
    retracted the child's own rows.
    """
    parent = tmp_path / "parent"
    _init_repo(parent, _PARENT_FILES)
    alpha = parent / "alpha"
    _init_repo(alpha, {"src/alpha.py": "def alpha():\n    return 1\n"})
    beta = tmp_path / "beta"
    _init_repo(beta, {"src/beta.py": "def beta():\n    return 3\n"})
    cfg = tmp_path / "external_repos.yaml"
    _write_config(cfg, [("alpha", alpha), ("beta", beta)])
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    _fresh_process_repo_cache(monkeypatch)
    runner.run(factory, mode="scope_complete")
    before = _live(clean_runtime)
    assert "file:alpha:src/alpha.py" in before[0]

    _damage_child_git(alpha, damage)
    _fresh_process_repo_cache(monkeypatch)
    partial = runner.run(factory, mode="scope_complete")
    nodes, edges = _live(clean_runtime)

    assert not {
        n for n in nodes if "PARENT_ONLY" in n or n.startswith("file:alpha:p/")
    }
    assert (nodes, edges) == before
    assert partial.retractions_node == 0
    assert partial.retractions_edge == 0
    assert partial.source_health is not None
    assert partial.source_health.status == "partial"
    assert "alpha" in (partial.source_health.reason or "")


def test_fresh_repo_nested_in_committed_parent_is_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new repository with no commits inside a parent WITH commits still
    reads as verified empty, not as the parent and not as a failure."""
    parent = tmp_path / "parent"
    _init_repo(parent, _PARENT_FILES)
    child = parent / "child"
    _git_init(child)
    _fresh_process_repo_cache(monkeypatch)
    assert _git_graph_files_emit.head_is_verified_unborn(child)
    source = GitFilesSource(repo="child", repo_root=str(child))

    run = source.run("run-1", mode="scope_complete", cursor=None)

    assert list(run.facts) == []
    assert run.health is None


@pytest.mark.parametrize(
    ("where", "line", "expected"),
    [
        ("worktree", f"{'0' * 40} {'a' * 40} f <f@x> 0 +0000\tcommit", True),
        ("common", f"{'0' * 40} {'a' * 40} f <f@x> 0 +0000\tcommit", True),
        ("worktree", "truncated", True),
        ("worktree", f"{'0' * 40} {'0' * 40} f <f@x> 0 +0000\tcheckout", False),
    ],
)
def test_reflog_trace_reads_both_git_dirs_and_doubts_bad_lines(
    tmp_path: Path, where: str, line: str, expected: bool,
) -> None:
    """A linked worktree keeps its HEAD reflog in its own git directory, and
    a line that does not parse is doubt, not "empty"."""
    git_dir = tmp_path / "common" / "worktrees" / "wt"
    common_dir = tmp_path / "common"
    target = git_dir if where == "worktree" else common_dir
    (target / "logs").mkdir(parents=True)
    (target / "logs" / "HEAD").write_text(line + "\n")
    git_dir.mkdir(parents=True, exist_ok=True)

    assert _git_graph_files_emit._shallow_or_reflog_records_commits(
        git_dir, common_dir,
    ) is expected


def _case_insensitive_fs(directory: Path) -> bool:
    probe = directory / "CaseProbe"
    probe.mkdir()
    try:
        return (directory / "caseprobe").exists()
    finally:
        probe.rmdir()


@pytest.mark.parametrize("bare", [False, True], ids=["worktree", "bare"])
def test_open_repository_at_accepts_a_case_changed_path(
    tmp_path: Path, bare: bool,
) -> None:
    """On a case-insensitive filesystem a repository opened through a path
    whose case differs from the on-disk name is the same repository."""
    if not _case_insensitive_fs(tmp_path):
        pytest.skip(
            "needs a case-insensitive filesystem (macOS default); "
            f"{tmp_path} is case-sensitive"
        )
    if bare:
        subprocess.run(
            ["git", "init", "--quiet", "--bare", str(tmp_path / "B.git")],
            check=True,
        )
        typed = tmp_path / "b.git"
    else:
        _init_repo(tmp_path / "Repo", _ONE_COMMIT)
        typed = tmp_path / "repo"

    repo = _git_helpers.open_repository_at(typed)

    assert repo.is_bare is bare


def test_open_repository_at_refuses_a_nested_path_it_is_not_rooted_at(
    tmp_path: Path,
) -> None:
    """A child directory whose own .git is damaged is not the parent: the
    open fails loudly instead of answering for the parent."""
    child = _child_in_committed_parent(tmp_path, "head_deleted")
    plain = tmp_path / "parent" / "p"

    for path in (child, plain):
        with pytest.raises(pygit2.GitError):
            _git_helpers.open_repository_at(path)
    # The parent itself still opens.
    assert _git_helpers.open_repository_at(tmp_path / "parent").workdir


def test_pull_never_touches_the_parent_of_a_damaged_checkout(
    tmp_path: Path,
) -> None:
    """A managed checkout whose own .git is damaged fails its acquisition.

    `git -C` searches parent directories, so the pull used to re-point the
    PARENT repository's origin to the child's URL and pull into it.
    """
    parent = tmp_path / "parent"
    _init_repo(parent, _PARENT_FILES)
    _git(parent, "remote", "add", "origin", "https://example.invalid/parent.git")
    child = parent / "child"
    _init_repo(child, _ONE_COMMIT)
    _damage_child_git(child, "head_deleted")
    parent_head = (parent / ".git" / "HEAD").read_text()

    outcome = _git_helpers.clone_or_pull(
        child, "https://example.invalid/child.git", timeout=30,
    )

    origin = subprocess.run(
        ["git", "-C", str(parent), "remote", "get-url", "origin"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    assert origin == "https://example.invalid/parent.git"
    assert (parent / ".git" / "HEAD").read_text() == parent_head
    assert outcome.status == "failed"
    assert "not a git repository rooted there" in (outcome.reason or "")


def test_acquire_stale_sub_run_is_not_a_repo_failure() -> None:
    """A stale checkout still indexed its whole tree; a failed one did not."""
    stale = SourceRun(
        facts=[],
        health=SourceHealth(status="acquire_stale", reason="fetch failed"),
    )
    ExternalRepoFactory._raise_on_degraded_sub_run("git_files", stale)

    failed = SourceRun(
        facts=[],
        health=SourceHealth(status="acquire_failed", reason="clone failed"),
    )
    with pytest.raises(RuntimeError, match="git_files sub-source reported acquire_failed"):
        ExternalRepoFactory._raise_on_degraded_sub_run("git_files", failed)


def _fail_git_files_head_for(
    monkeypatch: pytest.MonkeyPatch,
    repo_root: Path,
) -> None:
    """Make git-files' HEAD read raise for one checkout."""
    original = _git_graph_files_emit.in_proc_head_snapshot

    def _snapshot(root, *args, **kwargs):  # type: ignore[no-untyped-def]
        if Path(root).resolve() == repo_root.resolve():
            raise RuntimeError("transient HEAD read failure")
        return original(root, *args, **kwargs)

    monkeypatch.setattr(
        _git_graph_files_emit, "in_proc_head_snapshot", _snapshot,
    )


def test_git_files_head_failure_is_partial_not_an_empty_tree(
    two_repos: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Standalone git-files reports a failed HEAD read instead of hiding it.

    Before the fix it logged "git rev-parse failed ... emitting nothing" and
    returned an ordinary run, so a full run read the empty crawl as a tree
    with no files.
    """
    _cfg, _alpha, beta = two_repos
    _fail_git_files_head_for(monkeypatch, beta)
    source = GitFilesSource(repo="beta", repo_root=str(beta))
    prior_cursor = {"tree_sha": "0" * 40, "dirty_paths": []}

    run = source.run("run-1", mode="scope_complete", cursor=prior_cursor)

    assert list(run.facts) == []
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.next_cursor == prior_cursor
    assert run.health is not None
    assert run.health.status == "partial"
    assert "beta" in (run.health.reason or "")
    assert "transient HEAD read failure" in (run.health.reason or "")


def test_git_files_head_failure_keeps_the_repos_rows(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A git-files failure inside the factory fails that repository.

    Before the fix the factory composed git-files' empty crawl as success, so
    a full run retracted every edge only git-files emits for the repository
    (its files' ``member_of`` edges to the branch).
    """
    cfg, _alpha, beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    runner.run(factory, mode="scope_complete")
    nodes_before, edges_before = _live(clean_runtime)
    assert (
        "file:beta:src/beta.py", "member_of", "branch:beta:main"
    ) in edges_before

    with monkeypatch.context() as patch:
        _fail_git_files_head_for(patch, beta)
        partial = runner.run(factory, mode="scope_complete")
    nodes_after, edges_after = _live(clean_runtime)

    assert nodes_before <= nodes_after
    assert edges_before <= edges_after
    assert partial.retractions_node == 0
    assert partial.retractions_edge == 0
    assert partial.source_health is not None
    assert partial.source_health.status == "partial"
    reason = partial.source_health.reason or ""
    assert "beta" in reason
    assert "git_files sub-source reported partial" in reason
    assert "transient HEAD read failure" in reason


def _fail_git_history_for(
    monkeypatch: pytest.MonkeyPatch,
    repo_name: str,
) -> None:
    """Make one repository's git-history sub-run raise, as a transient git
    failure would."""
    original = GitHistorySource.run

    def _run(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self.repo == repo_name:
            raise RuntimeError("transient git failure")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(GitHistorySource, "run", _run)


def test_failed_repo_sub_run_keeps_its_rows_and_reports_partial(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repository whose sub-run fails is not declared empty.

    Before the fix the factory still put beta's record, holding only the
    repo node, into the full-run record set, so one git failure retracted
    every beta row except ``external_repo:beta``; the error survived only in
    the cursor's ``errors`` field.
    """
    cfg, alpha, _beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    runner.run(factory, mode="scope_complete")
    nodes_before, edges_before = _live(clean_runtime)
    beta_nodes = {
        n for n in nodes_before if n == "external_repo:beta" or ":beta:" in n
    }
    assert "file:beta:src/beta.py" in beta_nodes
    beta_edges = {e for e in edges_before if _touches(beta_nodes, e)}
    dropped_symbols = {
        n for n in nodes_before if n.startswith("symbol:alpha:src/drop.py:")
    }
    assert dropped_symbols

    # alpha really changes in the same run: the partial run must not act on
    # absence anywhere, and a later clean run must still catch it up.
    _git(alpha, "rm", "--quiet", "src/drop.py")
    _git(alpha, "commit", "--quiet", "-m", "drop")
    with monkeypatch.context() as patch:
        _fail_git_history_for(patch, "beta")
        partial = runner.run(factory, mode="scope_complete")
    nodes_mid, edges_mid = _live(clean_runtime)

    # Nothing was retracted: beta keeps every row, and alpha's deleted file
    # waits for a run that saw the whole scope.
    assert beta_nodes <= nodes_mid
    assert beta_edges <= edges_mid
    assert nodes_before <= nodes_mid
    assert edges_before <= edges_mid
    assert partial.retractions_node == 0
    assert partial.retractions_edge == 0
    # The failure is on the run's health, not only in the cursor.
    assert partial.completed_scope is False
    assert partial.source_health is not None
    assert partial.source_health.status == "partial"
    assert "beta" in (partial.source_health.reason or "")
    assert "transient git failure" in (partial.source_health.reason or "")
    assert "beta" in partial.next_cursor["errors"]

    recovered = runner.run(factory, mode="scope_complete")
    nodes_after, edges_after = _live(clean_runtime)

    assert recovered.completed_scope is True
    assert (
        recovered.source_health is None
        or recovered.source_health.status == "ok"
    )
    assert beta_nodes <= nodes_after
    assert beta_edges <= edges_after
    assert not (dropped_symbols & nodes_after)
    assert recovered.retractions_node == len(dropped_symbols)


def test_failed_repo_sub_run_is_refused_by_a_registered_crawl_runner(
    clean_runtime: str,
    runner: SourceRunner,
    two_repos: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A registered discovery-crawl runner refuses the partial full run.

    Deployments bind the factory under its `discovery_crawl` profile. A
    forced-full run that did not see every repository has no deletion
    authority, so that runner refuses it before writing anything.
    """
    cfg, _alpha, _beta = two_repos
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    registered = SourceRunner(
        clean_runtime,
        source_name=SOURCE,
        write_fact_log=True,
        executable_profile="discovery_crawl",
        registry_name=SOURCE,
        ownership_id=SOURCE,
        registry_binding_fingerprint="sha256:external-repos-failed-repo",
    )
    registered.run(factory, mode="scope_complete")
    before = _live(clean_runtime)

    with monkeypatch.context() as patch, caplog.at_level(
        logging.WARNING,
        logger="okg.substrate.library.sources.external_repos.factory",
    ):
        _fail_git_history_for(patch, "beta")
        with pytest.raises(
            SubstrateError,
            match="lacks complete deletion authority",
        ) as refused:
            registered.run(factory, mode="scope_complete")

    # The refusal names the failed repository and its error, and tells the
    # operator what to fix rather than pointing at adapter code.
    error = refused.value
    assert "beta" in error.message
    assert "transient git failure" in error.message
    assert error.data["source_health_status"] == "partial"
    assert "beta" in error.data["source_health_reason"]
    assert "transient git failure" in error.data["source_health_reason"]
    assert error.fix_hint is not None
    assert "sub-source failed" in error.fix_hint
    assert "fix or remove the failing repository" in error.fix_hint
    # The factory logs one warning per failed repository.
    warnings = [
        record for record in caplog.records
        if record.levelno == logging.WARNING
        and "sub-run failed" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert "repo=beta" in warnings[0].getMessage()
    assert "transient git failure" in warnings[0].getMessage()

    assert _live(clean_runtime) == before
    clean = registered.run(factory, mode="scope_complete")
    assert clean.retractions_node == 0
    assert clean.retractions_edge == 0
    assert _live(clean_runtime) == before
