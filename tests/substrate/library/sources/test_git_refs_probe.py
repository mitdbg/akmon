"""GitRefsProbe (#323) — ref-tips change-identity for a git history walk.

A history walk's emission is a function of the full ref-tip set, not the HEAD
tree: a branch/tag advance, create, or delete changes reachable history; a dirty
working-tree edit (uncommitted) does NOT. GitHistorySource declares this probe.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from okg.substrate.library.sources.change_probe import (
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.git_graph_history import GitHistorySource
from okg.substrate.library.sources.git_refs_probe import GitRefsProbe


def _git(d, *args):
    subprocess.run(
        ["git", "-C", str(d), *args], check=True, capture_output=True, text=True
    )


@pytest.fixture
def repo(tmp_path):
    d = tmp_path / "repo"
    d.mkdir()
    (d / "x.py").write_text("print(1)\n")
    _git(d, "init", "-q")
    _git(d, "config", "user.email", "t@t")
    _git(d, "config", "user.name", "t")
    _git(d, "add", "-A")
    _git(d, "commit", "-q", "-m", "c1")
    return d


def test_declares_git_refs_probe(repo):
    assert GitHistorySource.change_probe_kind == "git_refs"
    s = GitHistorySource(repo="demo", repo_root=str(repo))
    assert isinstance(s.change_probe, GitRefsProbe)
    validate_token_composition(s.change_probe.build_token())


def test_mtime_churn_is_immune(repo):
    s = GitHistorySource(repo="demo", repo_root=str(repo))
    t0 = s.change_probe.build_token()
    os.utime(repo / "x.py", (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token())


def test_dirty_uncommitted_edit_is_ignored(repo):
    # The history walk only sees committed history, so an uncommitted edit must
    # NOT bust the token (this is correct, not a missed change).
    s = GitHistorySource(repo="demo", repo_root=str(repo))
    t0 = s.change_probe.build_token()
    (repo / "x.py").write_text("print(2)\n")
    assert tokens_equal(t0, s.change_probe.build_token())


def test_source_file_exclusion_config_busts_token(repo):
    unfiltered = GitHistorySource(repo="demo", repo_root=str(repo))
    filtered = GitHistorySource(
        repo="demo",
        repo_root=str(repo),
        source_file_exclude_globs=["x.py"],
    )

    assert not tokens_equal(
        unfiltered.change_probe.build_token(),
        filtered.change_probe.build_token(),
    )


def test_commit_advance_busts_token(repo):
    s = GitHistorySource(repo="demo", repo_root=str(repo))
    t0 = s.change_probe.build_token()
    (repo / "y.py").write_text("print(3)\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "c2")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_branch_create_and_delete_busts_token(repo):
    s = GitHistorySource(repo="demo", repo_root=str(repo))
    t0 = s.change_probe.build_token()
    _git(repo, "branch", "feature/x")  # new ref entry (+membership)
    t1 = s.change_probe.build_token()
    assert not tokens_equal(t0, t1)
    _git(repo, "branch", "-D", "feature/x")  # entry drops (-membership)
    assert not tokens_equal(t1, s.change_probe.build_token())


def test_tag_add_busts_token(repo):
    s = GitHistorySource(repo="demo", repo_root=str(repo))
    t0 = s.change_probe.build_token()
    _git(repo, "tag", "v1")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_annotated_tag_replacement_busts_token_and_binds_exact_emit(repo):
    source = GitHistorySource(repo="demo", repo_root=str(repo))
    _git(repo, "tag", "v1")
    lightweight = source.change_probe.build_token()

    _git(repo, "tag", "-d", "v1")
    _git(repo, "tag", "-a", "v1", "-m", "annotated")
    annotated = source.change_probe.build_token()
    assert not tokens_equal(lightweight, annotated)

    run = source.run("annotated", mode="scope_complete")
    facts = list(run.facts)
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert tokens_equal(run.applied_raw_token, annotated)
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "tag:demo:v1"
        and fact.attrs["is_annotated"] is True
        for fact in facts
    )


def test_ref_tips_helper_scopes_remotes_by_flag(repo, tmp_path):
    # The helper itself honors the flag. Callers choose the scope: GitRefsProbe
    # always asks for remotes (the history walk always reaches them) -- see
    # `test_probe_busts_on_remote_only_advance_regardless_of_include_remotes`.
    from okg.substrate.library.sources._git_helpers import in_proc_ref_tips

    upstream = tmp_path / "upstream"
    upstream.mkdir()
    _git(upstream, "init", "-q", "--bare")
    _git(repo, "remote", "add", "origin", str(upstream))
    _git(repo, "push", "-q", "origin", "HEAD:refs/heads/main")
    _git(repo, "fetch", "-q", "origin")

    local = in_proc_ref_tips(repo, include_remotes=False)
    withr = in_proc_ref_tips(repo, include_remotes=True)
    assert not any(n.startswith("refs/remotes/") for n in local)
    assert any(n.startswith("refs/remotes/") for n in withr)


def test_probe_busts_on_remote_only_advance_regardless_of_include_remotes(repo):
    """Probe scope must be a SUPERSET of walk scope, never narrower.

    Ref enumeration is the revwalk's positive set, so a remote-only commit is
    reachable and emitted whatever `include_remotes` says. `include_remotes`
    gates BRANCH-NODE emission alone. A probe that skipped remote tips when
    `include_remotes=False` would let that commit enter or leave the record set
    without busting the token -- and on the next completed-scope run the diff
    would retract it. On a clone (what `external_repos` always produces) every
    branch but one lives under `refs/remotes/`, so that is most of history.
    """
    without_remotes = GitHistorySource(
        repo="demo",
        repo_root=repo,
        include_remotes=False,
    )
    with_remotes = GitHistorySource(
        repo="demo",
        repo_root=repo,
        include_remotes=True,
    )
    local_token = without_remotes.change_probe.build_token()
    remote_token = with_remotes.change_probe.build_token()

    hidden_sha = subprocess.check_output(
        ["git", "-C", str(repo), "commit-tree", "HEAD^{tree}", "-m", "remote only"],
        text=True,
    ).strip()
    _git(repo, "update-ref", "refs/remotes/origin/hidden", hidden_sha)

    # Both settings walk the new commit, so both tokens must change.
    assert not tokens_equal(local_token, without_remotes.change_probe.build_token())
    assert not tokens_equal(remote_token, with_remotes.change_probe.build_token())

    local_facts = list(without_remotes.run("local", mode="scope_complete").facts)
    remote_facts = list(with_remotes.run("remote", mode="scope_complete").facts)
    hidden_id = f"commit:demo:{hidden_sha}"

    # The COMMIT is emitted either way -- it is reachable.
    for facts in (local_facts, remote_facts):
        assert any(
            isinstance(fact, NodeFact) and fact.node_id == hidden_id
            for fact in facts
        )

    # The BRANCH NODE is what `include_remotes` gates.
    branch_id = "branch:demo:origin/hidden"
    assert not any(
        isinstance(fact, NodeFact) and fact.node_id == branch_id
        for fact in local_facts
    )
    assert any(
        isinstance(fact, NodeFact) and fact.node_id == branch_id
        for fact in remote_facts
    )


def test_probe_and_emitter_include_unreferenced_detached_head(repo):
    source = GitHistorySource(repo="demo", repo_root=repo)
    attached_token = source.change_probe.build_token()
    branch = subprocess.check_output(
        ["git", "-C", str(repo), "branch", "--show-current"],
        text=True,
    ).strip()
    attached_sha = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip()

    (repo / "detached.py").write_text("detached = True\n", encoding="utf-8")
    _git(repo, "add", "detached.py")
    _git(repo, "commit", "-q", "-m", "detached commit")
    detached_sha = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    _git(repo, "checkout", "-q", "--detach", detached_sha)
    _git(repo, "branch", "-f", branch, attached_sha)

    assert not tokens_equal(attached_token, source.change_probe.build_token())
    facts = list(source.run("detached", mode="scope_complete").facts)
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == f"commit:demo:{detached_sha}"
        for fact in facts
    )


def _add_remote_with_symbolic_head(repo, tmp_path):
    upstream = tmp_path / "upstream-sealed"
    upstream.mkdir()
    _git(upstream, "init", "-q", "--bare")
    _git(repo, "remote", "add", "origin", str(upstream))
    _git(repo, "push", "-q", "origin", "HEAD:refs/heads/main")
    _git(repo, "fetch", "-q", "origin")
    _git(
        repo,
        "symbolic-ref",
        "refs/remotes/origin/HEAD",
        "refs/remotes/origin/main",
    )


def test_unmoved_refs_give_equal_applied_and_probe_tokens(repo, tmp_path):
    # The post-run probe and the applied token cover the same ref set (local,
    # tags, remotes incl. a symbolic remote HEAD), so equal inputs give equal
    # tokens and an unmoved run is acknowledged by plain equality.
    _add_remote_with_symbolic_head(repo, tmp_path)
    _git(repo, "tag", "-a", "v1", "-m", "annotated")
    source = GitHistorySource(repo="demo", repo_root=repo)

    run = source.run("unmoved", mode="scope_complete")
    list(run.facts)

    assert run.applied_token_binding == "sealed_snapshot"
    assert tokens_equal(
        run.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_ref_moved_mid_run_keeps_sealed_token_of_walked_tips(
    repo,
    tmp_path,
    monkeypatch,
):
    # Another checkout of the same clone pushes a branch after the adapter
    # listed the ref tips. The walk used the pinned tips, so the applied
    # token must describe those tips -- not the live refs -- and the run
    # declares that promise as a sealed snapshot.
    from okg.substrate.library.sources import _git_graph_history_emit

    _add_remote_with_symbolic_head(repo, tmp_path)
    source = GitHistorySource(repo="demo", repo_root=repo)
    before = source.change_probe.build_token()
    moved_sha = subprocess.check_output(
        ["git", "-C", str(repo), "commit-tree", "HEAD^{tree}", "-m", "moved"],
        text=True,
    ).strip()
    original_list_refs = _git_graph_history_emit.GitHistorySource._list_refs

    def _list_refs_then_move(self):
        refs = original_list_refs(self)
        _git(repo, "update-ref", "refs/remotes/origin/moved", moved_sha)
        return refs

    monkeypatch.setattr(
        _git_graph_history_emit.GitHistorySource,
        "_list_refs",
        _list_refs_then_move,
    )

    run = source.run("moved", mode="scope_complete")
    facts = list(run.facts)
    after = source.change_probe.build_token()

    assert run.applied_token_binding == "sealed_snapshot"
    assert run.applied_token_authority == "exact"
    assert run.applied_token_scope == "global"
    assert tokens_equal(run.applied_raw_token, before)
    assert not tokens_equal(run.applied_raw_token, after)
    assert not any(
        isinstance(fact, NodeFact)
        and fact.node_id == f"commit:demo:{moved_sha}"
        for fact in facts
    )
    assert source.last_run_stats == {"snapshot_exact": True}


def test_inexact_ref_snapshot_does_not_declare_sealed_binding(
    repo,
    monkeypatch,
):
    from okg.substrate.library.sources import _git_graph_history_emit

    def _list_refs_failed(self):
        # Both in-process and git enumeration failed: no pinned tips.
        self._ref_snapshot_exact = False
        return {}

    monkeypatch.setattr(
        _git_graph_history_emit.GitHistorySource,
        "_list_refs",
        _list_refs_failed,
    )
    source = GitHistorySource(repo="demo", repo_root=repo)

    run = source.run("inexact", mode="scope_complete")
    list(run.facts)

    assert run.applied_raw_token is None
    assert run.applied_token_binding == "unverified"
    assert source.last_run_stats == {"snapshot_exact": False}


def test_symbolic_ref_walk_tip_is_the_recorded_tip(repo, tmp_path, monkeypatch):
    # refs/remotes/origin/HEAD is symbolic. The emitter records the tip that
    # resolve() returned; the tip it walks must come from that same lookup,
    # not from a second live resolution that could see a moved ref. The fake
    # symbolic ref below answers a second resolution (ref.peel) with a
    # different commit, as if another checkout moved the branch in between.
    import pygit2

    from okg.substrate.library.sources import _git_graph_history_emit, _git_helpers

    _add_remote_with_symbolic_head(repo, tmp_path)
    moved_sha = subprocess.check_output(
        ["git", "-C", str(repo), "commit-tree", "HEAD^{tree}", "-m", "moved"],
        text=True,
    ).strip()
    real_repo = pygit2.Repository(str(repo))
    moved_commit = real_repo[moved_sha]
    symbolic_name = "refs/remotes/origin/HEAD"

    class _MovingSymbolicRef:
        def __init__(self, real):
            self._real = real
            self.name = real.name

        def resolve(self):
            return self._real.resolve()

        def peel(self, _kind=None):
            return moved_commit

    class _References:
        def __init__(self, real):
            self._real = real

        def __iter__(self):
            return iter(self._real)

        def __getitem__(self, name):
            ref = self._real[name]
            return _MovingSymbolicRef(ref) if name == symbolic_name else ref

    class _Repo:
        def __init__(self, real):
            self._real = real
            self.references = _References(real.references)

        def __getattr__(self, name):
            return getattr(self._real, name)

    monkeypatch.setattr(_git_helpers, "_r", lambda _root: _Repo(real_repo))
    emitter = _git_graph_history_emit.GitHistorySource(
        repo_root=repo,
        repo="demo",
    )

    refs = emitter._list_refs()

    recorded = emitter._raw_ref_bindings[symbolic_name]
    assert recorded != moved_sha
    assert refs[symbolic_name] == recorded
