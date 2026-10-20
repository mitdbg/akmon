from __future__ import annotations

import os
import subprocess

import pygit2
import pytest


def _git_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Issue 979",
        "GIT_AUTHOR_EMAIL": "issue-979@example.test",
        "GIT_COMMITTER_NAME": "Issue 979",
        "GIT_COMMITTER_EMAIL": "issue-979@example.test",
    }
    subprocess.run(
        ["git", "init", "-b", "main"],
        cwd=repo,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    (repo / "owned.txt").write_text("expected\n", encoding="utf-8")
    subprocess.run(
        ["git", "add", "owned.txt"],
        cwd=repo,
        env=env,
        check=True,
    )
    subprocess.run(
        ["git", "commit", "-m", "expected commit"],
        cwd=repo,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        env=env,
        text=True,
    ).strip()
    return repo, head


def _partial_record():
    from okg.substrate.library.sources._git_helpers import CommitRecord

    return CommitRecord(
        sha="f" * 40,
        parents=[],
        author_email="partial@example.test",
        author_name="Partial",
        author_iso="2026-08-18T12:00:00+00:00",
        committer_iso="2026-08-18T12:00:00+00:00",
        subject="must be discarded",
        name_status=[("A", "partial.txt", None)],
    )


def test_partial_incremental_walk_is_discarded_for_successful_full_walk(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_history_emit as emit

    repo, head = _git_repo(tmp_path)
    original_walk = emit.in_proc_walk_commits
    calls = 0

    def fail_then_full(repo_root, positive, negative=(), *, detect_renames=False):
        nonlocal calls
        calls += 1
        if calls == 1:
            yield _partial_record()
            raise pygit2.GitError("incremental walk failed after one row")
        yield from original_walk(
            repo_root,
            positive,
            negative,
            detect_renames=detect_renames,
        )

    monkeypatch.setattr(emit, "in_proc_walk_commits", fail_then_full)
    source = emit.GitHistorySource(repo_root=repo, repo="issue-979")

    records = list(
        source._collect_commits(
            prior_seen=set(),
            cur_ref_shas=[head],
            prior_refs={"refs/heads/main": "0" * 40},
        )
    )

    assert calls == 2
    assert [record.sha for record in records] == [head]
    assert source._history_read_exact is True


def test_double_walk_failure_refuses_source_run_then_exact_retry_succeeds(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_history_emit as emit
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    repo, head = _git_repo(tmp_path)
    original_walk = emit.in_proc_walk_commits
    calls = 0

    def always_fail_after_partial(
        repo_root,
        positive,
        negative=(),
        *,
        detect_renames=False,
    ):
        del repo_root, positive, negative, detect_renames
        nonlocal calls
        calls += 1
        yield _partial_record()
        raise pygit2.GitError(f"walk {calls} failed after one row")

    monkeypatch.setattr(emit, "in_proc_walk_commits", always_fail_after_partial)
    source = GitHistorySource(repo="issue-979", repo_root=repo)

    with pytest.raises(
        RuntimeError,
        match="incremental and full walks failed",
    ) as exc:
        source.run("failed", mode="scope_complete")

    assert calls == 2
    assert "walk 2 failed" in str(exc.value.__cause__)
    monkeypatch.setattr(emit, "in_proc_walk_commits", original_walk)

    recovered = source.run("recovered", mode="scope_complete")
    recovered_facts = list(recovered.facts)
    commit_ids = [
        fact.node_id
        for fact in recovered_facts
        if isinstance(fact, NodeFact) and fact.subtype == "git_commit"
    ]
    assert recovered.completed_scope is True
    assert recovered.record_set is not None
    assert commit_ids == [f"commit:issue-979:{head}"]
