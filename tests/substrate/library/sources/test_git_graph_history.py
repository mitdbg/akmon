"""Substrate-shape parity tests for `GitHistorySource` (Phase 2.4
of `migrate-okg-self-to-substrate`).

Exercises the substrate adapter on the okg_self_micro fixture
corpus and asserts:

  - SourceAdapter Protocol fields are populated.
  - cursor mode emits NodeFact + EdgeFact for each commit / author /
    branch / tag in the domain emit set.
  - The five fixture-pinned commit SHAs surface as commit nodes.
  - next_cursor carries seen_commit_shas (capped at
    DEFAULT_SEEN_SHAS_CAP).
  - A second cursor-mode run with the same cursor emits zero
    new commit nodes.
"""

from __future__ import annotations

import os
import json
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest


_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def test_adapter_protocol_fields_populated() -> None:
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    src = GitHistorySource(repo="okg", repo_root=Path("/tmp"))
    assert src.name == "git_graph_history"
    assert src.profile == "discovery_crawl"


def test_adapter_emits_commit_nodes(fixture_paths) -> None:
    """Cursor-mode first run yields one commit NodeFact per
    fixture-pinned commit SHA."""
    from okg_self_micro import CRITICAL_NODE_IDS  # type: ignore
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )
    from okg.substrate.library.sources import (
        _git_graph_history_emit,
        git_graph_history,
    )
    from okg.substrate.library.sources.change_probe import emit_version_for
    from okg.substrate.library.sources.emission_identity import (
        emission_identity_targets,
    )

    src = GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root)
    token = src.change_probe.build_token()
    assert token["components"]["emit_version"]["value"] == emit_version_for(
        *emission_identity_targets(
            src.name,
            git_graph_history,
            _git_graph_history_emit,
        )
    )
    run = src.run("run-1", mode="cursor")
    emitted_ids = {f.node_id for f in run.facts if hasattr(f, "node_id")}

    expected_commits = {nid for nid in CRITICAL_NODE_IDS if nid.startswith("commit:")}
    missing = expected_commits - emitted_ids
    assert not missing, (
        f"adapter did not emit critical fixture commit ids: "
        f"{sorted(missing)}; emitted: {sorted(emitted_ids)[:10]}"
    )


def test_adapter_node_set_matches_domain(fixture_paths) -> None:
    """Graph-shape parity: the substrate adapter's emitted node_id
    set equals the domain domain class's emitted node_id set on
    the same fixture (cold-start cursor run)."""
    from okg.substrate.library.sources._git_graph_history_emit import (
        GitHistorySource as DomainGitHistorySource,
    )
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    domain = DomainGitHistorySource(
        repo_root=fixture_paths.repo_root,
        repo="okg",
    )
    domain_node_ids = {n.node_id for n in domain.nodes()}

    src = GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root)
    run = src.run("run-1", mode="cursor")
    substrate_node_ids = {
        f.node_id for f in run.facts if hasattr(f, "node_id") and f.op == "I"
    }
    assert substrate_node_ids == domain_node_ids


def test_adapter_edge_set_matches_domain(fixture_paths) -> None:
    """Same parity check on the edges side."""
    from okg.substrate.library.sources._git_graph_history_emit import (
        GitHistorySource as DomainGitHistorySource,
    )
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    domain = DomainGitHistorySource(
        repo_root=fixture_paths.repo_root,
        repo="okg",
    )
    list(domain.nodes())  # populate
    domain_edge_keys = {(e.src, e.dst, e.edge_type) for e in domain.edges()}

    src = GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root)
    run = src.run("run-1", mode="cursor")
    substrate_edge_keys = {
        (f.src, f.dst, f.edge_type) for f in run.facts if hasattr(f, "src")
    }
    assert substrate_edge_keys == domain_edge_keys


def test_next_cursor_carries_seen_commit_shas(fixture_paths) -> None:
    """The substrate cursor must round-trip the domain state shape
    (seen_commit_shas + ref_bindings)."""
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    src = GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root)
    run = src.run("run-1", mode="cursor")
    list(run.facts)

    cursor = run.next_cursor or {}
    assert "seen_commit_shas" in cursor
    assert isinstance(cursor["seen_commit_shas"], list)
    assert len(cursor["seen_commit_shas"]) >= 1


def test_seen_shas_cap_is_documented_default() -> None:
    """The substrate-side cap on seen_commit_shas (at construction
    time) is documented and configurable per-instance. Default
    is 10000 per the OpenSpec proposal."""
    from okg.substrate.library.sources.git_graph_history import (
        DEFAULT_SEEN_SHAS_CAP,
        GitHistorySource,
    )

    assert DEFAULT_SEEN_SHAS_CAP == 10000
    src = GitHistorySource(repo="okg", repo_root=Path("/tmp"))
    assert src.seen_shas_cap == DEFAULT_SEEN_SHAS_CAP


def test_second_cursor_run_emits_no_new_commits(fixture_paths) -> None:
    """Round-trip the cursor: a second cursor-mode run with the
    seen_commit_shas from the first should emit zero new commit
    NodeFacts."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    src = GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root)
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    src2 = GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root)
    second = src2.run("run-2", mode="cursor", cursor=first.next_cursor)
    new_commits = [
        f
        for f in second.facts
        if isinstance(f, NodeFact) and f.subtype == "git_commit"
    ]
    assert new_commits == []


def test_cursor_run_imports_new_commit_after_ref_change(fixture_paths) -> None:
    """A later cursor run imports a commit introduced by a ref change."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    repo_root = fixture_paths.repo_root
    src = GitHistorySource(repo="okg", repo_root=repo_root)
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    probe = repo_root / "src" / "okg_micro" / "sync_probe.py"
    probe.write_text("SYNC_PROBE = True\n")
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "OKG Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
    }
    subprocess.run(["git", "add", str(probe)], cwd=repo_root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-m", "sync: add probe"], cwd=repo_root, env=env, check=True
    )
    new_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        text=True,
    ).strip()

    second = GitHistorySource(repo="okg", repo_root=repo_root).run(
        "run-2",
        mode="cursor",
        cursor=first.next_cursor,
    )
    new_commits = [
        f
        for f in second.facts
        if isinstance(f, NodeFact) and f.node_id == f"commit:okg:{new_sha}"
    ]
    assert len(new_commits) == 1


def test_history_emits_source_file_node_for_deleted_path(tmp_path) -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    subprocess.run(
        ["git", "init"], cwd=repo_root, check=True, stdout=subprocess.DEVNULL
    )
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "OKG Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
    }
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.test"],
        cwd=repo_root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "OKG Fixture"], cwd=repo_root, check=True
    )

    deleted = repo_root / "deleted.txt"
    deleted.write_text("old\n")
    subprocess.run(["git", "add", "deleted.txt"], cwd=repo_root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-m", "add deleted file"],
        cwd=repo_root,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    deleted.unlink()
    subprocess.run(["git", "add", "-A"], cwd=repo_root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-m", "delete file"],
        cwd=repo_root,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )

    run = GitHistorySource(repo="demo", repo_root=repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    facts = list(run.facts)
    file_id = "file:demo:deleted.txt"

    file_fact = next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.node_id == file_id
    )
    assert file_fact.subtype == "source_file"
    assert file_fact.ownership_mode == "substitute"
    assert run.record_set is not None
    record_node = run.record_set.records[file_id].primary
    assert record_node is not None
    assert record_node.ownership_mode == "substitute"
    assert any(
        isinstance(fact, EdgeFact)
        and fact.edge_type == "affects"
        and fact.dst == file_id
        for fact in facts
    )


def test_history_walks_commits_reachable_only_from_remote_refs(tmp_path) -> None:
    """Ref enumeration is the revwalk's positive set, so it covers remotes.

    `external_repos` manages CLONES, where every branch but the checked-out one
    exists only as `refs/remotes/origin/*`. Gating enumeration on
    `include_remotes` (default False) would drop those commits from emission,
    and the next completed-scope run would retract every one of them.
    `include_remotes` gates BRANCH-NODE emission only.
    """
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "OKG Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
    }

    def git(cwd, *args):
        subprocess.run(
            ["git", *args], cwd=cwd, env=env, check=True, stdout=subprocess.DEVNULL
        )

    origin = tmp_path / "origin"
    origin.mkdir()
    git(origin, "init", "-b", "main")
    git(origin, "config", "user.email", "fixture@example.test")
    git(origin, "config", "user.name", "OKG Fixture")
    (origin / "kept.py").write_text("x = 1\n")
    git(origin, "add", "-A")
    git(origin, "commit", "-m", "main commit")
    git(origin, "checkout", "-b", "feature")
    (origin / "feat.py").write_text("y = 2\n")
    git(origin, "add", "-A")
    git(origin, "commit", "-m", "feature-only commit")
    feature_sha = subprocess.check_output(
        ["git", "-C", str(origin), "rev-parse", "HEAD"], text=True
    ).strip()
    git(origin, "checkout", "main")

    clone = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", str(origin), str(clone)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    facts = list(
        GitHistorySource(repo="demo", repo_root=clone)
        .run("run-1", mode="scope_complete")
        .facts
    )
    commit_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.node_id.startswith("commit:")
    }
    assert f"commit:demo:{feature_sha}" in commit_ids, (
        "a commit reachable only from refs/remotes must still be emitted; "
        "dropping it makes the next completed-scope run retract it"
    )

    # Same contract on the subprocess fallback. `_list_refs_via_git` is reached
    # whenever libgit2 cannot enumerate (see
    # `test_history_ref_list_falls_back_when_libgit2_chokes`), and it had its own
    # remote filter -- so the in-proc path alone does not pin this.
    from okg.substrate.library.sources._git_graph_history_emit import (
        GitHistorySource as DomainGitHistorySource,
    )

    domain = DomainGitHistorySource(repo_root=clone, repo="demo", state={})
    fallback_refs = domain._list_refs_via_git(reason="test")
    assert any(
        name.startswith("refs/remotes/") for name in fallback_refs
    ), (
        "the git for-each-ref fallback must enumerate remote-tracking refs too; "
        f"got {sorted(fallback_refs)}"
    )


def test_source_file_exclude_globs_shape_nodes_edges_and_record_set(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    baseline = GitHistorySource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
    ).run("baseline", mode="scope_complete")
    baseline_facts = list(baseline.facts)
    excluded = next(
        fact
        for fact in baseline_facts
        if isinstance(fact, NodeFact) and fact.subtype == "source_file"
    )
    excluded_path = str(excluded.attrs["path"])

    filtered = GitHistorySource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_file_exclude_globs=[excluded_path],
    ).run("filtered", mode="scope_complete")
    filtered_facts = list(filtered.facts)

    assert not any(
        isinstance(fact, NodeFact) and fact.node_id == excluded.node_id
        for fact in filtered_facts
    )
    assert not any(
        isinstance(fact, EdgeFact)
        and fact.edge_type == "affects"
        and fact.dst == excluded.node_id
        for fact in filtered_facts
    )
    assert filtered.record_set is not None
    assert excluded.node_id not in filtered.record_set.records


def test_fixed_refs_are_stable_across_worktree_and_run_ids(fixture_paths) -> None:
    """A completed history walk is a function of committed refs, not checkout
    dirt or execution identity. Every affects target is represented by a
    deterministic substitute candidate so a narrower file source cannot leave a
    publish-time dangling edge."""
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    repo = fixture_paths.repo_root
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.invalid"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "fixture"],
        cwd=repo,
        check=True,
    )

    def snapshot(
        run_id: str,
        *,
        branch_filter: list[str] | None = None,
    ) -> tuple[str, str, set[str], set[str], dict[tuple[str, ...], dict]]:
        run = GitHistorySource(
            repo="okg",
            repo_root=repo,
            branch_filter=branch_filter,
        ).run(
            run_id,
            mode="scope_complete",
        )
        facts = list(run.facts)
        normalized = json.dumps(
            [asdict(fact) for fact in facts],
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        assert run.record_set is not None
        record_set = json.dumps(
            run.record_set.to_cursor(),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        file_nodes = {
            fact.node_id
            for fact in facts
            if isinstance(fact, NodeFact) and fact.subtype == "source_file"
        }
        affects_targets = {
            fact.dst
            for fact in facts
            if isinstance(fact, EdgeFact) and fact.edge_type == "affects"
        }
        revisions = {
            (
                ("node", fact.node_id)
                if isinstance(fact, NodeFact)
                else ("edge", fact.src, fact.edge_type, fact.dst)
            ): fact.source_revision
            for fact in facts
            if isinstance(fact, (NodeFact, EdgeFact))
        }
        return normalized, record_set, file_nodes, affects_targets, revisions

    clean = snapshot("clean-run")
    tracked = subprocess.check_output(
        ["git", "ls-files"], cwd=repo, text=True
    ).splitlines()[0]
    tracked_path = repo / tracked
    tracked_path.write_text("dirty but not committed\n", encoding="utf-8")
    dirty = snapshot("dirty-run")
    subprocess.run(["git", "add", "--", tracked], cwd=repo, check=True)
    staged = snapshot("staged-run")
    tracked_path.unlink()

    dirty_missing = snapshot("dirty-missing-run")
    subprocess.run(
        ["git", "restore", "--staged", "--worktree", "--", tracked],
        cwd=repo,
        check=True,
    )
    restored = snapshot("restored-run")

    assert dirty[:2] == staged[:2] == dirty_missing[:2] == clean[:2] == restored[:2]
    assert clean[3]
    assert clean[3] <= clean[2]
    assert clean[2] == dirty_missing[2] == restored[2]

    # Checkout state is not part of the semantic input: with the full ref-tip
    # set held fixed, checking out another already-enumerated branch is stable.
    original_branch = subprocess.check_output(
        ["git", "branch", "--show-current"], cwd=repo, text=True
    ).strip()
    subprocess.run(["git", "branch", "older", "HEAD~1"], cwd=repo, check=True)
    before_checkout = snapshot("before-checkout")
    subprocess.run(["git", "checkout", "older"], cwd=repo, check=True)
    after_checkout = snapshot("after-checkout")
    assert after_checkout[:2] == before_checkout[:2]
    subprocess.run(["git", "checkout", original_branch], cwd=repo, check=True)

    filtered = snapshot("filtered", branch_filter=[original_branch])
    assert filtered[0] != before_checkout[0]

    before_commit = snapshot("before-commit")
    added = repo / "committed-change.txt"
    added.write_text("committed\n", encoding="utf-8")
    subprocess.run(["git", "add", added.name], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "committed change"], cwd=repo, check=True)
    new_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True
    ).strip()
    after_commit = snapshot("after-commit")

    assert after_commit[0] != before_commit[0]
    assert ("node", f"commit:okg:{new_sha}") in after_commit[4]
    branch_key = ("node", f"branch:okg:{original_branch}")
    assert after_commit[4][branch_key] != before_commit[4][branch_key]
    old_commit_key = next(
        key
        for key in before_commit[4]
        if key[0] == "node" and key[1].startswith("commit:okg:")
    )
    assert after_commit[4][old_commit_key] == before_commit[4][old_commit_key]


def test_edge_revision_preserves_provenance_type() -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource
    from okg.substrate.library.sources.source_specs import EdgeSpec

    source = GitHistorySource(repo="okg", repo_root=Path("/tmp"))
    absent = EdgeSpec("a", "b", "references", provenance=None)  # type: ignore[arg-type]
    literal = EdgeSpec("a", "b", "references", provenance="None")

    assert source._edge_revision(absent, confidence=1.0) != source._edge_revision(
        literal,
        confidence=1.0,
    )


def test_fixed_refs_are_stable_across_python_hash_seeds(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-b", "main"],
        cwd=repo,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    subprocess.run(
        ["git", "config", "user.email", "root@example.test"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Root Author"],
        cwd=repo,
        check=True,
    )
    (repo / "base.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", "base"],
        cwd=repo,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    base = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
    ).strip()

    fixed_date = "2001-01-01T00:00:00+00:00"
    for branch, author in (("alice", "Alice"), ("carol", "Carol")):
        subprocess.run(
            ["git", "checkout", "-B", branch, base],
            cwd=repo,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        (repo / f"{branch}.txt").write_text(f"{branch}\n", encoding="utf-8")
        subprocess.run(["git", "add", f"{branch}.txt"], cwd=repo, check=True)
        env = {
            **os.environ,
            "GIT_AUTHOR_NAME": author,
            "GIT_AUTHOR_EMAIL": "shared@example.test",
            "GIT_COMMITTER_NAME": author,
            "GIT_COMMITTER_EMAIL": "shared@example.test",
            "GIT_AUTHOR_DATE": fixed_date,
            "GIT_COMMITTER_DATE": fixed_date,
        }
        subprocess.run(
            ["git", "commit", "-m", branch],
            cwd=repo,
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )

    script = """
import json
import sys
from dataclasses import asdict
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.git_graph_history import GitHistorySource

run = GitHistorySource(repo="demo", repo_root=sys.argv[1]).run(
    "fixed", mode="scope_complete"
)
facts = list(run.facts)
author = next(
    fact for fact in facts
    if isinstance(fact, NodeFact) and fact.node_id == "author:shared@example.test"
)
assert run.record_set is not None
print(json.dumps(
    {
        "author": asdict(author),
        "facts": [asdict(fact) for fact in facts],
        "record_set": run.record_set.to_cursor(),
    },
    sort_keys=True,
    separators=(",", ":"),
    default=str,
))
"""
    outputs = {
        subprocess.check_output(
            [sys.executable, "-c", script, str(repo)],
            env={**os.environ, "PYTHONHASHSEED": str(seed)},
            text=True,
        ).strip()
        for seed in (1, 2, 3, 7, 10, 12)
    }

    assert len(outputs) == 1
    output = outputs.pop()
    assert json.loads(output)["author"]["attrs"]["display_name"] == "Alice"


def test_history_ignores_non_commit_auxiliary_refs(fixture_paths) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    repo_root = fixture_paths.repo_root
    tree_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD^{tree}"],
        cwd=repo_root,
        text=True,
    ).strip()
    subprocess.run(
        [
            "git",
            "update-ref",
            "refs/codex/turn-diffs/captures/test/base",
            tree_sha,
        ],
        cwd=repo_root,
        check=True,
    )

    run = GitHistorySource(repo="okg", repo_root=repo_root).run(
        "run-1",
        mode="reconcile",
    )
    facts = list(run.facts)

    assert any(
        isinstance(fact, NodeFact) and fact.subtype == "git_commit"
        for fact in facts
    )
    cursor = run.next_cursor or {}
    assert cursor.get("ref_bindings")
    assert not any(
        str(ref).startswith("refs/codex/")
        for ref in cursor["ref_bindings"]
    )
    assert run.record_set is not None
    assert run.record_set.records


def test_history_ref_list_falls_back_when_libgit2_chokes(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import _git_helpers
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    class _BrokenReferences:
        def __iter__(self):
            raise RuntimeError("unpeelable auxiliary ref")

    class _BrokenRepo:
        references = _BrokenReferences()

    original_repo = _git_helpers._r
    calls = {"n": 0}

    def _repo_with_broken_first_ref_list(repo_root):
        calls["n"] += 1
        if calls["n"] == 1:
            return _BrokenRepo()
        return original_repo(repo_root)

    monkeypatch.setattr(_git_helpers, "_r", _repo_with_broken_first_ref_list)

    run = GitHistorySource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
    ).run("run-1", mode="reconcile")
    facts = list(run.facts)

    assert any(
        isinstance(fact, NodeFact) and fact.subtype == "git_commit"
        for fact in facts
    )
    assert run.record_set is not None
    assert run.record_set.records


def test_seen_shas_capped_to_instance_setting(fixture_paths) -> None:
    """Setting a tiny cap on a fresh adapter instance trims the
    persisted seen_commit_shas list — keeping the most recent N."""
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    src = GitHistorySource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        seen_shas_cap=2,
    )
    run = src.run("run-1", mode="cursor")
    list(run.facts)
    cursor = run.next_cursor or {}
    assert len(cursor["seen_commit_shas"]) <= 2


def test_commit_subject_redacts_absolute_user_paths(tmp_path: Path) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    subprocess.run(
        ["git", "init"], cwd=repo_root, check=True, stdout=subprocess.DEVNULL
    )
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "OKG Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
    }
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.test"],
        cwd=repo_root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "OKG Fixture"],
        cwd=repo_root,
        check=True,
    )

    (repo_root / "demo.txt").write_text("demo\n", encoding="utf-8")
    subprocess.run(["git", "add", "demo.txt"], cwd=repo_root, env=env, check=True)
    subprocess.run(
        [
            "git",
            "commit",
            "-m",
            "Remove hardcoded /Users/alice/.codex/worktrees/okg path",
        ],
        cwd=repo_root,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )

    facts = list(GitHistorySource(repo="demo", repo_root=repo_root).run("run-1").facts)
    commit = next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "git_commit"
    )

    assert "/Users/alice" not in commit.attrs["message_subject"]
    assert "/Users/alice" not in commit.attrs["label"]
    assert "<path:redacted>" in commit.attrs["message_subject"]


def test_completed_scope_record_set_tracks_current_branch_reference(tmp_path) -> None:
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    subprocess.run(
        ["git", "init"],
        cwd=repo_root,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "OKG Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
    }
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.test"],
        cwd=repo_root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "OKG Fixture"],
        cwd=repo_root,
        check=True,
    )

    path = repo_root / "README.md"
    path.write_text("one\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo_root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=repo_root,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    first_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        text=True,
    ).strip()

    path.write_text("two\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo_root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-m", "update"],
        cwd=repo_root,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    second_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        text=True,
    ).strip()
    branch = subprocess.check_output(
        ["git", "branch", "--show-current"],
        cwd=repo_root,
        text=True,
    ).strip()

    run = GitHistorySource(repo="demo", repo_root=repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(run.facts)

    assert run.record_set is not None
    branch_record = run.record_set.records[f"branch:demo:{branch}"]
    branch_refs = [
        edge.dst
        for edge in branch_record.edges
        if edge.edge_type == "references"
    ]
    assert branch_refs == [f"commit:demo:{second_sha}"]
    assert f"commit:demo:{first_sha}" not in branch_refs


def test_author_node_is_shared_across_repos_not_exclusively_owned(
    tmp_path,
) -> None:
    """One person committing to two repos is ONE author node, owned by neither.

    `author:<email>` deliberately omits the repo, so every GitHistorySource in
    a multi-repo deployment declares the same author identities. Declaring
    them `authoritative` claims exclusive ownership only the last concurrent
    writer can keep; each sibling's coverage proof then reads its own author
    nodes as missing and refuses to advance its watermark. `substitute` is the
    honest declaration, and it is what the historical file endpoints already
    use.
    """
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )
    from tests.substrate._git_helpers import commit_repo

    email = "shared.author@example.test"
    author_id = f"author:{email}"

    node_ids_per_repo = []
    for repo in ("alpha", "beta"):
        root = tmp_path / repo
        commit_repo(
            root,
            email=email,
            name="Shared Author",
            filename=f"{repo}.txt",
        )
        run = GitHistorySource(repo=repo, repo_root=root).run(
            f"run-{repo}",
            mode="scope_complete",
        )
        facts = list(run.facts)

        author_fact = next(
            fact
            for fact in facts
            if isinstance(fact, NodeFact) and fact.node_id == author_id
        )
        assert author_fact.subtype == "git_author"
        assert author_fact.ownership_mode == "substitute"

        assert run.record_set is not None
        record_node = run.record_set.records[author_id].primary
        assert record_node is not None
        assert record_node.ownership_mode == "substitute"

        node_ids_per_repo.append(
            {fact.node_id for fact in facts if isinstance(fact, NodeFact)}
        )

    # The premise: the two repos really do collide on this identity, and only
    # on this one -- commits/branches/files stay repo-scoped.
    assert node_ids_per_repo[0] & node_ids_per_repo[1] == {author_id}


# --- Full commit messages (#2671 follow-up A) -------------------------------

_BODY_ENV = {
    "GIT_AUTHOR_NAME": "OKG Fixture",
    "GIT_AUTHOR_EMAIL": "fixture@example.test",
    "GIT_COMMITTER_NAME": "OKG Fixture",
    "GIT_COMMITTER_EMAIL": "fixture@example.test",
}


def _body_repo(root: Path) -> Path:
    root.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
    return root


def _commit_message(root: Path, message: str, filename: str) -> str:
    """Commit one new file with ``message`` verbatim; return the new SHA."""
    env = {**os.environ, **_BODY_ENV}
    (root / filename).write_text(f"{filename}\n", encoding="utf-8")
    subprocess.run(["git", "add", filename], cwd=root, env=env, check=True)
    msg_file = root.parent / f"{root.name}-{filename}.msg"
    msg_file.write_text(message, encoding="utf-8")
    subprocess.run(
        ["git", "commit", "-q", "--cleanup=verbatim", "-F", str(msg_file)],
        cwd=root,
        env=env,
        check=True,
    )
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def _commit_attrs(run) -> dict[str, dict]:
    from okg.substrate.library.sources.base import NodeFact

    return {
        f.node_id: f.attrs
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "git_commit"
    }


def _without_version(cursor: dict) -> dict:
    """The cursor as the subject-only code wrote it (no version key)."""
    return {k: v for k, v in cursor.items() if k != "message_schema_version"}


def test_commit_node_carries_full_message_body(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    root = _body_repo(tmp_path / "repo")
    body_sha = _commit_message(
        root,
        "Raise retries to 5\n\n"
        "The upstream mirror drops one request in forty.\n"
        "COMMIT deadbeef\n"
        "ENDMETA\n\n"
        "Measured over two weeks of logs.\n\n\n",
        "a.txt",
    )
    bare_sha = _commit_message(root, "Add entry point\n", "b.txt")
    # Several blank lines between subject and body are not part of the body.
    gap_sha = _commit_message(root, "Tidy\n\n\n\nOnly a body line.  \n", "c.txt")
    # Whitespace-only lines before the body are blank lines too; the first
    # kept line keeps its indentation.
    ws_sha = _commit_message(root, "Indent\n   \n\t\n    code sample\n", "d.txt")

    attrs = _commit_attrs(GitHistorySource(repo="r", repo_root=root).run("run-1"))

    body = attrs[f"commit:r:{body_sha}"]
    assert body["message_subject"] == "Raise retries to 5"
    assert body["message_body"] == (
        "The upstream mirror drops one request in forty.\n"
        "COMMIT deadbeef\n"
        "ENDMETA\n\n"
        "Measured over two weeks of logs."
    )
    bare = attrs[f"commit:r:{bare_sha}"]
    assert bare["message_subject"] == "Add entry point"
    assert "message_body" not in bare
    assert attrs[f"commit:r:{gap_sha}"]["message_body"] == "Only a body line."
    assert attrs[f"commit:r:{ws_sha}"]["message_body"] == "    code sample"


def test_commit_message_body_is_redacted(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    root = _body_repo(tmp_path / "repo")
    token = "ghp_" + "A1b2C3d4" * 3
    sha = _commit_message(
        root,
        "Rotate CI credential\n\n"
        f"The old value was {token} and lived in /Users/alice/.codex/x.\n",
        "a.txt",
    )
    body = _commit_attrs(GitHistorySource(repo="r", repo_root=root).run("run-1"))[
        f"commit:r:{sha}"
    ]["message_body"]
    assert token not in body and "ghp_" not in body
    assert "/Users/alice" not in body
    assert "<secret:redacted>" in body


def test_commit_message_body_cap_marks_the_cut(tmp_path: Path) -> None:
    from okg.substrate.library.sources._git_graph_history_emit import (
        MESSAGE_BODY_MAX_BYTES,
        _cap_message_body,
    )
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    # Multi-byte text is cut on a character boundary, the marker names the
    # kept and total byte counts, and the result stays within the cap.
    wide = "é" * 10_000  # 20,000 UTF-8 bytes
    capped = _cap_message_body(wide)
    kept, marker = capped.rsplit("\n", 1)
    assert set(kept) == {"é"}
    assert marker == (
        f"[message body truncated: kept {len(kept.encode())} of 20000 bytes]"
    )
    assert len(capped.encode("utf-8")) <= MESSAGE_BODY_MAX_BYTES
    assert _cap_message_body("short") == "short"
    exact = "x" * MESSAGE_BODY_MAX_BYTES
    assert _cap_message_body(exact) == exact

    # Redaction runs before the cap: a token straddling the cut point would
    # survive in part (too short for its pattern) if the cap ran first.
    token = "ghp_" + "Z9y8X7w6" * 4
    total_guess = MESSAGE_BODY_MAX_BYTES + 4_000
    marker_len = len(
        f"\n[message body truncated: kept {total_guess} of {total_guess} bytes]"
    )
    cut = MESSAGE_BODY_MAX_BYTES - marker_len
    raw_body = "x" * (cut - 6) + " " + token + " " + "y" * 4_000
    root = _body_repo(tmp_path / "repo")
    sha = _commit_message(root, f"Big body\n\n{raw_body}\n", "a.txt")
    body = _commit_attrs(GitHistorySource(repo="r", repo_root=root).run("run-1"))[
        f"commit:r:{sha}"
    ]["message_body"]
    assert "ghp_" not in body and "Z9y8" not in body
    assert len(body.encode("utf-8")) <= MESSAGE_BODY_MAX_BYTES
    assert body.rsplit("\n", 1)[1].startswith("[message body truncated: kept ")


def test_pre_body_cursor_rewalks_every_commit_once(tmp_path: Path) -> None:
    """A cursor written before `message_body` existed (no version key) makes
    the next cursor run re-emit every commit with its body, even though the
    refs did not move; the next unchanged runs emit nothing, and a new commit
    is emitted alone."""
    from okg.substrate.library.sources._git_graph_history_emit import (
        MESSAGE_SCHEMA_VERSION,
    )
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    root = _body_repo(tmp_path / "repo")
    first_sha = _commit_message(root, "First\n\nwhy the first change\n", "a.txt")
    second_sha = _commit_message(root, "Second\n", "b.txt")
    both = {f"commit:r:{first_sha}", f"commit:r:{second_sha}"}

    seeded = GitHistorySource(repo="r", repo_root=root).run("run-0", mode="cursor")
    assert set(_commit_attrs(seeded)) == both
    assert seeded.next_cursor["message_schema_version"] == MESSAGE_SCHEMA_VERSION

    rewalk = GitHistorySource(repo="r", repo_root=root).run(
        "run-1", mode="cursor", cursor=_without_version(seeded.next_cursor)
    )
    attrs = _commit_attrs(rewalk)
    assert set(attrs) == both
    assert attrs[f"commit:r:{first_sha}"]["message_body"] == "why the first change"
    assert rewalk.next_cursor["message_schema_version"] == MESSAGE_SCHEMA_VERSION
    assert sorted(rewalk.next_cursor["seen_commit_shas"]) == sorted(
        [first_sha, second_sha]
    )

    # Every exit path writes the version: two unchanged runs in a row
    # (the short-circuit) emit nothing, so the re-walk does not repeat.
    cursor = rewalk.next_cursor
    for run_id in ("run-2", "run-3"):
        unchanged = GitHistorySource(repo="r", repo_root=root).run(
            run_id, mode="cursor", cursor=cursor
        )
        assert list(unchanged.facts) == []
        assert (
            unchanged.next_cursor["message_schema_version"]
            == MESSAGE_SCHEMA_VERSION
        )
        cursor = unchanged.next_cursor

    third_sha = _commit_message(root, "Third\n\nwhy the third\n", "c.txt")
    incremental = GitHistorySource(repo="r", repo_root=root).run(
        "run-4", mode="cursor", cursor=cursor
    )
    attrs = _commit_attrs(incremental)
    assert set(attrs) == {f"commit:r:{third_sha}"}
    assert attrs[f"commit:r:{third_sha}"]["message_body"] == "why the third"
    assert (
        incremental.next_cursor["message_schema_version"] == MESSAGE_SCHEMA_VERSION
    )


def test_pre_body_cursor_rewalk_still_lists_deleted_branch_as_stale(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._git_graph_history_emit import (
        GitHistorySource as DomainGitHistorySource,
    )

    root = _body_repo(tmp_path / "repo")
    _commit_message(root, "First\n\nbody\n", "a.txt")
    subprocess.run(["git", "branch", "topic"], cwd=root, check=True)
    seeded = DomainGitHistorySource(repo_root=root, repo="r")
    list(seeded.nodes())
    legacy_state = _without_version(seeded.get_run_state())
    subprocess.run(["git", "branch", "-D", "topic"], cwd=root, check=True)

    rewalk = DomainGitHistorySource(repo_root=root, repo="r", state=legacy_state)
    commits = [n for n in rewalk.nodes() if n.subtype == "git_commit"]
    assert len(commits) == 1
    assert rewalk.stale_node_ids == ["branch:r:topic"]


def test_rewalk_applies_history_since_like_a_cold_start(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    root = _body_repo(tmp_path / "repo")
    old_env = {
        **os.environ,
        **_BODY_ENV,
        "GIT_AUTHOR_DATE": "2020-01-01T00:00:00+00:00",
        "GIT_COMMITTER_DATE": "2020-01-01T00:00:00+00:00",
    }
    (root / "old.txt").write_text("old\n", encoding="utf-8")
    subprocess.run(["git", "add", "old.txt"], cwd=root, env=old_env, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "Old", "-m", "old body"],
        cwd=root,
        env=old_env,
        check=True,
    )
    new_sha = _commit_message(root, "New\n\nnew body\n", "new.txt")

    def source() -> object:
        return GitHistorySource(repo="r", repo_root=root, history_since="2024-01-01")

    seeded = source().run("run-0", mode="cursor")
    assert set(_commit_attrs(seeded)) == {f"commit:r:{new_sha}"}
    rewalk = source().run(
        "run-1", mode="cursor", cursor=_without_version(seeded.next_cursor)
    )
    assert set(_commit_attrs(rewalk)) == {f"commit:r:{new_sha}"}


# --- Opt-in commit diffs (#2671 follow-up B) --------------------------------


def _git(root: Path, *args: str, env: dict | None = None) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        env={**os.environ, **_BODY_ENV, **(env or {})},
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write_commit(
    root: Path, files: dict[str, bytes | str | None], message: str
) -> str:
    """Write (or with None delete) files, commit them, return the SHA."""
    for rel, content in files.items():
        path = root / rel
        if content is None:
            _git(root, "rm", "-q", rel)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
        _git(root, "add", rel)
    _git(root, "commit", "-q", "-m", message)
    return _git(root, "rev-parse", "HEAD")


def _diff_attrs(run) -> dict[str, dict]:
    from okg.substrate.library.sources.base import NodeFact

    return {
        f.node_id: f.attrs
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "git_file_diff"
    }


def _run_facts(src, run_id: str = "run-1", **kwargs):
    run = src.run(run_id, **kwargs)
    return run, list(run.facts)


def _config_repo(root: Path) -> tuple[Path, str, str]:
    root = _body_repo(root)
    first = _write_commit(
        root, {"config.yaml": "pool_name: bluefalcon\ntimeout: 30\n"}, "Add config"
    )
    second = _write_commit(
        root,
        {"config.yaml": "pool_name: greenheron\ntimeout: 30\n"},
        "Move to the new pool",
    )
    return root, first, second


def test_diffs_are_off_by_default(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, _first, _second = _config_repo(tmp_path / "repo")
    default_run, default_facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root), mode="scope_complete"
    )
    off_run, off_facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root, store_diffs=False),
        mode="scope_complete",
    )
    assert [repr(f) for f in default_facts] == [repr(f) for f in off_facts]
    assert not any(getattr(f, "subtype", None) == "git_file_diff" for f in default_facts)
    assert not any(
        "diff_omitted_file_count" in getattr(f, "attrs", {}) for f in default_facts
    )
    assert not any(str(getattr(f, "dst", "")).startswith("diff:") for f in default_facts)
    assert "diff_settings" not in default_run.next_cursor
    assert default_run.record_set is not None
    assert not any(key.startswith("diff:") for key in default_run.record_set.records)


def test_every_diff_setting_changes_the_probe_token(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, _first, _second = _config_repo(tmp_path / "repo")
    variants = [
        {},
        {"store_diffs": True},
        {"store_diffs": True, "diff_max_file_bytes": 4096},
        {"store_diffs": True, "diff_max_commit_bytes": 65536},
        {"store_diffs": True, "diff_exclude_globs": ["*.lock"]},
        {"store_diffs": True, "diffs_since": "2024-01-01"},
    ]
    tokens = {
        GitHistorySource(repo="r", repo_root=root, **kw).change_probe.build_token()[
            "components"
        ]["config"]["value"]
        for kw in variants
    }
    assert len(tokens) == len(variants)


def test_diff_node_holds_the_old_and_new_value(tmp_path: Path) -> None:
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, first, second = _config_repo(tmp_path / "repo")
    run, facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True), mode="cursor"
    )
    diffs = {
        f.node_id: f.attrs
        for f in facts
        if getattr(f, "subtype", None) == "git_file_diff"
    }
    changed = diffs[f"diff:r:{second}:config.yaml"]
    assert "-pool_name: bluefalcon" in changed["patch"]
    assert "+pool_name: greenheron" in changed["patch"]
    assert changed["commit_sha"] == second
    assert changed["path"] == "config.yaml"
    assert changed["change_type"] == "modified"
    assert changed["message_subject"] == "Move to the new pool"
    assert changed["label"] == f"{second[:7]} config.yaml"
    assert changed["patch_truncated"] is False
    assert changed["patch_bytes"] == len(changed["patch"].encode())
    # The root commit diffs against the empty tree.
    added = diffs[f"diff:r:{first}:config.yaml"]
    assert added["change_type"] == "added"
    assert "+pool_name: bluefalcon" in added["patch"]

    edges = {
        (e.src, e.edge_type, e.dst) for e in facts if isinstance(e, EdgeFact)
    }
    assert (f"commit:r:{second}", "contains", f"diff:r:{second}:config.yaml") in edges
    assert (f"diff:r:{second}:config.yaml", "references", "file:r:config.yaml") in edges
    commits = {
        f.node_id: f.attrs for f in facts if getattr(f, "subtype", None) == "git_commit"
    }
    assert commits[f"commit:r:{second}"]["diff_omitted_file_count"] == 0
    assert run.next_cursor["diff_settings"]["max_file_bytes"] == 32_768


def test_merges_binaries_and_excluded_files_get_no_diff(tmp_path: Path) -> None:
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    _write_commit(root, {"a.txt": "one\n"}, "Base")
    _git(root, "checkout", "-q", "-b", "topic")
    topic = _write_commit(
        root,
        {
            "logo.bin": b"\x00\x01\x02binary\x00" * 10,
            "uv.lock": "lock v1\n",
            ".env": "DB_PASSWORD=hunter2\n",
            "deploy/prod.env": "API=1\n",
            "b.txt": "two\n",
        },
        "Topic work",
    )
    _git(root, "checkout", "-q", "main")
    _write_commit(root, {"c.txt": "three\n"}, "Main work")
    _git(root, "merge", "-q", "--no-ff", "-m", "Merge topic", "topic")
    merge = _git(root, "rev-parse", "HEAD")

    _run, facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True), mode="cursor"
    )
    diff_ids = {
        f.node_id for f in facts if getattr(f, "subtype", None) == "git_file_diff"
    }
    assert f"diff:r:{topic}:b.txt" in diff_ids
    for skipped in ("logo.bin", "uv.lock", ".env", "deploy/prod.env"):
        assert f"diff:r:{topic}:{skipped}" not in diff_ids
    assert not any(d.startswith(f"diff:r:{merge}:") for d in diff_ids)
    affects = {
        (e.src, e.dst) for e in facts if isinstance(e, EdgeFact) and e.edge_type == "affects"
    }
    for kept in ("logo.bin", "uv.lock", ".env", "deploy/prod.env"):
        assert (f"commit:r:{topic}", f"file:r:{kept}") in affects
    commits = {
        f.node_id: f.attrs for f in facts if getattr(f, "subtype", None) == "git_commit"
    }
    assert "diff_omitted_file_count" not in commits[f"commit:r:{merge}"]
    # Excluded and binary files are out of scope, not omitted.
    assert commits[f"commit:r:{topic}"]["diff_omitted_file_count"] == 0


def test_non_utf8_file_is_decoded_with_replacement(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    sha = _write_commit(root, {"legacy.txt": "caf\xe9 au lait\n".encode("latin-1")}, "Add")
    diffs = _diff_attrs(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True).run("run-1")
    )
    patch = diffs[f"diff:r:{sha}:legacy.txt"]["patch"]
    assert "caf� au lait" in patch


def test_file_cap_and_commit_budget_are_visible(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    sha = _write_commit(
        root,
        {
            "big.txt": "".join(f"line {i} of the big file\n" for i in range(400)),
            "small.txt": "small\n",
            "medium.txt": "".join(f"medium {i}\n" for i in range(60)),
        },
        "Three files",
    )
    # With room in the budget every file is kept; the big one is cut with a
    # marker at the per-file cap.
    roomy = GitHistorySource(
        repo="r", repo_root=root, store_diffs=True, diff_max_file_bytes=2048
    )
    all_diffs = _diff_attrs(roomy.run("run-1"))
    big = all_diffs[f"diff:r:{sha}:big.txt"]
    assert big["patch_truncated"] is True
    assert big["patch_bytes"] > 2048
    big_bytes = len(big["patch"].encode())
    assert big_bytes <= 2048
    assert big["patch"].rsplit("\n", 1)[1].startswith("[patch truncated: kept ")
    small_bytes = len(all_diffs[f"diff:r:{sha}:small.txt"]["patch"].encode())
    medium_bytes = len(all_diffs[f"diff:r:{sha}:medium.txt"]["patch"].encode())
    assert small_bytes < medium_bytes < big_bytes

    # A budget that holds all but one byte of the three: smallest first keeps
    # small and medium and omits big (path order would have kept big and
    # medium and omitted small). The omission is counted, never silent.
    src = GitHistorySource(
        repo="r",
        repo_root=root,
        store_diffs=True,
        diff_max_file_bytes=2048,
        diff_max_commit_bytes=small_bytes + medium_bytes + big_bytes - 1,
    )
    _run, facts = _run_facts(src, "run-2", mode="cursor")
    assert set(_diff_attrs_from(facts)) == {
        f"diff:r:{sha}:small.txt",
        f"diff:r:{sha}:medium.txt",
    }
    commit = next(
        f.attrs for f in facts if getattr(f, "subtype", None) == "git_commit"
    )
    assert commit["diff_omitted_file_count"] == 1


def _diff_attrs_from(facts) -> dict[str, dict]:
    return {
        f.node_id: f.attrs
        for f in facts
        if getattr(f, "subtype", None) == "git_file_diff"
    }


def test_oversized_blob_is_counted_not_diffed(tmp_path: Path, monkeypatch) -> None:
    import functools

    from okg.substrate.library.sources import _git_graph_history_emit as emit
    from okg.substrate.library.sources._git_helpers import in_proc_walk_commits
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    sha = _write_commit(
        root, {"huge.txt": "x" * 400 + "\n", "tiny.txt": "t\n"}, "Two files"
    )
    records = list(
        in_proc_walk_commits(root, [sha], with_patches=True, max_patch_blob_bytes=100)
    )
    assert records[0].oversized_patch_paths == ("huge.txt",)
    assert [p.path for p in records[0].patches] == ["tiny.txt"]

    monkeypatch.setattr(
        emit,
        "in_proc_walk_commits",
        functools.partial(in_proc_walk_commits, max_patch_blob_bytes=100),
    )
    _run, facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True), mode="cursor"
    )
    assert set(_diff_attrs_from(facts)) == {f"diff:r:{sha}:tiny.txt"}
    commit = next(
        f.attrs for f in facts if getattr(f, "subtype", None) == "git_commit"
    )
    assert commit["diff_omitted_file_count"] == 1


def test_blob_missing_from_the_object_store_is_counted_not_fatal(
    tmp_path: Path,
) -> None:
    """A partial clone lacks some blobs and libgit2 cannot fetch them; the
    history walk must still finish and count the file as not diffed."""
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    # Not in the root commit: the root commit's file list reads every blob
    # of its tree (existing behavior, diffs or not).
    _write_commit(root, {"base.txt": "base\n"}, "Base")
    _write_commit(root, {"gone.txt": "first version\n", "kept.txt": "a\n"}, "Add")
    sha = _write_commit(
        root, {"gone.txt": "second version\n", "kept.txt": "b\n"}, "Change"
    )
    old_blob = _git(root, "rev-parse", "HEAD~1:gone.txt")
    (root / ".git" / "objects" / old_blob[:2] / old_blob[2:]).unlink()

    run, facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True), mode="cursor"
    )
    diffs = _diff_attrs_from(facts)
    assert f"diff:r:{sha}:kept.txt" in diffs
    assert f"diff:r:{sha}:gone.txt" not in diffs
    commit = next(
        f.attrs
        for f in facts
        if getattr(f, "node_id", "") == f"commit:r:{sha}"
    )
    assert commit["diff_omitted_file_count"] == 1
    assert run.next_cursor["snapshot_exact"] is True


def test_diffs_since_bounds_which_commits_get_diffs(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    old_env = {
        "GIT_AUTHOR_DATE": "2020-01-01T00:00:00+00:00",
        "GIT_COMMITTER_DATE": "2020-01-01T00:00:00+00:00",
    }
    (root / "old.txt").write_text("old\n", encoding="utf-8")
    _git(root, "add", "old.txt")
    _git(root, "commit", "-q", "-m", "Old", env=old_env)
    old = _git(root, "rev-parse", "HEAD")
    new = _write_commit(root, {"new.txt": "new\n"}, "New")

    _run, facts = _run_facts(
        GitHistorySource(
            repo="r", repo_root=root, store_diffs=True, diffs_since="2024-01-01"
        ),
        mode="cursor",
    )
    assert set(_diff_attrs_from(facts)) == {f"diff:r:{new}:new.txt"}
    commits = {
        f.node_id: f.attrs for f in facts if getattr(f, "subtype", None) == "git_commit"
    }
    assert set(commits) == {f"commit:r:{old}", f"commit:r:{new}"}
    assert "diff_omitted_file_count" not in commits[f"commit:r:{old}"]

    with pytest.raises(ValueError, match="diffs_since"):
        GitHistorySource(
            repo="r", repo_root=root, store_diffs=True, diffs_since="last tuesday"
        ).run("run-bad")


def test_detected_rename_carries_the_old_path(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    body = "".join(f"setting_{i}: {i}\n" for i in range(30))
    _write_commit(root, {"old.yaml": body}, "Add")
    _git(root, "mv", "old.yaml", "new.yaml")
    (root / "new.yaml").write_text(body + "extra: 1\n", encoding="utf-8")
    _git(root, "add", "new.yaml")
    _git(root, "commit", "-q", "-m", "Rename")
    sha = _git(root, "rev-parse", "HEAD")

    diffs = _diff_attrs(
        GitHistorySource(
            repo="r", repo_root=root, store_diffs=True, detect_renames=True
        ).run("run-1")
    )
    renamed = diffs[f"diff:r:{sha}:new.yaml"]
    assert renamed["change_type"] == "renamed"
    assert renamed["renamed_from_path"] == "old.yaml"
    assert "+extra: 1" in renamed["patch"]


def _rename_repo(tmp_path: Path) -> tuple[Path, str]:
    """A repository whose second commit is `git mv a.py b/a.py` (R100)."""
    root = _body_repo(tmp_path / "repo")
    body = "".join(f"value_{i} = {i}\n" for i in range(30))
    _write_commit(root, {"a.py": body}, "Add")
    (root / "b").mkdir()
    _git(root, "mv", "a.py", "b/a.py")
    _git(root, "commit", "-q", "-m", "Move")
    return root, _git(root, "rev-parse", "HEAD")


def _history_run(root: Path, run_id: str, *, detect_renames: bool, cursor=None):
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    run = GitHistorySource(
        repo="r", repo_root=root, detect_renames=detect_renames
    ).run(run_id, mode="cursor", cursor=cursor)
    return list(run.facts), run.next_cursor


def _commit_affects(facts, sha: str) -> dict[str, dict]:
    """The commit's `affects` edges by target; refuses a duplicate edge."""
    from okg.substrate.library.sources.base import EdgeFact

    edges = [
        f for f in facts
        if isinstance(f, EdgeFact)
        and f.edge_type == "affects"
        and f.src == f"commit:r:{sha}"
    ]
    by_dst = {str(e.dst): dict(e.attrs) for e in edges}
    assert len(by_dst) == len(edges), "duplicate affects edge"
    return by_dst


def _change(attrs: dict) -> tuple[str, str | None]:
    return attrs["change_type"], attrs.get("renamed_from_path")


def test_turning_rename_detection_on_rewalks_and_rewrites_the_rename(
    tmp_path: Path,
) -> None:
    """History ingested with `detect_renames` off stores a rename as
    added + deleted. Turning the setting on re-walks the unchanged refs once
    and rewrites the same two edges (no third one): the new path becomes
    `renamed` from the old one. Turning it off again rewrites it back."""
    root, sha = _rename_repo(tmp_path)

    off_facts, off_cursor = _history_run(root, "run-0", detect_renames=False)
    assert {
        dst: _change(a) for dst, a in _commit_affects(off_facts, sha).items()
    } == {
        "file:r:b/a.py": ("added", None),
        "file:r:a.py": ("deleted", None),
    }
    assert "detect_renames" not in off_cursor

    on_facts, on_cursor = _history_run(
        root, "run-1", detect_renames=True, cursor=off_cursor
    )
    assert {
        dst: _change(a) for dst, a in _commit_affects(on_facts, sha).items()
    } == {
        "file:r:b/a.py": ("renamed", "a.py"),
        "file:r:a.py": ("deleted", None),
    }
    assert on_cursor["detect_renames"] is True
    assert sorted(on_cursor["seen_commit_shas"]) == sorted(
        off_cursor["seen_commit_shas"]
    )

    # The re-walk happens once: the next run with the same setting emits
    # nothing.
    again, on_cursor = _history_run(
        root, "run-2", detect_renames=True, cursor=on_cursor
    )
    assert again == []

    back_facts, back_cursor = _history_run(
        root, "run-3", detect_renames=False, cursor=on_cursor
    )
    assert {
        dst: _change(a) for dst, a in _commit_affects(back_facts, sha).items()
    } == {
        "file:r:b/a.py": ("added", None),
        "file:r:a.py": ("deleted", None),
    }
    assert "detect_renames" not in back_cursor


@pytest.mark.parametrize("detect_renames", [False, True])
def test_unchanged_rename_setting_does_not_rewalk(
    tmp_path: Path, detect_renames: bool
) -> None:
    """With the setting unchanged (including a cursor written before the
    setting was recorded, which had it off), unchanged refs emit nothing and
    a new commit is emitted alone."""
    root, _sha = _rename_repo(tmp_path)
    _facts, cursor = _history_run(
        root, "run-0", detect_renames=detect_renames
    )
    unchanged, cursor = _history_run(
        root, "run-1", detect_renames=detect_renames, cursor=cursor
    )
    assert unchanged == []

    new_sha = _write_commit(root, {"c.py": "C = 1\n"}, "Add c")
    incremental, _cursor = _history_run(
        root, "run-2", detect_renames=detect_renames, cursor=cursor
    )
    commits = {
        f.node_id for f in incremental
        if getattr(f, "subtype", None) == "git_commit"
    }
    assert commits == {f"commit:r:{new_sha}"}


def test_patch_secrets_are_redacted_on_added_and_removed_lines(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    # Fake credentials are joined at runtime so no complete credential-shaped
    # literal sits in the source (the public-release scan refuses one, fake or
    # not). Each runtime value is identical to the old single literal.
    aws_key = "AKIA" + "ABCDEFGHIJKLMNOP"
    slack_hook = "hooks.slack.com/services/" + "T000/B000/XXXX"
    pem_begin = "-----BEGIN RSA " + "PRIVATE KEY-----"
    secrets = [
        "hunter2old",
        "hunter2new",
        "abc123clientsecret",
        "sk-live-1234567890abcdef",
        aws_key,
        "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",
        "tokval98765",
        "MIIEowIBAAKCAQEAsecretkeybody",
        slack_hook,
    ]
    old = (
        'db:\n  password: "hunter2old"\n'
        "DB_PASSWORD=hunter2old\n"
        "plain: keep-me\n"
    )
    new = (
        'db:\n  password: "hunter2new"\n'
        '{"clientSecret": "abc123clientsecret"}\n'
        "api_key: 'sk-live-1234567890abcdef'\n"
        f"AWS_ACCESS_KEY_ID={aws_key}\n"
        "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY\n"
        "token: tokval98765\n"
        f"webhook: https://{slack_hook}\n"
        f"{pem_begin}\n"
        "MIIEowIBAAKCAQEAsecretkeybody\n"
        "-----END RSA PRIVATE KEY-----\n"
        "plain: keep-me\n"
    )
    root = _body_repo(tmp_path / "repo")
    _write_commit(root, {"settings.yaml": old}, "Add settings")
    sha = _write_commit(root, {"settings.yaml": new}, "Rotate settings")
    patch = _diff_attrs(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True).run("run-1")
    )[f"diff:r:{sha}:settings.yaml"]["patch"]
    for secret in secrets:
        assert secret not in patch, secret
    assert "<secret:redacted>" in patch
    assert "plain: keep-me" in patch  # ordinary values survive
    # Diff structure survives: every line keeps its prefix, the key keeps its
    # quotes, and a marker is never redacted a second time.
    assert '-  password: "<secret:redacted>"' in patch
    assert "-DB_PASSWORD=<secret:redacted>" in patch
    assert '+{"clientSecret": "<secret:redacted>"}' in patch
    assert "<secret:<secret" not in patch
    body_lines = patch.split("@@", 2)[2].split("\n")[1:]
    assert all(line[:1] in ("+", "-", " ", "") for line in body_lines), body_lines


def test_patch_redaction_keeps_line_structure_and_is_linear() -> None:
    import time

    from okg.substrate.library.sources._git_graph_history_emit import _redact_patch

    root = Path("/nonexistent-repo-root")
    cases = {
        "-except TokenError:\n-    raise": "-except TokenError:\n-    raise",
        " credentials:\n-  db_password: hunter2\n return x": (
            " credentials:\n-  db_password: <secret:redacted>\n return x"
        ),
        "+x = 'ghp_" + "A" * 24 + "'": "+x = '<secret:redacted>'",
        "+Authorization: Bearer abcdefghijklmnop": (
            "+Authorization: Bearer <secret:redacted>"
        ),
        "-DB_PASS=hunter2": "-DB_PASS=<secret:redacted>",
        '+password: "correct horse battery staple"': '+password: "<secret:redacted>"',
        "+  if token:": "+  if token:",
        "+compass: north": "+compass: north",
        "+url: https://example.com/a": "+url: https://example.com/a",
    }
    for raw, expected in cases.items():
        assert _redact_patch(raw, repo_root=root) == expected, raw
        # Idempotent: redacting the output again changes nothing.
        assert _redact_patch(expected, repo_root=root) == expected, expected

    # One long line (a hex fixture, a minified blob) must not stall the walk:
    # the old pattern took seconds at 8,000 characters and grew with the
    # square (or cube) of the length.
    for body in ("a" * 200_000, "token" * 40_000, "a=" * 100_000):
        started = time.monotonic()
        _redact_patch("+" + body, repo_root=root)
        assert time.monotonic() - started < 2.0, body[:10]


def test_type_change_yields_one_diff_node(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    _write_commit(root, {"target.txt": "t\n", "f.txt": "plain file\n"}, "Add")
    (root / "f.txt").unlink()
    (root / "f.txt").symlink_to("target.txt")
    _git(root, "add", "f.txt")
    _git(root, "commit", "-q", "-m", "Make f a link")
    sha = _git(root, "rev-parse", "HEAD")

    _run, facts = _run_facts(
        GitHistorySource(repo="r", repo_root=root, store_diffs=True), mode="cursor"
    )
    ids = [
        f.node_id
        for f in facts
        if getattr(f, "subtype", None) == "git_file_diff"
        and f.node_id.startswith(f"diff:r:{sha}:")
    ]
    assert ids == [f"diff:r:{sha}:f.txt"]
    node = _diff_attrs_from(facts)[ids[0]]
    assert "-plain file" in node["patch"] and "+target.txt" in node["patch"]
    assert node["change_type"] == "modified"


# --- Issue #3666: a type change is one modified file -------------------------

_LONG_BODY = "".join(f"line {i} of a file long enough to pair\n" for i in range(40))


def _type_change_repo(root: Path, case: str) -> tuple[Path, str, dict[str, str]]:
    """A repo whose last commit has a type change; returns it, that commit,
    and the change type each changed path must get.

    ``link``: a file becomes a symlink. ``moved``: a file's text moves to a
    new file and the old path becomes a symlink to it (what rename detection
    pairs). ``unlinked``: a symlink becomes a file holding a deleted file's
    text (detection pairs the other half).
    """
    root = _body_repo(root)
    if case == "unlinked":
        _write_commit(root, {"target.txt": "t\n", "gone.txt": _LONG_BODY}, "Add")
        (root / "f.txt").symlink_to("target.txt")
        _git(root, "add", "f.txt")
        _git(root, "commit", "-q", "-m", "Add link")
        _git(root, "rm", "-q", "f.txt")
        _git(root, "mv", "gone.txt", "f.txt")
        _git(root, "commit", "-q", "-m", "Make f a file")
        expected = {"f.txt": "modified", "gone.txt": "deleted"}
    else:
        _write_commit(root, {"target.txt": "t\n", "f.txt": _LONG_BODY}, "Add")
        if case == "moved":
            _git(root, "mv", "f.txt", "g.txt")
            (root / "f.txt").symlink_to("g.txt")
            expected = {"f.txt": "modified", "g.txt": "added"}
        else:
            (root / "f.txt").unlink()
            (root / "f.txt").symlink_to("target.txt")
            expected = {"f.txt": "modified"}
        _git(root, "add", "-A")
        _git(root, "commit", "-q", "-m", "Make f a link")
    return root, _git(root, "rev-parse", "HEAD"), expected


@pytest.mark.parametrize("case", ["link", "moved", "unlinked"])
@pytest.mark.parametrize("detect_renames", [False, True], ids=["exact", "renames"])
@pytest.mark.parametrize(
    "store_diffs", [False, True], ids=["git-log-reader", "libgit2-reader"]
)
def test_type_change_is_one_modified_affects_edge(
    tmp_path: Path, case: str, detect_renames: bool, store_diffs: bool
) -> None:
    """Before #3666 a type change came out as a delete plus an add of one
    path: two `affects` edges, the last one ("added", or "deleted" with
    rename detection) winning. Both readers now keep it one modified file."""
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, sha, expected = _type_change_repo(tmp_path / "repo", case)
    _run, facts = _run_facts(
        GitHistorySource(
            repo="r",
            repo_root=root,
            store_diffs=store_diffs,
            detect_renames=detect_renames,
        ),
        mode="cursor",
    )
    edges = [
        (fact.dst, fact.attrs["change_type"])
        for fact in facts
        if isinstance(fact, EdgeFact)
        and fact.edge_type == "affects"
        and fact.src == f"commit:r:{sha}"
    ]
    assert sorted(edges) == sorted(
        (f"file:r:{path}", change) for path, change in expected.items()
    )
    if store_diffs:
        diffs = {
            attrs["path"]: attrs["change_type"]
            for attrs in _diff_attrs_from(facts).values()
            if attrs["commit_sha"] == sha
        }
        assert diffs["f.txt"] == "modified"


@pytest.mark.parametrize("case", ["link", "moved", "unlinked"])
def test_type_change_never_pairs_in_rename_detection(
    tmp_path: Path, case: str
) -> None:
    """Rename detection keeps a type change one unpaired T row, as git does,
    on the libgit2 diff and on the streamed git log alike."""
    from okg.substrate.library.sources._git_helpers import in_proc_walk_commits

    root, sha, _expected = _type_change_repo(tmp_path / "repo", case)
    git_rows = sorted(
        tuple(line.split("\t"))
        for line in _git(
            root, "log", "-1", "-M", "--name-status", "--format=", sha
        ).splitlines()
    )
    for with_patches in (False, True):
        (rec,) = [
            r
            for r in in_proc_walk_commits(
                root, [sha], detect_renames=True, with_patches=with_patches
            )
            if r.sha == sha
        ]
        rows = sorted(rec.name_status)
        assert [row for row in rows if row[1] == "f.txt"] == [("T", "f.txt", None)]
        assert not any(old == "f.txt" for _s, _p, old in rows)
        assert sorted((s, p) for s, p, _old in rows) == git_rows
        if with_patches:
            assert [(p.status, p.path) for p in rec.patches if p.path == "f.txt"] == [
                ("T", "f.txt")
            ]


@pytest.mark.parametrize(
    "rows",
    [
        [("D", "f.txt", None), ("A", "f.txt", None)],
        [("R", "g.txt", "f.txt"), ("A", "f.txt", None)],
    ],
    ids=["delete-and-add", "rename-source-and-add"],
)
def test_two_change_types_for_one_file_refuse_the_ingest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows: list
) -> None:
    """One commit changes a file one way. Two rows that disagree must stop
    the ingest loudly, never write two edges and let the order decide."""
    from okg.substrate.library.sources import _git_graph_history_emit as emit
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, sha, _expected = _type_change_repo(tmp_path / "repo", "link")
    original_walk = emit.in_proc_walk_commits

    def walk_with_conflict(*args, **kwargs):
        for rec in original_walk(*args, **kwargs):
            yield rec._replace(name_status=rows) if rec.sha == sha else rec

    monkeypatch.setattr(emit, "in_proc_walk_commits", walk_with_conflict)
    with pytest.raises(emit.ConflictingFileChangeError, match=sha):
        _run_facts(GitHistorySource(repo="r", repo_root=root), mode="cursor")


def test_date_typed_diffs_since_does_not_rewalk_every_run(tmp_path: Path) -> None:
    """YAML reads an unquoted `2024-01-01` as a date; the JSON cursor stores
    a string. The setting must compare equal after a round trip."""
    import datetime
    import json

    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, _first, _second = _config_repo(tmp_path / "repo")

    def source() -> GitHistorySource:
        return GitHistorySource(
            repo="r",
            repo_root=root,
            store_diffs=True,
            diffs_since=datetime.date(2024, 1, 1),  # type: ignore[arg-type]
        )

    first = source().run("run-1", mode="cursor")
    list(first.facts)
    cursor = json.loads(json.dumps(first.next_cursor, default=str))
    second = source().run("run-2", mode="cursor", cursor=cursor)
    assert list(second.facts) == []


def test_rename_from_an_excluded_file_is_not_diffed(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root = _body_repo(tmp_path / "repo")
    body = "".join(f"KEY_{i}=value{i}\n" for i in range(30))
    _write_commit(root, {"prod.env": body}, "Add env")
    _git(root, "mv", "prod.env", "settings.txt")
    # Change a line too: a pure rename has no hunk and would be skipped
    # for that reason alone.
    (root / "settings.txt").write_text(body.replace("value0", "rotated0"))
    _git(root, "add", "settings.txt")
    _git(root, "commit", "-q", "-m", "Rename")
    sha = _git(root, "rev-parse", "HEAD")
    diffs = _diff_attrs(
        GitHistorySource(
            repo="r", repo_root=root, store_diffs=True, detect_renames=True
        ).run("run-1")
    )
    assert not any(d.startswith(f"diff:r:{sha}:") for d in diffs)


def test_diff_settings_leave_the_probe_alone_while_diffs_are_off(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, _first, _second = _config_repo(tmp_path / "repo")

    def config_token(**kwargs) -> str:
        return GitHistorySource(
            repo="r", repo_root=root, **kwargs
        ).change_probe.build_token()["components"]["config"]["value"]

    assert config_token() == config_token(diff_max_file_bytes=4096, diffs_since="2024-01-01")


def test_turning_diffs_on_rewalks_once(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, first, second = _config_repo(tmp_path / "repo")
    off = GitHistorySource(repo="r", repo_root=root).run("run-0", mode="cursor")
    list(off.facts)
    assert "diff_settings" not in off.next_cursor

    # Diffs still off, unchanged refs: a cursor without the settings key is
    # "off", so nothing is re-walked (upgrading costs no history walk).
    still_off = GitHistorySource(repo="r", repo_root=root).run(
        "run-1", mode="cursor", cursor=off.next_cursor
    )
    assert list(still_off.facts) == []

    on = GitHistorySource(repo="r", repo_root=root, store_diffs=True).run(
        "run-2", mode="cursor", cursor=still_off.next_cursor
    )
    assert set(_diff_attrs(on)) == {
        f"diff:r:{first}:config.yaml",
        f"diff:r:{second}:config.yaml",
    }
    assert on.next_cursor["diff_settings"]["max_commit_bytes"] == 262_144

    cursor = on.next_cursor
    for run_id in ("run-3", "run-4", "run-5"):
        unchanged = GitHistorySource(repo="r", repo_root=root, store_diffs=True).run(
            run_id, mode="cursor", cursor=cursor
        )
        assert list(unchanged.facts) == []
        cursor = unchanged.next_cursor

    # A cap change re-walks; a cursor run cannot retract, so it re-emits.
    capped = GitHistorySource(
        repo="r", repo_root=root, store_diffs=True, diff_max_file_bytes=4096
    ).run("run-6", mode="cursor", cursor=cursor)
    assert len(_diff_attrs(capped)) == 2
    assert capped.next_cursor["diff_settings"]["max_file_bytes"] == 4096


def test_completed_scope_record_set_follows_the_diff_setting(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    root, _first, second = _config_repo(tmp_path / "repo")
    on = GitHistorySource(repo="r", repo_root=root, store_diffs=True).run(
        "run-1", mode="scope_complete"
    )
    assert on.record_set is not None
    diff_id = f"diff:r:{second}:config.yaml"
    assert diff_id in on.record_set.records
    commit_edges = on.record_set.records[f"commit:r:{second}"].edges
    assert any(e.dst == diff_id and e.edge_type == "contains" for e in commit_edges)

    off = GitHistorySource(repo="r", repo_root=root).run("run-2", mode="scope_complete")
    assert off.record_set is not None
    assert not any(key.startswith("diff:") for key in off.record_set.records)
