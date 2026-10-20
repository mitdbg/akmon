"""Substrate-shape parity tests for `GitFilesSource` (Phase 2.4
of `migrate-okg-self-to-substrate`).

The substrate adapter wraps the domain domain class. These tests
exercise the adapter on the okg_self_micro fixture corpus and
assert the emitted node/edge set matches the domain emitter's
emit set on the same fixture, plus the SourceAdapter Protocol
contract:

  - .name and .profile fields populated.
  - run() returns a SourceRun whose facts iterate.
  - cursor mode emits NodeFact(op='I'); reconcile mode is
    additionally permitted to emit op='R' retractions.
  - next_cursor carries the domain state (tree_sha + dirty_paths).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


# Make the fixture loader importable without dropping a __init__.py
# into tests/substrate/fixtures/.
_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


def _current_branch(repo_root: Path) -> str:
    return (
        subprocess.check_output(
            ["git", "-C", str(repo_root), "branch", "--show-current"],
            text=True,
        ).strip()
        or "HEAD"
    )


def _current_head(repo_root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        text=True,
    ).strip()


def _external_record_set_cursor(
    cursor: dict,
    *,
    record_count: int,
    verified: bool = True,
    source_name: str = "okg-workspace.git-files",
    graph_branch: str = "default",
) -> dict:
    out = {
        **cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {
            "schema": "okg.source_runner.record_set_summary.v1",
            "source_name": source_name,
            "graph_branch": graph_branch,
            "record_count": record_count,
            "node_count": record_count,
            "edge_count": max(0, record_count - 1),
            "chronos_graph_verified": verified,
        },
    }
    out.pop("record_set", None)
    if verified:
        out["record_set_chronos_proof"] = {
            "schema": "okg.source_runner.record_set_chronos_proof.v1",
            "ok": True,
            "source_name": source_name,
            "graph_branch": graph_branch,
        }
    else:
        out.pop("record_set_chronos_proof", None)
    return out


def _init_git_repo(repo_root: Path) -> None:
    repo_root.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(repo_root), "init"], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.name", "OKG Test"],
        check=True,
    )


def test_git_files_probe_authority_requires_a_real_worktree(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    plain = tmp_path / "plain"
    plain.mkdir()
    assert (
        GitFilesSource(repo="demo", repo_root=plain).probe_short_circuit_safe
        is False
    )

    repo = tmp_path / "repo"
    _init_git_repo(repo)
    assert (
        GitFilesSource(repo="demo", repo_root=repo).probe_short_circuit_safe
        is True
    )


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def test_adapter_protocol_fields_populated() -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(repo="okg", repo_root=Path("/tmp"))
    assert src.name == "git_graph_files"
    assert src.profile == "discovery_crawl"


def test_language_for_path_covers_common_extensions() -> None:
    from okg.substrate.library.sources._git_helpers import language_for_path

    assert language_for_path("src/app.py") == "python"
    assert language_for_path("web/app.tsx") == "typescript"
    assert language_for_path("docs/README.md") == "markdown"
    assert language_for_path("unknown/no-extension") is None


def test_adapter_emits_node_facts_for_tracked_files(fixture_paths) -> None:
    """First-run cursor mode emits one NodeFact per tracked file."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.library.sources import (
        _git_graph_files_emit,
        git_graph_files,
    )
    from okg.substrate.library.sources.change_probe import emit_version_for
    from okg.substrate.library.sources.emission_identity import (
        emission_identity_targets,
    )

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    run = src.run("run-1", mode="cursor")
    token = src.change_probe.build_token()
    assert token["components"]["emit_version"]["value"] == emit_version_for(
        *emission_identity_targets(
            src.name,
            git_graph_files,
            _git_graph_files_emit,
        )
    )

    facts = list(run.facts)
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert run.applied_raw_token == token
    node_facts = [f for f in facts if isinstance(f, NodeFact)]
    source_nodes = [f for f in node_facts if f.subtype == "source_file"]
    assert node_facts, "expected at least one NodeFact for a non-empty repo"

    # Every emitted node is op='I' in cursor mode.
    assert all(n.op == "I" for n in node_facts)
    # source_record_id carries the repo identity.
    assert all(n.source_record_id["repo"] == "okg" for n in node_facts)
    # node_ids follow the file:<repo>:<path> convention.
    assert all(n.node_id.startswith("file:okg:") for n in source_nodes)
    assert all("working_tree_mtime" not in n.attrs for n in source_nodes)
    assert all("working_tree_mtime_epoch" not in n.attrs for n in source_nodes)


def test_tracked_file_fact_ignores_filesystem_timestamp_churn(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    def _source_files() -> list[NodeFact]:
        run = GitFilesSource(
            repo="okg",
            repo_root=fixture_paths.repo_root,
        ).run("acquisition", mode="cursor")
        return sorted(
            (
                fact
                for fact in run.facts
                if isinstance(fact, NodeFact)
                and fact.subtype == "source_file"
            ),
            key=lambda fact: fact.node_id,
        )

    before = _source_files()
    target = fixture_paths.repo_root / before[0].attrs["path"]
    os.utime(target, (2_000_000_000, 2_000_000_000))
    after = _source_files()

    assert after == before


def test_adapter_emits_git_branch_anchor(fixture_paths) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    branch_name = _current_branch(fixture_paths.repo_root)
    head_sha = _current_head(fixture_paths.repo_root)

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    run = src.run("run-1", mode="cursor")

    branch_nodes = [
        f for f in run.facts if isinstance(f, NodeFact) and f.subtype == "git_branch"
    ]
    assert len(branch_nodes) == 1
    assert branch_nodes[0].node_id == f"branch:okg:{branch_name}"
    assert branch_nodes[0].attrs["head_commit_sha"] == head_sha


def test_adapter_emits_new_branch_anchor_at_same_head(fixture_paths) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    first = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
    ).run("first", mode="cursor")
    list(first.facts)
    assert first.next_cursor is not None

    subprocess.run(
        [
            "git",
            "-C",
            str(fixture_paths.repo_root),
            "checkout",
            "-q",
            "-b",
            "same-head",
        ],
        check=True,
    )
    second = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
    ).run("second", mode="cursor", cursor=first.next_cursor)
    facts = list(second.facts)

    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "branch:okg:same-head"
        for fact in facts
    )


def test_adapter_can_include_untracked_worktree_files(fixture_paths) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    rel_path = "deployments/okg-workspace/fixtures/security/example.env"
    abs_path = fixture_paths.repo_root / rel_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    abs_path.write_text("TOKEN=fixture-redacted-token\n", encoding="utf-8")

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        include_untracked=True,
    )
    run = src.run("run-1", mode="cursor")

    nodes = [fact for fact in run.facts if isinstance(fact, NodeFact)]
    node = next(fact for fact in nodes if fact.node_id == f"file:okg:{rel_path}")
    assert node.attrs["path"] == rel_path
    assert node.attrs["is_dirty"] is True
    assert str(node.attrs["git_blob_sha"]).startswith("working-tree:")
    assert "working_tree_mtime" not in node.attrs
    assert "working_tree_mtime_epoch" not in node.attrs
    assert "last_modified_at" not in node.attrs
    assert "created_at" not in node.attrs


def test_include_untracked_prunes_local_claude_worktrees_before_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )

    repo_root = tmp_path / "repo"
    _init_git_repo(repo_root)
    (repo_root / ".gitignore").write_text("ignored.log\n", encoding="utf-8")
    (repo_root / "tracked.py").write_text("print('tracked')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repo_root), "add", ".gitignore", "tracked.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", "initial"],
        check=True,
    )

    (repo_root / "src").mkdir()
    (repo_root / "src/new.py").write_text("print('new')\n", encoding="utf-8")
    (repo_root / "ignored.log").write_text("ignored\n", encoding="utf-8")
    claude_cache = repo_root / ".claude/worktrees/local/archive"
    claude_cache.mkdir(parents=True)
    (claude_cache / "slow.py").write_text("print('slow')\n", encoding="utf-8")

    original_scandir = os.scandir
    claude_root = (repo_root / ".claude").resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == claude_root or claude_root in scan_path.parents:
            raise AssertionError(f"walk descended into excluded path {scan_path}")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    src = DomainGitFilesSource(
        repo_root=repo_root,
        repo="okg",
        include_untracked=True,
        exclude_globs=(".git/**",),
    )
    tracked = src._all_tracked_paths()

    assert src._all_untracked_paths(tracked) == {"src/new.py"}


def test_include_untracked_excludes_linked_worktree_git_control_file(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    main_root = tmp_path / "main"
    _init_git_repo(main_root)
    (main_root / "tracked.py").write_text("print('tracked')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(main_root), "add", "tracked.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(main_root), "commit", "-m", "initial"],
        check=True,
    )
    worktree_root = tmp_path / "linked"
    subprocess.run(
        [
            "git",
            "-C",
            str(main_root),
            "worktree",
            "add",
            "-b",
            "linked",
            str(worktree_root),
        ],
        check=True,
    )
    (worktree_root / "new.py").write_text("print('new')\n", encoding="utf-8")

    src = DomainGitFilesSource(
        repo_root=worktree_root,
        repo="okg",
        include_untracked=True,
    )
    tracked = src._all_tracked_paths()

    assert (worktree_root / ".git").is_file()
    assert src._all_untracked_paths(tracked) == {"new.py"}

    adapter = GitFilesSource(
        repo="okg",
        repo_root=worktree_root,
        include_untracked=True,
    )
    run = adapter.run("linked-worktree", mode="scope_complete")
    nodes = [fact for fact in run.facts if isinstance(fact, NodeFact)]

    assert all(node.node_id != "file:okg:.git" for node in nodes)
    assert run.applied_raw_token == adapter.change_probe.build_token()


def test_include_untracked_prunes_nested_git_repository_boundaries(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    outer_root = tmp_path / "outer"
    _init_git_repo(outer_root)
    (outer_root / "tracked.py").write_text("print('outer')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(outer_root), "add", "tracked.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(outer_root), "commit", "-m", "outer"],
        check=True,
    )
    (outer_root / "new.py").write_text("print('new')\n", encoding="utf-8")

    nested_root = outer_root / "nested"
    _init_git_repo(nested_root)
    (nested_root / "child.py").write_text("print('nested')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(nested_root), "add", "child.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(nested_root), "commit", "-m", "nested"],
        check=True,
    )

    linked_main = tmp_path / "linked-main"
    _init_git_repo(linked_main)
    (linked_main / "linked.py").write_text("print('linked')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(linked_main), "add", "linked.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(linked_main), "commit", "-m", "linked"],
        check=True,
    )
    linked_root = outer_root / "nested-linked"
    subprocess.run(
        [
            "git",
            "-C",
            str(linked_main),
            "worktree",
            "add",
            "-b",
            "nested-linked",
            str(linked_root),
        ],
        check=True,
    )
    copied_linked_root = outer_root / "copied-linked"
    shutil.copytree(linked_root, copied_linked_root)
    bare_target = tmp_path / "bare-target.git"
    subprocess.run(["git", "init", "--bare", str(bare_target)], check=True)
    bare_linked_root = outer_root / "bare-linked"
    bare_linked_root.mkdir()
    (bare_linked_root / ".git").write_text(
        f"gitdir: {bare_target}\n",
        encoding="utf-8",
    )
    (bare_linked_root / "child.py").write_text(
        "print('bare-linked')\n",
        encoding="utf-8",
    )

    domain = DomainGitFilesSource(
        repo_root=outer_root,
        repo="okg",
        include_untracked=True,
    )
    tracked = domain._all_tracked_paths()
    adapter = GitFilesSource(
        repo="okg",
        repo_root=outer_root,
        include_untracked=True,
    )

    assert domain._all_untracked_paths(tracked) == {"new.py"}
    assert adapter.change_probe._all_untracked_paths() == {"new.py"}
    run = adapter.run("nested-repositories", mode="scope_complete")
    list(run.facts)
    assert run.applied_raw_token == adapter.change_probe.build_token()


def test_include_untracked_keeps_files_below_fake_git_directory(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo_root = tmp_path / "repo"
    _init_git_repo(repo_root)
    (repo_root / "tracked.py").write_text("print('tracked')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repo_root), "add", "tracked.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", "initial"],
        check=True,
    )
    fake_git = repo_root / "container" / ".git"
    fake_git.mkdir(parents=True)
    (fake_git / "note.txt").write_text("not repository data\n", encoding="utf-8")
    (repo_root / "container" / "child.py").write_text(
        "print('child')\n",
        encoding="utf-8",
    )

    domain = DomainGitFilesSource(
        repo_root=repo_root,
        repo="okg",
        include_untracked=True,
    )
    adapter = GitFilesSource(
        repo="okg",
        repo_root=repo_root,
        include_untracked=True,
    )
    domain_paths = domain._all_untracked_paths(domain._all_tracked_paths())
    probe_paths = adapter.change_probe._all_untracked_paths()

    assert "container/child.py" in domain_paths
    assert domain_paths == probe_paths
    run = adapter.run("fake-git-directory", mode="scope_complete")
    list(run.facts)
    assert run.applied_raw_token == adapter.change_probe.build_token()


def test_include_untracked_excludes_unemittable_broken_symlink(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo_root = tmp_path / "repo"
    _init_git_repo(repo_root)
    (repo_root / "tracked.py").write_text("print('tracked')\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repo_root), "add", "tracked.py"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", "initial"],
        check=True,
    )
    (repo_root / "broken.py").symlink_to("missing.py")

    domain = DomainGitFilesSource(
        repo_root=repo_root,
        repo="okg",
        include_untracked=True,
    )
    adapter = GitFilesSource(
        repo="okg",
        repo_root=repo_root,
        include_untracked=True,
    )
    domain_paths = domain._all_untracked_paths(domain._all_tracked_paths())
    probe_paths = adapter.change_probe._all_untracked_paths()

    assert domain_paths == set()
    assert probe_paths == set()
    run = adapter.run("broken-symlink", mode="scope_complete")
    list(run.facts)
    assert run.applied_raw_token == adapter.change_probe.build_token()


def test_adapter_emits_critical_files_from_fixture(fixture_paths) -> None:
    """Sanity-pin the fixture: the three critical source-file ids
    listed in CRITICAL_NODE_IDS must appear in the first-run emit."""
    from okg_self_micro import CRITICAL_NODE_IDS  # type: ignore
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    run = src.run("run-1", mode="cursor")
    emitted_ids = {f.node_id for f in run.facts if hasattr(f, "node_id")}

    expected_files = {nid for nid in CRITICAL_NODE_IDS if nid.startswith("file:okg:")}
    missing = expected_files - emitted_ids
    assert not missing, (
        f"adapter did not emit critical fixture file ids: {sorted(missing)}"
    )


def test_adapter_skips_gitlink_submodule_entries(tmp_path) -> None:
    """Submodules are directory anchors, not source_file blobs."""
    from okg.substrate.library.sources._git_helpers import (
        in_proc_list_tracked,
        in_proc_ls_tree,
    )
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    child = tmp_path / "child"
    _init_git_repo(child)
    (child / "README.md").write_text("# child\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(child), "add", "README.md"], check=True)
    subprocess.run(
        ["git", "-C", str(child), "commit", "-m", "child root"],
        check=True,
    )

    parent = tmp_path / "parent"
    _init_git_repo(parent)
    (parent / "app.py").write_text("print('okg')\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(parent), "add", "app.py"], check=True)
    subprocess.run(
        ["git", "-C", str(parent), "commit", "-m", "parent root"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(parent),
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            str(child),
            "vendor/child",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(parent), "commit", "-m", "add submodule"], check=True
    )

    assert "vendor/child" not in in_proc_list_tracked(parent)
    assert in_proc_ls_tree(parent, {"app.py", "vendor/child"}) == {
        "app.py": (_current_head_blob(parent, "app.py"), len("print('okg')\n")),
    }

    src = GitFilesSource(repo="okg", repo_root=parent)
    run = src.run("run-1", mode="cursor")
    source_ids = {
        f.node_id
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "source_file"
    }

    assert "file:okg:app.py" in source_ids
    assert "file:okg:.gitmodules" in source_ids
    assert "file:okg:vendor/child" not in source_ids


def _current_head_blob(repo_root: Path, path: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo_root), "rev-parse", f"HEAD:{path}"],
        text=True,
    ).strip()


def test_scoped_run_emits_only_requested_file(fixture_paths) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    run = src.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["docs/usage.md"]},
    )
    emitted_ids = {
        f.node_id for f in run.facts if getattr(f, "subtype", None) == "source_file"
    }

    assert emitted_ids == {"file:okg:docs/usage.md"}
    assert run.completed_scope is False
    assert run.record_set is None


def test_scoped_run_does_not_enumerate_full_repo(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    def fail_full_enumeration(self):  # noqa: ANN001
        raise AssertionError("scoped git-files run enumerated the full repo")

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_tracked_paths",
        fail_full_enumeration,
    )
    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_untracked_paths",
        fail_full_enumeration,
    )

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    run = src.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["docs/usage.md"]},
    )
    emitted_ids = {
        f.node_id for f in run.facts if getattr(f, "subtype", None) == "source_file"
    }

    assert emitted_ids == {"file:okg:docs/usage.md"}


def test_adapter_node_set_matches_domain(fixture_paths) -> None:
    """Graph-shape parity: the substrate adapter's emitted node_id
    set equals the domain domain class's emitted node_id set on
    the same fixture."""
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    domain = DomainGitFilesSource(
        repo_root=fixture_paths.repo_root,
        repo="okg",
    )
    domain_node_ids = {n.node_id for n in domain.nodes()}

    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    run = src.run("run-1", mode="cursor")
    substrate_node_ids = {
        f.node_id
        for f in run.facts
        if getattr(f, "subtype", None) == "source_file" and f.op == "I"
    }
    assert substrate_node_ids == domain_node_ids


def test_next_cursor_carries_tree_sha(fixture_paths) -> None:
    """The substrate cursor must round-trip the domain state shape
    (tree_sha + dirty_paths) so a second cursor-mode run can
    short-circuit on no change."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    run = src.run("run-1", mode="cursor")
    list(run.facts)  # drain to populate state

    cursor = run.next_cursor or {}
    assert "tree_sha" in cursor and cursor["tree_sha"], (
        "expected non-empty tree_sha in next_cursor"
    )
    assert "dirty_paths" in cursor


def test_second_cursor_run_with_unchanged_tree_emits_no_facts(
    fixture_paths,
) -> None:
    """Round-trip the cursor: unchanged tree + unchanged HEAD is a true no-op."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    first = src.run("run-1", mode="cursor")
    list(first.facts)  # materialize cursor

    src2 = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    second = src2.run("run-2", mode="cursor", cursor=first.next_cursor)
    second_facts = [f for f in second.facts if isinstance(f, NodeFact)]
    assert second_facts == []
    assert second.next_cursor["head_sha"] == _current_head(fixture_paths.repo_root)


def test_scoped_no_change_ignores_unrelated_dirty_paths(
    fixture_paths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="cursor",
    )
    list(first.facts)
    cursor = first.next_cursor
    assert cursor is not None

    (fixture_paths.repo_root / "README.md").write_text(
        "# dirty outside scoped frontier\n",
        encoding="utf-8",
    )

    def fail_full_dirty_paths(self):  # noqa: ANN001
        raise AssertionError("scoped git-files run computed full repo dirty paths")

    def fail_last_modified(repo_root, paths):  # noqa: ANN001
        raise AssertionError("scoped no-change run resolved git metadata")

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_tracked_dirty_paths",
        fail_full_dirty_paths,
    )
    monkeypatch.setattr(
        _git_graph_files_emit,
        "in_proc_path_history",
        fail_last_modified,
    )

    src = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    second = src.run(
        "run-2",
        mode="scope_incomplete",
        cursor=cursor,
        sync_scope={"scope_keys": ["docs/usage.md"]},
    )
    facts = list(second.facts)

    assert facts == []
    assert second.next_cursor["tree_sha"] == cursor["tree_sha"]
    assert second.next_cursor["head_sha"] == cursor["head_sha"]
    assert src.last_run_stats["scope_status"] == "scoped_unchanged_noop"
    assert src.last_run_stats["dirty_strategy"] == "scoped"
    assert src.last_run_stats["dirty_paths_checked"] == 1
    assert src.last_run_stats["files_emitted"] == 0
    assert src.last_run_stats["branch_anchor_emitted"] is False
    assert src.last_run_stats["emitted_nodes"] == 0
    assert src.last_run_stats["emitted_edges"] == 0


def test_scope_complete_with_unchanged_tree_carries_prior_record_set(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    branch_name = _current_branch(fixture_paths.repo_root)

    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    second = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
    )
    facts = [f for f in second.facts if isinstance(f, NodeFact)]

    assert [f.node_id for f in facts] == [f"branch:okg:{branch_name}"]
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == set(first.record_set.records)
    assert f"__branch__:{branch_name}" in second.record_set.records


@pytest.mark.parametrize("stored", ["inline", "by-reference"])
def test_scope_complete_branch_switch_with_unchanged_tree_relinks_every_file(
    fixture_paths,
    stored: str,
) -> None:
    """Issue #3780: the run after a switch to a branch at the same tree
    rebuilds the whole footprint on the new branch. Before the fix it kept
    the old branch record and linked no file to the new one."""
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    root = fixture_paths.repo_root
    old_branch = _current_branch(root)
    first = GitFilesSource(
        repo="okg", repo_root=root, source_name="okg-workspace.git-files"
    ).run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    file_keys = {
        key for key in first.record_set.records if not key.startswith("__branch__:")
    }
    assert file_keys
    if stored == "inline":
        cursor = dict(first.next_cursor or {})
        cursor["record_set"] = first.record_set.to_cursor()
    else:
        cursor = _external_record_set_cursor(
            first.next_cursor or {},
            record_count=len(first.record_set.records),
        )

    subprocess.run(
        ["git", "-C", str(root), "checkout", "-q", "-b", "switched"],
        check=True,
    )
    second = GitFilesSource(
        repo="okg", repo_root=root, source_name="okg-workspace.git-files"
    ).run("run-2", mode="scope_complete", cursor=cursor)
    facts = list(second.facts)

    assert second.completed_scope is True
    assert second.record_set is not None
    # The whole footprint, diffed by the runner against the prior: no slice.
    assert second.record_set_replacement_keys is None
    records = second.record_set.records
    assert f"__branch__:{old_branch}" not in records
    assert "__branch__:switched" in records
    assert set(records) == file_keys | {"__branch__:switched"}
    for key in file_keys:
        edges = records[key].edges
        if records[key].primary.subtype == "source_file":
            assert [e.dst for e in edges] == ["branch:okg:switched"]
    linked = {
        f.src
        for f in facts
        if isinstance(f, EdgeFact)
        and f.edge_type == "member_of"
        and f.dst == "branch:okg:switched"
    }
    assert linked == {
        records[key].primary.node_id
        for key in file_keys
        if records[key].primary.subtype == "source_file"
    }
    assert second.next_cursor is not None
    assert second.next_cursor["branch_name"] == "switched"
    assert second.next_cursor["files_linked_branch"] == "switched"


@pytest.mark.parametrize("between", ["cursor", "scoped"])
def test_branch_switch_seen_by_a_partial_run_still_relinks_on_complete_run(
    fixture_paths,
    between: str,
) -> None:
    """Issue #3780: a cursor or scoped run after the switch links only what
    it reads, so the cursor keeps the branch the files are linked to and the
    next complete run still rebuilds the footprint on the new branch."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    root = fixture_paths.repo_root
    old_branch = _current_branch(root)
    first = GitFilesSource(repo="okg", repo_root=root).run(
        "run-1", mode="scope_complete"
    )
    list(first.facts)
    assert first.record_set is not None
    assert first.next_cursor is not None
    assert first.next_cursor["files_linked_branch"] == old_branch
    file_keys = {
        key for key in first.record_set.records if not key.startswith("__branch__:")
    }

    subprocess.run(
        ["git", "-C", str(root), "checkout", "-q", "-b", "switched"],
        check=True,
    )
    if between == "cursor":
        partial = GitFilesSource(repo="okg", repo_root=root).run(
            "run-2", mode="cursor", cursor=first.next_cursor
        )
    else:
        partial = GitFilesSource(repo="okg", repo_root=root).run(
            "run-2",
            mode="scope_complete",
            cursor={
                **first.next_cursor,
                "record_set": first.record_set.to_cursor(),
            },
            sync_scope={"scope_keys": [sorted(file_keys)[0]]},
        )
    list(partial.facts)
    assert partial.next_cursor is not None
    assert partial.next_cursor["branch_name"] == "switched"
    assert partial.next_cursor["files_linked_branch"] == old_branch

    cursor = {
        **partial.next_cursor,
        "record_set": first.record_set.to_cursor(),
    }
    third = GitFilesSource(repo="okg", repo_root=root).run(
        "run-3", mode="scope_complete", cursor=cursor
    )
    list(third.facts)
    assert third.record_set is not None
    assert third.record_set_replacement_keys is None
    assert set(third.record_set.records) == file_keys | {"__branch__:switched"}
    assert third.next_cursor is not None
    assert third.next_cursor["files_linked_branch"] == "switched"


def test_scope_complete_external_record_set_ref_unchanged_tree_is_noop(
    fixture_paths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    branch_name = _current_branch(fixture_paths.repo_root)
    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.next_cursor is not None
    assert first.record_set is not None

    inventory_calls = 0

    def count_full_inventory(self):  # noqa: ANN001
        nonlocal inventory_calls
        inventory_calls += 1
        raise AssertionError("unchanged verified external cursor rebuilt inventory")

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_tracked_paths",
        count_full_inventory,
    )
    cursor = _external_record_set_cursor(
        first.next_cursor,
        record_count=len(first.record_set.records),
        verified=True,
    )
    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    second = src.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
    )
    facts = [fact for fact in second.facts if isinstance(fact, NodeFact)]

    assert inventory_calls == 0
    assert [fact.node_id for fact in facts] == [f"branch:okg:{branch_name}"]
    assert not any(fact.subtype == "source_file" for fact in facts)
    assert second.completed_scope is True
    # Issue #3666: the run declares its branch-only record set as a
    # replacement slice, which the runner merges into the external prior.
    # Declaring none let the runner read the branch record alone as the whole
    # footprint and retract every file.
    assert second.record_set is not None
    assert set(second.record_set.records) == {f"__branch__:{branch_name}"}
    assert second.record_set_replacement_keys == {f"__branch__:{branch_name}"}
    assert second.next_cursor is not None
    assert src.last_run_stats["scope_status"] == "full_unchanged_noop"
    assert src.last_run_stats["files_emitted"] == 0
    assert src.last_run_stats["tree_changed"] is False


def test_reconcile_external_record_set_ref_unchanged_tree_is_noop(
    fixture_paths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    branch_name = _current_branch(fixture_paths.repo_root)
    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.next_cursor is not None
    assert first.record_set is not None

    def fail_full_inventory(self):  # noqa: ANN001
        raise AssertionError("unchanged verified external cursor rebuilt inventory")

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_tracked_paths",
        fail_full_inventory,
    )
    cursor = _external_record_set_cursor(
        first.next_cursor,
        record_count=len(first.record_set.records),
        verified=True,
    )
    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    second = src.run("run-2", mode="reconcile", cursor=cursor)
    facts = [fact for fact in second.facts if isinstance(fact, NodeFact)]

    assert [fact.node_id for fact in facts] == [f"branch:okg:{branch_name}"]
    assert second.completed_scope is True
    # Issue #3666: a branch-only replacement slice, as for scope_complete.
    assert second.record_set is not None
    assert set(second.record_set.records) == {f"__branch__:{branch_name}"}
    assert second.record_set_replacement_keys == {f"__branch__:{branch_name}"}
    assert second.next_cursor is not None
    assert src.last_run_stats["scope_status"] == "full_unchanged_noop"
    assert src.last_run_stats["files_emitted"] == 0


def test_scope_complete_unverified_external_record_set_ref_does_not_noop(
    fixture_paths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.next_cursor is not None
    assert first.record_set is not None

    inventory_calls = 0
    original_all_tracked = _git_graph_files_emit.GitFilesSource._all_tracked_paths

    def count_full_inventory(self):  # noqa: ANN001
        nonlocal inventory_calls
        inventory_calls += 1
        return original_all_tracked(self)

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_tracked_paths",
        count_full_inventory,
    )
    cursor = _external_record_set_cursor(
        first.next_cursor,
        record_count=len(first.record_set.records),
        verified=False,
    )
    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    second = src.run("run-2", mode="scope_complete", cursor=cursor)
    facts = [fact for fact in second.facts if isinstance(fact, NodeFact)]

    assert inventory_calls == 1
    assert any(fact.subtype == "source_file" for fact in facts)
    assert second.completed_scope is True
    assert second.record_set is not None
    assert second.record_set_replacement_keys is None
    assert second.next_cursor is not None
    assert "record_set_ref" not in second.next_cursor
    assert src.last_run_stats["scope_status"] == "full_emit"


def test_scope_complete_mismatched_external_record_set_proof_does_not_noop(
    fixture_paths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.next_cursor is not None
    assert first.record_set is not None

    inventory_calls = 0
    original_all_tracked = _git_graph_files_emit.GitFilesSource._all_tracked_paths

    def count_full_inventory(self):  # noqa: ANN001
        nonlocal inventory_calls
        inventory_calls += 1
        return original_all_tracked(self)

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_tracked_paths",
        count_full_inventory,
    )
    cursor = _external_record_set_cursor(
        first.next_cursor,
        record_count=len(first.record_set.records),
        verified=True,
        source_name="other.git-files",
    )
    src = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        source_name="okg-workspace.git-files",
    )
    second = src.run("run-2", mode="scope_complete", cursor=cursor)
    facts = [fact for fact in second.facts if isinstance(fact, NodeFact)]

    assert inventory_calls == 1
    assert any(fact.subtype == "source_file" for fact in facts)
    assert second.completed_scope is True
    assert second.record_set is not None
    assert second.next_cursor is not None
    assert "record_set_ref" not in second.next_cursor
    assert src.last_run_stats["scope_status"] == "full_emit"


def test_scope_complete_external_record_set_ref_recovers_missing_tracked_file(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    first = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.record_set is not None
    missing_key = next(
        key for key in first.record_set.records
        if key != f"__branch__:{_current_branch(fixture_paths.repo_root)}"
    )

    cursor = {
        **(first.next_cursor or {}),
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "incomplete",
        },
        "record_set_summary": {
            "record_count": len(first.record_set.records) - 1,
        },
    }

    second = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root).run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
    )
    list(second.facts)

    assert second.completed_scope is True
    assert second.record_set is not None
    assert missing_key in second.record_set.records
    assert set(second.record_set.records) == set(first.record_set.records)
    assert second.record_set_replacement_keys is None


def test_scope_complete_external_record_set_ref_replaces_empty_inventory(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo_root = tmp_path / "repo"
    _init_git_repo(repo_root)
    (repo_root / "a.txt").write_text("alpha\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_root), "add", "a.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", "add file"],
        check=True,
    )

    first = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.next_cursor is not None
    assert first.record_set is not None
    assert "a.txt" in first.record_set.records

    subprocess.run(["git", "-C", str(repo_root), "rm", "a.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", "remove file"],
        check=True,
    )
    branch_key = f"__branch__:{_current_branch(repo_root)}"
    cursor = {
        **first.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "stale",
        },
        "record_set_summary": {
            "record_count": len(first.record_set.records),
        },
    }

    second = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
    )
    list(second.facts)

    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {branch_key}
    assert second.record_set_replacement_keys is None
    assert second.next_cursor is not None
    assert "record_set_ref" not in second.next_cursor


def test_scope_complete_external_record_set_ref_repairs_dirty_frontier(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    rel_path = "tests/okg-workspace/test_coord_cli.py"
    abs_path = fixture_paths.repo_root / rel_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    abs_path.write_text("def test_cli():\n    pass\n", encoding="utf-8")

    first = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        include_untracked=True,
    ).run("run-1", mode="cursor")
    list(first.facts)
    cursor = {
        **(first.next_cursor or {}),
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 3},
    }

    second = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
        include_untracked=True,
    ).run("run-2", mode="scope_complete", cursor=cursor)
    facts = [fact for fact in second.facts if isinstance(fact, NodeFact)]

    assert any(fact.node_id == f"file:okg:{rel_path}" for fact in facts)
    assert second.completed_scope is True
    assert second.record_set is not None
    assert rel_path in second.record_set.records
    assert second.record_set_replacement_keys is None


def test_scope_complete_with_cursor_removes_deleted_file_record(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo_root = fixture_paths.repo_root
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.name", "OKG Test"],
        check=True,
    )

    first = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    subprocess.run(["git", "-C", str(repo_root), "rm", "docs/usage.md"], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", "remove usage docs"],
        check=True,
    )

    second = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
    )
    facts = [f for f in second.facts if isinstance(f, NodeFact)]

    assert any(
        f.node_id == "file:okg:docs/usage.md" and f.op == "R"
        for f in facts
    )
    assert second.completed_scope is True
    assert second.record_set is not None
    assert "docs/usage.md" not in second.record_set.records


def test_scope_complete_type_change_keeps_one_file_record(fixture_paths) -> None:
    """A file that becomes a symlink is one changed file, not a deleted one
    plus a created one (issue #3666): no retraction, one current record."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo_root = fixture_paths.repo_root
    git = ["git", "-C", str(repo_root)]
    subprocess.run([*git, "config", "user.email", "test@example.com"], check=True)
    subprocess.run([*git, "config", "user.name", "OKG Test"], check=True)

    first = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-1",
        mode="scope_complete",
    )
    list(first.facts)
    assert first.record_set is not None
    assert "docs/usage.md" in first.record_set.records
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    # The text moves to a new file and the old path becomes a link to it,
    # which rename detection would otherwise pair.
    subprocess.run([*git, "mv", "docs/usage.md", "docs/usage-v2.md"], check=True)
    (repo_root / "docs" / "usage.md").symlink_to("usage-v2.md")
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "link usage docs"], check=True)

    second = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
    )
    facts = [f for f in second.facts if isinstance(f, NodeFact)]
    usage = [f for f in facts if f.node_id == "file:okg:docs/usage.md"]

    assert [f.op for f in usage if f.op == "R"] == []
    assert len(usage) == 1
    assert second.record_set is not None
    assert "docs/usage.md" in second.record_set.records
    assert "docs/usage-v2.md" in second.record_set.records


def test_tree_diff_counts_a_type_change_once_as_modified(tmp_path: Path) -> None:
    """A file that becomes a symlink is one added or modified path (issue
    #3666); a file that becomes a submodule entry is still deleted, because
    the files source keeps no record for a submodule."""
    from okg.substrate.library.sources._git_helpers import in_proc_diff_name_status

    root = tmp_path / "repo"
    root.mkdir()
    git = ["git", "-C", str(root)]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "config", "user.email", "test@example.com"], check=True)
    subprocess.run([*git, "config", "user.name", "OKG Test"], check=True)
    body = "".join(f"line {i} of a file long enough to pair\n" for i in range(40))
    (root / "f.txt").write_text(body, encoding="utf-8")
    (root / "c.txt").write_text("c\n" * 30, encoding="utf-8")
    (root / "m.txt").write_text("module placeholder\n", encoding="utf-8")
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "one"], check=True)
    first = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
    prior = subprocess.check_output([*git, "rev-parse", "HEAD^{tree}"], text=True)
    subprocess.run([*git, "mv", "f.txt", "g.txt"], check=True)
    (root / "f.txt").symlink_to("g.txt")
    (root / "c.txt").unlink()
    (root / "c.txt").symlink_to("g.txt")
    subprocess.run([*git, "add", "-A"], check=True)
    # m.txt becomes a submodule entry (a gitlink).
    subprocess.run([*git, "rm", "-q", "m.txt"], check=True)
    subprocess.run(
        [*git, "update-index", "--add", "--cacheinfo", f"160000,{first},m.txt"],
        check=True,
    )
    subprocess.run([*git, "commit", "-q", "-m", "two"], check=True)
    cur = subprocess.check_output([*git, "rev-parse", "HEAD^{tree}"], text=True)
    assert "160000 commit" in subprocess.check_output(
        [*git, "ls-tree", "HEAD", "m.txt"], text=True
    )

    added, deleted = in_proc_diff_name_status(root, prior.strip(), cur.strip())

    assert {"f.txt", "g.txt", "c.txt"} <= added
    assert deleted == {"m.txt"}


def test_empty_commit_with_same_tree_emits_updated_branch_anchor(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo_root = fixture_paths.repo_root
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.name", "OKG Test"],
        check=True,
    )

    src = GitFilesSource(repo="okg", repo_root=repo_root)
    first = src.run("run-1", mode="cursor")
    list(first.facts)
    first_tree = first.next_cursor["tree_sha"]

    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "--allow-empty", "-m", "empty"],
        check=True,
    )
    new_head = _current_head(repo_root)
    branch_name = _current_branch(repo_root)

    second = GitFilesSource(repo="okg", repo_root=repo_root).run(
        "run-2",
        mode="cursor",
        cursor=first.next_cursor,
    )
    facts = [f for f in second.facts if isinstance(f, NodeFact)]

    assert second.next_cursor["tree_sha"] == first_tree
    assert [f.node_id for f in facts] == [f"branch:okg:{branch_name}"]
    assert facts[0].attrs["head_commit_sha"] == new_head


def test_cursor_mode_does_not_emit_retractions(fixture_paths) -> None:
    """Even if the domain emitter collected stale_node_ids, cursor
    mode is not authorized to emit op='R' rows."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    run = src.run("run-1", mode="cursor")
    retractions = [f for f in run.facts if isinstance(f, NodeFact) and f.op == "R"]
    assert retractions == []


def test_reconcile_mode_completed_scope_is_true(fixture_paths) -> None:
    """`completed_scope` is the runner's signal that absence implies
    retraction. True only in reconcile / scope_complete modes."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    run_cursor = src.run("rc", mode="cursor")
    assert run_cursor.completed_scope is False

    src2 = GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root)
    run_reconcile = src2.run("rr", mode="reconcile")
    assert run_reconcile.completed_scope is True
    assert run_reconcile.record_set is not None
    member_of_edges = [
        edge
        for record in run_reconcile.record_set.records.values()
        for edge in record.edges
        if edge.edge_type == "member_of"
    ]
    assert member_of_edges


@pytest.mark.postgres
def test_record_set_retracts_deleted_file(
    clean_runtime: str,
    fixture_paths,
) -> None:
    import psycopg

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    source_name = "test.git-files-record-set"
    runner = SourceRunner(clean_runtime, source_name=source_name)
    first = runner.run(
        GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root),
        mode="scope_complete",
    )
    assert first.next_cursor is not None
    assert "record_set" in first.next_cursor

    subprocess.run(
        [
            "git",
            "-C",
            str(fixture_paths.repo_root),
            "config",
            "user.email",
            "test@example.com",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(fixture_paths.repo_root), "config", "user.name", "OKG Test"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(fixture_paths.repo_root), "rm", "docs/usage.md"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(fixture_paths.repo_root),
            "commit",
            "-m",
            "remove usage docs",
        ],
        check=True,
    )

    second = runner.run(
        GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root),
        mode="scope_complete",
    )
    assert second.retractions_node == 1

    assert (
        _graph_node_exists(
            clean_runtime,
            node_id="file:okg:docs/usage.md",
        )
        is False
    )


@pytest.mark.postgres
def test_scope_complete_path_delete_retracts_exact_file(
    clean_runtime: str,
    fixture_paths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources import _git_graph_files_emit
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    runner = SourceRunner(clean_runtime, source_name="test.git-files-scoped-delete")
    first = runner.run(
        GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root),
        mode="scope_complete",
    )
    assert first.completed_scope is True
    assert "record_set" in (first.next_cursor or {})

    subprocess.run(
        [
            "git",
            "-C",
            str(fixture_paths.repo_root),
            "config",
            "user.email",
            "test@example.com",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(fixture_paths.repo_root), "config", "user.name", "OKG Test"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(fixture_paths.repo_root), "rm", "docs/usage.md"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(fixture_paths.repo_root),
            "commit",
            "-m",
            "remove usage docs scoped",
        ],
        check=True,
    )

    def fail_full_inventory(self):  # noqa: ANN001
        raise AssertionError("scoped delete widened to full git inventory")

    monkeypatch.setattr(
        _git_graph_files_emit.GitFilesSource,
        "_all_tracked_paths",
        fail_full_inventory,
    )

    adapter_run = GitFilesSource(
        repo="okg",
        repo_root=fixture_paths.repo_root,
    ).run(
        "adapter-scoped-delete",
        mode="scope_complete",
        cursor=first.next_cursor,
        sync_scope={"scope_keys": ["docs/usage.md"]},
    )
    adapter_facts = list(adapter_run.facts)
    assert adapter_run.completed_scope is False
    assert adapter_run.record_set is not None
    assert adapter_run.record_set_replacement_keys == {
        "docs/usage.md",
        f"__branch__:{_current_branch(fixture_paths.repo_root)}",
    }
    assert not any(
        isinstance(fact, NodeFact) and fact.op == "R"
        for fact in adapter_facts
    )

    second = runner.run(
        GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root),
        mode="scope_complete",
        sync_scope={"scope_keys": ["docs/usage.md"]},
    )

    assert second.completed_scope is False
    assert second.retractions_node == 1
    assert second.retractions_edge == 1
    assert (
        _graph_node_exists(
            clean_runtime,
            node_id="file:okg:docs/usage.md",
        )
        is False
    )
    assert (
        _graph_node_exists(
            clean_runtime,
            node_id="file:okg:README.md",
        )
        is True
    )


def _graph_node_exists(dsn: str, *, node_id: str) -> bool:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT 1 AS present
              FROM okg.graph_nodes
             WHERE node_id = :node_id
             LIMIT 1
            """,
            {"node_id": node_id},
        )
    return bool(rows)


# --- issue #3259: a scope key narrows the source's paths, never widens them --


def _cmssw_like_repo(repo_root: Path) -> None:
    _init_git_repo(repo_root)
    files = {
        "DataFormats/Track/Track.cc": "track\n",
        "DataFormats/Track/gen/Dict.cc": "generated\n",
        "DataFormats/Track/Track.lock": "lock\n",
        "RecoTracker/Seed/Seed.cc": "seed\n",
        "FWCore/Framework/src/Event.cc": "event\n",
        "FWCore/Framework/src/Run.cc": "run\n",
    }
    for rel, text in files.items():
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_root), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-q", "-m", "initial"],
        check=True,
    )


_CMSSW_INCLUDES = ["DataFormats/**", "RecoTracker/**"]


def _source_file_ids(run) -> set[str]:  # noqa: ANN001
    return {
        f.node_id
        for f in run.facts
        if getattr(f, "subtype", None) == "source_file"
    }


def test_scoped_run_reads_only_scope_keys_inside_include_globs(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "cmssw"
    _cmssw_like_repo(repo)
    src = GitFilesSource(
        repo="cmssw",
        repo_root=repo,
        include_globs=list(_CMSSW_INCLUDES),
    )

    run = src.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                "DataFormats/Track/Track.cc",
                "FWCore/Framework/src/Event.cc",
            ],
        },
    )

    assert _source_file_ids(run) == {"file:cmssw:DataFormats/Track/Track.cc"}
    assert src.last_run_stats["scoped"] is True
    assert src.last_run_stats["scope_keys_outside_source_paths"] == 1
    assert src.last_run_stats["scope_keys"] == ["DataFormats/Track/Track.cc"]


def test_scoped_run_with_only_outside_keys_reads_nothing(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "cmssw"
    _cmssw_like_repo(repo)
    # An uncommitted edit outside the source's paths must not reach the
    # cursor's dirty frontier either.
    (repo / "FWCore/Framework/src/Event.cc").write_text(
        "event v2\n", encoding="utf-8",
    )
    src = GitFilesSource(
        repo="cmssw",
        repo_root=repo,
        include_globs=list(_CMSSW_INCLUDES),
    )

    run = src.run(
        "run-scoped-outside",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["FWCore/Framework/src/Event.cc"]},
    )

    assert _source_file_ids(run) == set()
    assert src.last_run_stats["scope_keys_outside_source_paths"] == 1
    assert "FWCore/Framework/src/Event.cc" not in (
        run.next_cursor or {}
    ).get("dirty_paths", [])


def test_scoped_run_exclude_globs_win_over_scope_keys(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "cmssw"
    _cmssw_like_repo(repo)
    keys = [
        "DataFormats/Track/Track.cc",
        "DataFormats/Track/gen/Dict.cc",
        "DataFormats/Track/Track.lock",
    ]

    explicit = GitFilesSource(
        repo="cmssw",
        repo_root=repo,
        include_globs=list(_CMSSW_INCLUDES),
        exclude_globs=("DataFormats/**/gen/**",),
    )
    explicit_run = explicit.run(
        "run-explicit-exclude",
        mode="scope_incomplete",
        sync_scope={"scope_keys": keys},
    )
    # An explicit exclude list replaces the defaults, so the .lock file is
    # inside this source's paths and the generated file is not.
    assert _source_file_ids(explicit_run) == {
        "file:cmssw:DataFormats/Track/Track.cc",
        "file:cmssw:DataFormats/Track/Track.lock",
    }
    assert explicit.last_run_stats["scope_keys_outside_source_paths"] == 1

    defaults = GitFilesSource(
        repo="cmssw",
        repo_root=repo,
        include_globs=list(_CMSSW_INCLUDES),
    )
    defaults_run = defaults.run(
        "run-default-exclude",
        mode="scope_incomplete",
        sync_scope={"scope_keys": keys},
    )
    # The default excludes apply to a scoped run exactly as to a full run.
    full = GitFilesSource(
        repo="cmssw",
        repo_root=repo,
        include_globs=list(_CMSSW_INCLUDES),
    ).run("run-full", mode="reconcile")
    full_ids = _source_file_ids(full)
    assert "file:cmssw:DataFormats/Track/Track.lock" not in full_ids
    assert _source_file_ids(defaults_run) == {
        "file:cmssw:DataFormats/Track/Track.cc",
        "file:cmssw:DataFormats/Track/gen/Dict.cc",
    }
    assert _source_file_ids(defaults_run) <= full_ids
    assert defaults.last_run_stats["scope_keys_outside_source_paths"] == 1


def test_scope_complete_outside_keys_neither_retract_nor_resurrect(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "cmssw"
    _cmssw_like_repo(repo)

    def source() -> GitFilesSource:
        return GitFilesSource(
            repo="cmssw",
            repo_root=repo,
            include_globs=list(_CMSSW_INCLUDES),
        )

    first = source().run("run-full", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    full_keys = set(first.record_set.records)
    assert "DataFormats/Track/Track.cc" in full_keys
    assert not any(key.startswith("FWCore/") for key in full_keys)
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    # One outside file is deleted, one outside file and one inside file
    # change; the watcher sends an event for each.
    subprocess.run(
        ["git", "-C", str(repo), "rm", "-q", "FWCore/Framework/src/Event.cc"],
        check=True,
    )
    (repo / "FWCore/Framework/src/Run.cc").write_text("run v2\n", encoding="utf-8")
    (repo / "DataFormats/Track/Track.cc").write_text("track v2\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "edit"],
        check=True,
    )
    branch = _current_branch(repo)

    adapter = source()
    scoped = adapter.run(
        "run-scoped",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={
            "scope_keys": [
                "FWCore/Framework/src/Event.cc",
                "FWCore/Framework/src/Run.cc",
                "DataFormats/Track/Track.cc",
            ],
        },
    )
    facts = list(scoped.facts)

    emitted = {
        f.node_id for f in facts if getattr(f, "subtype", None) == "source_file"
    }
    assert emitted == {"file:cmssw:DataFormats/Track/Track.cc"}
    assert not any(getattr(f, "op", "I") == "R" for f in facts)
    assert scoped.record_set is not None
    assert set(scoped.record_set.records) == {
        "DataFormats/Track/Track.cc",
        f"__branch__:{branch}",
    }
    assert scoped.record_set_replacement_keys == {
        "DataFormats/Track/Track.cc",
        f"__branch__:{branch}",
    }
    assert adapter.last_run_stats["scope_keys_outside_source_paths"] == 2


def _commit_all(repo_root: Path, message: str) -> None:
    subprocess.run(["git", "-C", str(repo_root), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-q", "-m", message],
        check=True,
    )


def _untracked_parity_repo(repo_root: Path) -> None:
    """Tracked files plus untracked files the full crawl's walk refuses."""
    _init_git_repo(repo_root)
    files = {
        ".gitignore": "*.tmp\n",
        "DataFormats/Track/Track.cc": "track\n",
        "tools/build.py": "tracked under a pruned dir\n",
        "top.py": "top\n",
    }
    for rel, text in files.items():
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _commit_all(repo_root, "initial")
    untracked = {
        # gitignored, under an included directory
        "DataFormats/Track/cache.tmp": "ignored\n",
        # under .claude/, which the walk always prunes
        ".claude/notes.py": "agent notes\n",
        # an untracked file the walk does admit
        "DataFormats/Track/New.cc": "new\n",
        "new_top.py": "new top\n",
        # under a subdirectory a `*.py` include prunes
        "tools/gen.py": "untracked under a pruned dir\n",
    }
    for rel, text in untracked.items():
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    nested = repo_root / "vendor" / "lib"
    _init_git_repo(nested)
    (nested / "lib.py").write_text("nested\n", encoding="utf-8")
    _commit_all(nested, "nested")


def _scoped_ids(src, keys: list[str], *, cursor=None):  # noqa: ANN001
    run = src.run(
        "run-scoped",
        mode="scope_incomplete",
        cursor=cursor,
        sync_scope={"scope_keys": keys},
    )
    return _source_file_ids(run), run


@pytest.mark.parametrize(
    ("include_globs", "keys", "expected"),
    [
        (
            None,
            [
                "DataFormats/Track/cache.tmp",
                ".claude/notes.py",
                "vendor/lib/lib.py",
                "DataFormats/Track/New.cc",
            ],
            {"DataFormats/Track/New.cc"},
        ),
        (
            ["*.py"],
            ["tools/gen.py", "tools/build.py", "new_top.py", ".claude/notes.py"],
            {"tools/build.py", "new_top.py"},
        ),
    ],
    ids=["gitignored-claude-nested", "star-py-prunes-subdirs"],
)
def test_scoped_untracked_reads_equal_the_full_crawl(
    tmp_path: Path,
    include_globs,  # noqa: ANN001
    keys: list[str],
    expected: set[str],
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "repo"
    _untracked_parity_repo(repo)

    def source() -> GitFilesSource:
        return GitFilesSource(
            repo="r",
            repo_root=repo,
            include_globs=include_globs,
            include_untracked=True,
        )

    full = _source_file_ids(source().run("run-full", mode="reconcile"))
    scoped, _ = _scoped_ids(source(), keys)

    expected_ids = {f"file:r:{path}" for path in expected}
    assert scoped == expected_ids
    # Parity: the scoped read is the full read restricted to the keys.
    assert scoped == {f"file:r:{key}" for key in keys} & full


def test_scoped_ignored_key_does_not_flip_the_next_full_run(
    tmp_path: Path,
) -> None:
    """A scoped run's cursor must not carry a path the full crawl never
    counts, or the next full run sees a dirty-set change and, under
    include_untracked, a stale id for a file it never emitted."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "repo"
    _untracked_parity_repo(repo)

    def source() -> GitFilesSource:
        return GitFilesSource(repo="r", repo_root=repo, include_untracked=True)

    first = source().run("run-1", mode="cursor")
    list(first.facts)
    keys = ["DataFormats/Track/cache.tmp", ".claude/notes.py", "vendor/lib/lib.py"]
    scoped_ids, scoped = _scoped_ids(source(), keys, cursor=first.next_cursor)
    assert scoped_ids == set()
    assert set((scoped.next_cursor or {}).get("dirty_paths") or ()) == set(
        (first.next_cursor or {}).get("dirty_paths") or ()
    )

    after = source().run("run-2", mode="cursor", cursor=scoped.next_cursor)
    facts = [f for f in after.facts if isinstance(f, NodeFact)]
    assert {f.node_id for f in facts if f.subtype == "source_file"} == set()
    assert not any(f.op == "R" for f in facts)


def test_scope_keys_are_normalized_like_git_paths(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "cmssw"
    _cmssw_like_repo(repo)
    src = GitFilesSource(
        repo="cmssw", repo_root=repo, include_globs=list(_CMSSW_INCLUDES),
    )

    ids, _ = _scoped_ids(
        src,
        [
            "./DataFormats/Track/Track.cc",
            "RecoTracker//Seed/Seed.cc",
            "../cmssw/DataFormats/Track/Track.cc",
        ],
    )

    assert ids == {
        "file:cmssw:DataFormats/Track/Track.cc",
        "file:cmssw:RecoTracker/Seed/Seed.cc",
    }
    # The key that leaves the repository is outside the source's paths.
    assert src.last_run_stats["scope_keys_outside_source_paths"] == 1


def test_scoped_run_with_only_escaping_keys_reads_nothing(tmp_path: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "cmssw"
    _cmssw_like_repo(repo)
    src = GitFilesSource(
        repo="cmssw", repo_root=repo, include_globs=list(_CMSSW_INCLUDES),
    )

    ids, _ = _scoped_ids(src, ["../elsewhere/x.cc", "."])

    assert ids == set()
    assert src.last_run_stats["scoped"] is True
    assert src.last_run_stats["scope_keys_outside_source_paths"] == 2


def _git_quiet(repo_root: Path, *args: str, check: bool = True) -> None:
    subprocess.run(
        [
            "git", "-C", str(repo_root),
            "-c", "user.email=test@example.com",
            "-c", "user.name=OKG Test",
            "-c", "protocol.file.allow=always",
            *args,
        ],
        check=check,
        capture_output=True,
    )


def test_scoped_run_on_a_conflicted_file_matches_the_full_dirty_set(
    tmp_path: Path,
) -> None:
    """A path mid-merge has index stages 1-3 and no stage 0. The scoped
    run must treat it as tracked, not fail on it."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "repo"
    _init_git_repo(repo)
    (repo / "a.cc").write_text("base\n", encoding="utf-8")
    (repo / "b.cc").write_text("b\n", encoding="utf-8")
    _commit_all(repo, "base")
    base_branch = _current_branch(repo)
    _git_quiet(repo, "checkout", "-q", "-b", "other")
    (repo / "a.cc").write_text("theirs\n", encoding="utf-8")
    _commit_all(repo, "theirs")
    _git_quiet(repo, "checkout", "-q", base_branch)
    (repo / "a.cc").write_text("ours\n", encoding="utf-8")
    _commit_all(repo, "ours")
    _git_quiet(repo, "merge", "-q", "other", check=False)
    status = subprocess.run(
        ["git", "-C", str(repo), "status", "--porcelain"],
        check=True, capture_output=True, text=True,
    ).stdout
    assert "UU a.cc" in status

    full = GitFilesSource(repo="r", repo_root=repo).run("full", mode="cursor")
    list(full.facts)
    scoped = GitFilesSource(repo="r", repo_root=repo).run(
        "scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["a.cc", "b.cc"]},
    )
    list(scoped.facts)

    assert "a.cc" in (full.next_cursor or {})["dirty_paths"]
    assert set((scoped.next_cursor or {})["dirty_paths"]) == set(
        (full.next_cursor or {})["dirty_paths"]
    )


def test_scoped_run_keeps_a_modified_submodule_in_the_dirty_set(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    inner = tmp_path / "inner"
    _init_git_repo(inner)
    (inner / "lib.cc").write_text("v1\n", encoding="utf-8")
    _commit_all(inner, "inner")
    repo = tmp_path / "repo"
    _init_git_repo(repo)
    (repo / "a.cc").write_text("a\n", encoding="utf-8")
    _commit_all(repo, "outer")
    _git_quiet(repo, "submodule", "add", "-q", str(inner), "sub")
    _git_quiet(repo, "commit", "-q", "-m", "add submodule")
    (repo / "sub" / "lib.cc").write_text("v2\n", encoding="utf-8")
    _git_quiet(repo / "sub", "commit", "-q", "-am", "advance")
    names = subprocess.run(
        ["git", "-C", str(repo), "diff", "--name-only"],
        check=True, capture_output=True, text=True,
    ).stdout.split()
    assert names == ["sub"]

    full = GitFilesSource(repo="r", repo_root=repo).run("full", mode="cursor")
    list(full.facts)
    scoped = GitFilesSource(repo="r", repo_root=repo).run(
        "scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["sub", "a.cc"]},
    )
    list(scoped.facts)

    assert "sub" in (full.next_cursor or {})["dirty_paths"]
    assert set((scoped.next_cursor or {})["dirty_paths"]) == set(
        (full.next_cursor or {})["dirty_paths"]
    )


def test_index_paths_rereads_an_index_changed_on_disk(tmp_path: Path) -> None:
    from okg.substrate.library.sources._git_helpers import in_proc_index_paths

    repo = tmp_path / "repo"
    _init_git_repo(repo)
    (repo / "a.cc").write_text("a\n", encoding="utf-8")
    _commit_all(repo, "a")
    assert in_proc_index_paths(repo, ["a.cc", "new.cc"]) == {"a.cc"}

    (repo / "new.cc").write_text("new\n", encoding="utf-8")
    _git_quiet(repo, "add", "new.cc")

    assert in_proc_index_paths(repo, ["a.cc", "new.cc"]) == {"a.cc", "new.cc"}


def test_scoped_run_skips_keys_inside_a_submodule(tmp_path: Path) -> None:
    """A tracked or new file inside a submodule is not part of the outer
    worktree: the full crawl never emits it, and the scoped run must
    neither emit it nor fail on it."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    inner = tmp_path / "inner"
    _init_git_repo(inner)
    (inner / "lib.cc").write_text("v1\n", encoding="utf-8")
    _commit_all(inner, "inner")
    repo = tmp_path / "repo"
    _init_git_repo(repo)
    (repo / "a.cc").write_text("a\n", encoding="utf-8")
    _commit_all(repo, "outer")
    _git_quiet(repo, "submodule", "add", "-q", str(inner), "sub")
    _git_quiet(repo, "commit", "-q", "-m", "add submodule")
    (repo / "sub" / "new.cc").write_text("new\n", encoding="utf-8")

    def source() -> GitFilesSource:
        return GitFilesSource(repo="r", repo_root=repo, include_untracked=True)

    full = source().run("full", mode="reconcile")
    full_ids = _source_file_ids(full)
    ids, scoped = _scoped_ids(source(), ["sub/lib.cc", "sub/new.cc", "a.cc"])

    assert not any(i.startswith("file:r:sub/") for i in full_ids)
    assert ids == {"file:r:a.cc"}


def test_full_and_scoped_runs_skip_a_deinitialized_submodule(tmp_path: Path) -> None:
    """After `git submodule deinit`, `sub/` has no `.git`, but its gitlink
    still bounds the outer worktree and `git check-ignore` refuses a path
    inside it (exit 128). Neither run may fail on, or emit, a file there."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    inner = tmp_path / "inner"
    _init_git_repo(inner)
    (inner / "lib.cc").write_text("v1\n", encoding="utf-8")
    _commit_all(inner, "inner")
    repo = tmp_path / "repo"
    _init_git_repo(repo)
    (repo / "a.cc").write_text("a\n", encoding="utf-8")
    _commit_all(repo, "outer")
    _git_quiet(repo, "submodule", "add", "-q", str(inner), "sub")
    _git_quiet(repo, "commit", "-q", "-m", "add submodule")
    _git_quiet(repo, "submodule", "deinit", "-q", "-f", "sub")
    assert not (repo / "sub" / ".git").exists()
    (repo / "sub" / "stray.cc").write_text("stray\n", encoding="utf-8")

    def source() -> GitFilesSource:
        return GitFilesSource(repo="r", repo_root=repo, include_untracked=True)

    full = source().run("full", mode="reconcile")
    full_ids = _source_file_ids(full)
    ids, _ = _scoped_ids(source(), ["sub/stray.cc", "sub/lib.cc", "a.cc"])

    assert full_ids == {"file:r:a.cc", "file:r:.gitmodules"}
    assert ids == {"file:r:a.cc"}


@pytest.mark.parametrize("dirname", [":x", ":(y"], ids=["colon", "colon-paren"])
def test_untracked_files_under_magic_looking_directory_names(
    tmp_path: Path,
    dirname: str,
) -> None:
    """`git check-ignore` reads each name as a pathspec: `:(y/a.cc` exited
    128 and failed both runs. Both now read the path literally."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    repo = tmp_path / "repo"
    _init_git_repo(repo)
    (repo / ".gitignore").write_text("*.log\n", encoding="utf-8")
    (repo / dirname).mkdir()
    (repo / dirname / "t.cc").write_text("t\n", encoding="utf-8")
    _commit_all(repo, "initial")
    (repo / dirname / "new.cc").write_text("new\n", encoding="utf-8")
    (repo / dirname / "debug.log").write_text("ignored\n", encoding="utf-8")

    def source() -> GitFilesSource:
        return GitFilesSource(repo="r", repo_root=repo, include_untracked=True)

    full_ids = _source_file_ids(source().run("full", mode="reconcile"))
    ids, _ = _scoped_ids(
        source(), [f"{dirname}/new.cc", f"{dirname}/debug.log"],
    )

    assert f"file:r:{dirname}/new.cc" in full_ids
    assert f"file:r:{dirname}/debug.log" not in full_ids
    assert ids == {f"file:r:{dirname}/new.cc"}
