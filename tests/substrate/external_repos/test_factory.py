"""Tests for `ExternalRepoFactory.run()` against ephemeral repos.

Covers spec Requirements: *Multi-Repo Factory Source Class*,
*ExternalRepo Subtype Identity and Required Attributes*,
*Source File To External Repo Narrowing*, *External Repo
Retraction On Config Removal*.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterable

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.external_repos import ExternalRepoFactory


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _init_repo(repo: Path, *, files: dict[str, str]) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(repo)],
        check=True,
    )
    _git(repo, "config", "user.email", "fixture@example.invalid")
    _git(repo, "config", "user.name", "fixture")
    for rel, content in files.items():
        p = repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    _git(repo, "add", "-A")
    _git(repo, "commit", "--quiet", "-m", "seed")


def _write_config(
    cfg: Path, *, repos: Iterable[tuple[str, Path, dict[str, object]]]
) -> None:
    lines = ["repos:"]
    for name, local_path, extras in repos:
        lines.append(f"  - name: {name}")
        lines.append(f"    url: https://example.invalid/{name}.git")
        lines.append(f"    local_path: {local_path}")
        for key, value in extras.items():
            if isinstance(value, list):
                lines.append(f"    {key}:")
                for item in value:
                    lines.append(f"      - {item!r}")
            else:
                lines.append(f"    {key}: {value}")
    cfg.write_text("\n".join(lines) + "\n")


def _node_facts(facts: Iterable[object]) -> list[NodeFact]:
    return [f for f in facts if isinstance(f, NodeFact)]


def _edge_facts(facts: Iterable[object]) -> list[EdgeFact]:
    return [f for f in facts if isinstance(f, EdgeFact)]


@pytest.fixture
def two_repos_with_files(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Build two repos with distinct files and a config pointing
    at both."""
    repo_a = tmp_path / "a"
    repo_b = tmp_path / "b"
    _init_repo(repo_a, files={
        "src/alpha.py": "def alpha():\n    return 1\n",
        "README.md": "alpha repo\n",
    })
    _init_repo(repo_b, files={
        "src/beta.py": "def beta():\n    return 2\n",
        "data/skip.csv": "x\n",
    })
    cfg = tmp_path / "config.yaml"
    _write_config(
        cfg,
        repos=[
            ("alpha", repo_a, {}),
            ("beta", repo_b, {}),
        ],
    )
    return cfg, repo_a, repo_b


def test_factory_emits_one_external_repo_per_declared(
    two_repos_with_files: tuple[Path, Path, Path],
) -> None:
    cfg, _, _ = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    run = factory.run("run-1", mode="reconcile")

    facts = list(run.facts)
    external_repo_nodes = [
        f for f in _node_facts(facts) if f.subtype == "external_repo"
    ]
    assert len(external_repo_nodes) == 2
    names = sorted(n.attrs["name"] for n in external_repo_nodes)
    assert names == ["alpha", "beta"]


def test_external_repo_identity_keyed_on_name(
    two_repos_with_files: tuple[Path, Path, Path],
) -> None:
    cfg, _, _ = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    run = factory.run("run-1", mode="reconcile")

    external_repo_nodes = [
        f for f in _node_facts(run.facts) if f.subtype == "external_repo"
    ]
    ids = {n.node_id for n in external_repo_nodes}
    assert ids == {"external_repo:alpha", "external_repo:beta"}


def test_external_repo_required_attrs_present(
    two_repos_with_files: tuple[Path, Path, Path],
) -> None:
    cfg, _, _ = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    run = factory.run("run-1", mode="reconcile")

    repos = [
        f for f in _node_facts(run.facts) if f.subtype == "external_repo"
    ]
    for repo in repos:
        for attr in ("name", "url", "local_path", "tracked_branch"):
            assert attr in repo.attrs, f"{attr} missing on {repo.node_id}"


def test_factory_emits_member_of_edges(
    two_repos_with_files: tuple[Path, Path, Path],
) -> None:
    cfg, _, _ = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    run = factory.run("run-1", mode="reconcile")

    edges = _edge_facts(run.facts)
    # Sub-sources also emit member_of edges (file → branch); filter
    # to only those pointing at external_repo nodes.
    nodes = _node_facts(run.facts)
    external_repo_ids = {
        n.node_id for n in nodes if n.subtype == "external_repo"
    }
    factory_edges = [
        e
        for e in edges
        if e.edge_type == "member_of" and e.dst in external_repo_ids
    ]
    assert factory_edges, (
        "expected at least one source_file -> external_repo edge"
    )

    source_file_ids = {
        n.node_id for n in nodes if n.subtype == "source_file"
    }
    for edge in factory_edges:
        assert edge.src in source_file_ids
        assert edge.dst in external_repo_ids
        # Provenance attr names which repo claimed this edge.
        assert "repo_name" in edge.attrs


def test_factory_isolates_files_per_repo(
    two_repos_with_files: tuple[Path, Path, Path],
) -> None:
    """SourceFiles emitted for repo `alpha` carry `repo=alpha` in
    their identity (`file:<repo>:<path>`) and vice versa."""
    cfg, _, _ = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    run = factory.run("run-1", mode="reconcile")

    edges = [e for e in _edge_facts(run.facts) if e.edge_type == "member_of"]
    by_repo: dict[str, set[str]] = {"alpha": set(), "beta": set()}
    for edge in edges:
        repo_name = edge.dst.removeprefix("external_repo:")
        by_repo.setdefault(repo_name, set()).add(edge.src)

    assert by_repo["alpha"], "alpha should have source files"
    assert by_repo["beta"], "beta should have source files"
    # Each source file id encodes its repo: file:<repo>:<path>.
    for sf_id in by_repo["alpha"]:
        assert sf_id.startswith("file:alpha:")
    for sf_id in by_repo["beta"]:
        assert sf_id.startswith("file:beta:")


def test_factory_no_repos_is_clean_empty_run(tmp_path: Path) -> None:
    cfg = tmp_path / "empty.yaml"
    cfg.write_text("repos: []\n")
    factory = ExternalRepoFactory(
        repos_config_path=str(cfg), require_local_path=False
    )
    run = factory.run("run-1", mode="reconcile")
    assert _node_facts(run.facts) == []
    assert _edge_facts(run.facts) == []
    assert run.completed_scope is True


def test_factory_retraction_via_record_set(
    two_repos_with_files: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    """When a repo is removed from the config, the next run's
    record_set excludes that repo. Substrate succession retires the
    prior facts; the factory just emits the new (smaller) record_set."""
    cfg, repo_a, repo_b = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    first = factory.run("run-1", mode="reconcile")
    assert first.record_set is not None
    assert {
        "external_repo:alpha",
        "external_repo:beta",
    } <= set(first.record_set.records.keys())

    # Operator removes `beta` from the config.
    _write_config(
        cfg,
        repos=[
            ("alpha", repo_a, {}),
        ],
    )
    second = factory.run("run-2", mode="reconcile", cursor=first.next_cursor)

    assert second.record_set is not None
    keys = set(second.record_set.records.keys())
    assert "external_repo:alpha" in keys
    assert "external_repo:beta" not in keys


def test_factory_isolates_per_repo_non_git_path(tmp_path: Path) -> None:
    """D1 part 1: a non-git `local_path` does NOT abort the run.

    With `require_local_path=False` the factory passes config
    validation, then the per-repo sub-sources for the non-git path
    return zero facts (they log warnings and no-op rather than
    raising). The healthy repo emits source files normally.
    """
    good = tmp_path / "good"
    _init_repo(good, files={"main.py": "print('ok')\n"})
    bad = tmp_path / "bad"
    bad.mkdir()  # exists but no .git
    cfg = tmp_path / "mixed.yaml"
    _write_config(
        cfg,
        repos=[
            ("good", good, {}),
            ("bad", bad, {}),
        ],
    )
    factory = ExternalRepoFactory(
        repos_config_path=str(cfg), require_local_path=False
    )
    run = factory.run("run-1", mode="reconcile")
    repos = [
        f for f in _node_facts(run.facts) if f.subtype == "external_repo"
    ]
    assert {n.attrs["name"] for n in repos} == {"good", "bad"}

    nodes = _node_facts(run.facts)
    external_repo_ids = {
        n.node_id for n in nodes if n.subtype == "external_repo"
    }
    factory_edges = [
        e for e in _edge_facts(run.facts)
        if e.edge_type == "member_of" and e.dst in external_repo_ids
    ]
    good_edges = [e for e in factory_edges if e.dst == "external_repo:good"]
    bad_edges = [e for e in factory_edges if e.dst == "external_repo:bad"]
    assert good_edges, "healthy repo emits member_of edges"
    assert not bad_edges, "non-git repo emits zero member_of edges"


def test_factory_isolates_per_repo_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D1 part 2: a sub-source that raises is caught; other repos
    continue. The failing repo's name is recorded in
    `next_cursor.errors`."""
    repo_a = tmp_path / "a"
    repo_b = tmp_path / "b"
    _init_repo(repo_a, files={"x.py": "x = 1\n"})
    _init_repo(repo_b, files={"y.py": "y = 2\n"})
    cfg = tmp_path / "cfg.yaml"
    _write_config(
        cfg,
        repos=[("a", repo_a, {}), ("b", repo_b, {})],
    )
    factory = ExternalRepoFactory(repos_config_path=str(cfg))

    # Patch _run_sub_sources to raise for repo `b` only.
    original = factory._run_sub_sources

    def _patched(self_repo, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self_repo.name == "b":
            raise RuntimeError("simulated sub-source failure for repo b")
        return original(self_repo, *args, **kwargs)

    monkeypatch.setattr(factory, "_run_sub_sources", _patched)

    run = factory.run("run-1", mode="reconcile")
    repos = [
        f for f in _node_facts(run.facts) if f.subtype == "external_repo"
    ]
    # Both ExternalRepo nodes still emit — the failure is AFTER
    # the parent node was appended.
    assert {n.attrs["name"] for n in repos} == {"a", "b"}
    # The healthy repo emitted source files and edges.
    sf_for_a = [
        f for f in _node_facts(run.facts)
        if f.subtype == "source_file" and f.node_id.startswith("file:a:")
    ]
    assert sf_for_a, "repo a's source files should be emitted"
    # The failing repo's error is recorded in next_cursor.
    errors = (run.next_cursor or {}).get("errors", {})
    assert "b" in errors
    assert "simulated sub-source failure" in errors["b"]


def test_factory_completed_scope_only_in_reconcile_modes(
    two_repos_with_files: tuple[Path, Path, Path],
) -> None:
    cfg, _, _ = two_repos_with_files
    factory = ExternalRepoFactory(repos_config_path=str(cfg))

    cursor_run = factory.run("run-1", mode="cursor")
    assert cursor_run.completed_scope is False
    assert cursor_run.record_set is None

    reconcile_run = factory.run("run-1", mode="reconcile")
    assert reconcile_run.completed_scope is True
    assert reconcile_run.record_set is not None


def test_factory_per_repo_exclude_globs_applied(tmp_path: Path) -> None:
    """D2: per-repo override replaces factory default."""
    repo = tmp_path / "r"
    _init_repo(repo, files={
        "keep.py": "x = 1\n",
        "data/skip.txt": "skip-me\n",
    })
    cfg = tmp_path / "with-override.yaml"
    _write_config(
        cfg,
        repos=[
            (
                "r",
                repo,
                {"exclude_globs": ["data/**"]},
            )
        ],
    )
    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    run = factory.run("run-1", mode="reconcile")
    sf_paths = {
        f.attrs.get("path")
        for f in _node_facts(run.facts)
        if f.subtype == "source_file"
    }
    assert "keep.py" in sf_paths
    assert "data/skip.txt" not in sf_paths


def test_factory_composes_file_ownership_explicitly_across_runs(
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest.runner import SourceRunner

    repo = tmp_path / "r"
    _init_repo(repo, files={
        "keep.py": "x = 1\n",
        "gone.py": "removed = True\n",
    })
    _git(repo, "rm", "gone.py")
    _git(repo, "commit", "--quiet", "-m", "remove historical file")
    cfg = tmp_path / "repos.yaml"
    _write_config(cfg, repos=[("r", repo, {})])

    factory = ExternalRepoFactory(repos_config_path=str(cfg))
    first = factory.run("run-1", mode="reconcile")
    second = factory.run(
        "run-2",
        mode="reconcile",
        cursor=first.next_cursor,
    )

    keep_id = "file:r:keep.py"
    gone_id = "file:r:gone.py"
    for run in (first, second):
        facts = _node_facts(run.facts)
        assert {
            fact.ownership_mode for fact in facts if fact.node_id == keep_id
        } == {"authoritative"}
        assert {
            fact.ownership_mode for fact in facts if fact.node_id == gone_id
        } == {"substitute"}

        assert run.record_set is not None
        record_nodes = {
            node.node_id: node
            for record in run.record_set.records.values()
            for node in (
                *([record.primary] if record.primary is not None else []),
                *record.descendants,
            )
        }
        assert record_nodes[keep_id].ownership_mode == "authoritative"
        assert record_nodes[gone_id].ownership_mode == "substitute"

    # Reproduce the verified second-run admission check without a database:
    # every emitted node must declare the same explicit mode as its record set.
    runner = SourceRunner("postgresql://unused", source_name="external_repos")
    assert second.record_set is not None
    covered_nodes, _ = runner._record_set_identities(second.record_set)
    covered_modes = runner._record_set_node_ownership_modes(second.record_set)
    for fact in _node_facts(second.facts):
        runner._assert_record_set_covers_emitted_fact(
            fact,
            covered_nodes=covered_nodes,
            covered_node_ownership_modes=covered_modes,
            covered_edges=set(),
        )
