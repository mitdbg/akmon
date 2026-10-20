from __future__ import annotations

import subprocess
from pathlib import Path

from okg.bench.differential_replay_fixture import (
    SYNTHETIC_REPLAY_EXPERIMENT_ID,
    SYNTHETIC_REPO_NAME,
    changed_paths,
    create_synthetic_differential_repo,
)


def test_synthetic_differential_repo_has_authored_replay_commits(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)

    assert fixture.repo_root.name == SYNTHETIC_REPO_NAME
    assert SYNTHETIC_REPLAY_EXPERIMENT_ID == "synthetic-differential-e2e"
    assert len(fixture.commits) == 4
    assert [commit.label for commit in fixture.commits] == [
        "doc_only_symbol_mention",
        "code_symbol_change",
        "rename_and_delete",
        "package_manifest_change",
    ]
    assert [commit.subject for commit in fixture.commits] == [
        "doc-only symbol mention",
        "add weighted total symbol",
        "rename docs and remove legacy module",
        "add package dependency",
    ]

    rev_list = _git(
        fixture.repo_root,
        "rev-list",
        "--reverse",
        "--topo-order",
        "HEAD",
    ).splitlines()
    assert rev_list == [fixture.baseline_sha, *fixture.replay_commit_shas]


def test_synthetic_differential_repo_reports_expected_changed_paths(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    previous = fixture.baseline_sha

    for commit in fixture.commits:
        assert changed_paths(fixture.repo_root, previous, commit.sha) == (
            commit.expected_changed_paths
        )
        previous = commit.sha


def test_synthetic_differential_repo_final_state_and_commits_file(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    commits_file = fixture.write_commits_file(tmp_path / "commits.txt")

    assert commits_file.read_text().splitlines() == list(
        fixture.replay_commit_shas
    )
    assert (fixture.repo_root / "README.md").read_text().count(
        "WeightedTotal"
    ) == 1
    assert "class WeightedTotal" in (
        fixture.repo_root / "src/toy/core.py"
    ).read_text()
    assert (fixture.repo_root / "docs/design.md").is_file()
    assert not (fixture.repo_root / "docs/notes.md").exists()
    assert not (fixture.repo_root / "src/toy/legacy.py").exists()
    assert "typing-extensions>=4.0" in (
        fixture.repo_root / "pyproject.toml"
    ).read_text()


def test_synthetic_differential_repo_stage_tags_name_pipeline_surfaces(
    tmp_path: Path,
) -> None:
    fixture = create_synthetic_differential_repo(tmp_path)
    tags_by_label = {
        commit.label: set(commit.expected_stage_tags)
        for commit in fixture.commits
    }

    assert {
        "git-files",
        "doc-corpus",
        "code-reference-linker",
        "native-view",
    } <= tags_by_label["doc_only_symbol_mention"]
    assert {
        "code-structure",
        "interfaces",
        "consistency-gate",
    } <= tags_by_label["code_symbol_change"]
    assert {"retraction", "derived-relation"} <= tags_by_label[
        "rename_and_delete"
    ]
    assert {"package-manifests", "dependency-relation"} <= tags_by_label[
        "package_manifest_change"
    ]


def _git(repo_root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=repo_root,
        text=True,
    ).strip()
