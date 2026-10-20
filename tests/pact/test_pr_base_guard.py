"""Pre-merge PR-base guard: PRs must target the project's pr_policy.base (dev)."""
from __future__ import annotations

from pathlib import Path

from okg.substrate.pact.cli import _pr_base_guard, evaluate_pr_base
from okg.substrate.pact.project_settings import pr_base_policy


def test_evaluate_pr_base_blocks_wrong_base() -> None:
    blocker = evaluate_pr_base("main", "dev")
    assert blocker is not None
    assert blocker["code"] == "base_not_dev"
    assert blocker["base"] == "main"
    assert blocker["expected"] == "dev"


def test_evaluate_pr_base_allows_matching_base() -> None:
    assert evaluate_pr_base("dev", "dev") is None


def test_evaluate_pr_base_allows_when_nothing_to_judge() -> None:
    # No open PR -> no base_ref -> nothing to judge (fail-open).
    assert evaluate_pr_base(None, "dev") is None


def test_evaluate_pr_base_non_dev_expected_uses_generic_code() -> None:
    blocker = evaluate_pr_base("main", "release")
    assert blocker is not None
    assert blocker["code"] == "pr_base_mismatch"


def test_pr_base_policy_reads_project_yaml(tmp_path: Path) -> None:
    pact_dir = tmp_path / "pact"
    pact_dir.mkdir()
    (pact_dir / "project.yaml").write_text(
        "pr_policy:\n  base: trunk\n", encoding="utf-8",
    )
    assert pr_base_policy(tmp_path) == "trunk"


def test_pr_base_policy_defaults_to_dev_when_unset(tmp_path: Path) -> None:
    assert pr_base_policy(tmp_path) == "dev"


def test_pr_base_guard_is_fail_open_outside_a_git_repo(tmp_path: Path) -> None:
    # No git repo / no branch / no gh: the guard must never block on "can't judge".
    guard = _pr_base_guard(tmp_path)
    assert guard["ok"] is True
    assert guard["status"] in {
        "not_git_repo", "no_branch", "gh_unavailable", "gh_error", "no_pr",
    }


def test_repo_project_yaml_declares_dev_base() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    assert pr_base_policy(repo_root) == "dev"
