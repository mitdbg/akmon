"""Tests for `ExternalRepoConfig` YAML parsing + validation.

Covers spec Requirement: *External Repos Config File Schema*
and *Per-Repo Configuration Overrides* (replace not union, D2).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.errors import SchemaValidationFailed
from okg.substrate.library.sources.external_repos import ExternalRepoConfig


def _git_init(path: Path) -> None:
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(path)],
        check=True,
    )


def _write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


@pytest.fixture
def two_repos(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Create two ephemeral git repos and a config pointing at them."""
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    _git_init(repo_a)
    _git_init(repo_b)
    cfg = tmp_path / "external_repos.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: repo-a
    url: https://example.invalid/repo_a.git
    local_path: {repo_a}
    tracked_branch: main
  - name: repo-b
    url: https://example.invalid/repo_b.git
    local_path: {repo_b}
    description: second repo
""",
    )
    return cfg, repo_a, repo_b


def test_config_loads_two_repos(two_repos: tuple[Path, Path, Path]) -> None:
    cfg, repo_a, repo_b = two_repos
    parsed = ExternalRepoConfig.from_yaml(cfg)
    assert len(parsed.repos) == 2

    a = parsed.repos[0]
    assert a.name == "repo-a"
    assert a.url == "https://example.invalid/repo_a.git"
    assert a.local_path == repo_a
    assert a.tracked_branch == "main"
    assert a.description is None
    assert a.exclude_globs is None
    assert a.include_exts is None

    b = parsed.repos[1]
    assert b.name == "repo-b"
    assert b.tracked_branch == "main"  # default applied
    assert b.description == "second repo"
    assert b.to_attrs()["text"] == "repo-b: second repo"


def test_config_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.yaml"
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(missing)
    assert exc_info.value.code == "schema_validation_failed"
    assert "not found" in str(exc_info.value).lower()


def test_config_invalid_yaml_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    _write(bad, "repos: [name: x\n  url: y\n")  # malformed YAML
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(bad)
    assert "not valid yaml" in str(exc_info.value).lower()


def test_config_missing_repos_key_raises(tmp_path: Path) -> None:
    cfg = tmp_path / "no-repos.yaml"
    _write(cfg, "other: value\n")
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg)
    assert "missing `repos:` key" in str(exc_info.value)


def test_config_repos_must_be_list(tmp_path: Path) -> None:
    cfg = tmp_path / "scalar.yaml"
    _write(cfg, "repos: 42\n")
    with pytest.raises(SchemaValidationFailed):
        ExternalRepoConfig.from_yaml(cfg)


def test_entry_missing_name_raises(tmp_path: Path) -> None:
    cfg = tmp_path / "no-name.yaml"
    _write(
        cfg,
        f"""\
repos:
  - url: https://example.invalid/x.git
    local_path: {tmp_path}
""",
    )
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg, require_local_path=False)
    assert "missing `name`" in str(exc_info.value)


def test_entry_non_slug_name_rejected(tmp_path: Path) -> None:
    cfg = tmp_path / "bad-name.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: Not_A_Slug
    url: https://example.invalid/x.git
    local_path: {tmp_path}
""",
    )
    with pytest.raises(SchemaValidationFailed):
        ExternalRepoConfig.from_yaml(cfg, require_local_path=False)


def test_entry_duplicate_name_rejected(tmp_path: Path) -> None:
    cfg = tmp_path / "dupe.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: same
    url: https://example.invalid/a.git
    local_path: {tmp_path}
  - name: same
    url: https://example.invalid/b.git
    local_path: {tmp_path}
""",
    )
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg, require_local_path=False)
    assert "duplicate" in str(exc_info.value).lower()
    assert exc_info.value.data["duplicate_name"] == "same"


def test_entry_missing_local_path_rejected(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    cfg = tmp_path / "missing-path.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: gone
    url: https://example.invalid/g.git
    local_path: {missing}
""",
    )
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg)
    assert "does not exist" in str(exc_info.value)


def test_entry_local_path_not_git_repo_rejected(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    (plain / "README").write_text("not a repo")
    cfg = tmp_path / "not-git.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: plain
    url: https://example.invalid/p.git
    local_path: {plain}
""",
    )
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg)
    assert "not a git checkout" in str(exc_info.value)


def test_per_repo_overrides_replace_defaults(tmp_path: Path) -> None:
    """D2: per-repo `exclude_globs` REPLACE defaults (not union)."""
    repo = tmp_path / "r"
    _git_init(repo)
    cfg = tmp_path / "overrides.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: with-override
    url: https://example.invalid/o.git
    local_path: {repo}
    exclude_globs:
      - "data/**"
  - name: with-defaults
    url: https://example.invalid/d.git
    local_path: {repo}
""",
    )
    parsed = ExternalRepoConfig.from_yaml(
        cfg,
        default_exclude_globs=("*.pyc", "__pycache__/**"),
    )
    over = parsed.repos[0]
    default = parsed.repos[1]
    # Override replaces the defaults — exactly the per-repo list.
    assert over.exclude_globs == ("data/**",)
    # Absent override means use defaults.
    assert default.exclude_globs == ("*.pyc", "__pycache__/**")


def test_per_repo_include_exts_overrides(tmp_path: Path) -> None:
    repo = tmp_path / "r"
    _git_init(repo)
    cfg = tmp_path / "exts.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: py-only
    url: https://example.invalid/p.git
    local_path: {repo}
    include_exts:
      - .py
""",
    )
    parsed = ExternalRepoConfig.from_yaml(
        cfg,
        default_include_exts=(".py", ".md", ".rst"),
    )
    assert parsed.repos[0].include_exts == (".py",)
