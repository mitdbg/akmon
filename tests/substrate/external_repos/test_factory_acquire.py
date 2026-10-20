"""External-repos managed acquisition defers the load-time checkout hard-fail.

An entry with `manage_checkout: true` no longer requires a pre-existing local
checkout at config-load time (acquisition is deferred to the git-files source's
ensure_checkout at ingest); an unmanaged entry keeps the loud require-a-clone
validation. No clone happens during load.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SchemaValidationFailed
from okg.substrate.library.sources.external_repos import ExternalRepoConfig


def _write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def test_managed_entry_defers_missing_checkout(tmp_path: Path) -> None:
    missing = tmp_path / "not_cloned_yet"
    cfg = tmp_path / "external_repos.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: repo-a
    url: https://example.invalid/repo_a.git
    local_path: {missing}
    manage_checkout: true
""",
    )
    # Does NOT raise even though local_path is absent + not a git checkout.
    parsed = ExternalRepoConfig.from_yaml(cfg)
    assert len(parsed.repos) == 1
    assert parsed.repos[0].manage_checkout is True
    # Load performed no clone — the path is still absent.
    assert not missing.exists()


def test_unmanaged_entry_missing_checkout_hard_fails(tmp_path: Path) -> None:
    missing = tmp_path / "not_cloned_yet"
    cfg = tmp_path / "external_repos.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: repo-a
    url: https://example.invalid/repo_a.git
    local_path: {missing}
""",
    )
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg)
    assert "does not exist" in str(exc_info.value).lower()


def test_unmanaged_non_git_dir_hard_fails(tmp_path: Path) -> None:
    plain = tmp_path / "plain_dir"
    plain.mkdir()
    cfg = tmp_path / "external_repos.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: repo-a
    url: https://example.invalid/repo_a.git
    local_path: {plain}
""",
    )
    with pytest.raises(SchemaValidationFailed) as exc_info:
        ExternalRepoConfig.from_yaml(cfg)
    assert "not a git checkout" in str(exc_info.value).lower()


def test_managed_non_git_dir_also_defers(tmp_path: Path) -> None:
    plain = tmp_path / "plain_dir"
    plain.mkdir()
    cfg = tmp_path / "external_repos.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: repo-a
    url: https://example.invalid/repo_a.git
    local_path: {plain}
    manage_checkout: true
""",
    )
    parsed = ExternalRepoConfig.from_yaml(cfg)  # no raise
    assert parsed.repos[0].manage_checkout is True


def _failing_managed_factory(tmp_path: Path):
    from okg.substrate.library.sources.external_repos import ExternalRepoFactory

    missing_remote = tmp_path / "no-such.git"  # unreachable -> clone fails
    dest = tmp_path / "checkout"
    cfg = tmp_path / "external_repos.yaml"
    _write(
        cfg,
        f"""\
repos:
  - name: repo-a
    url: {missing_remote}
    local_path: {dest}
    manage_checkout: true
""",
    )
    return ExternalRepoFactory(repos_config_path=str(cfg)), dest


def test_managed_clone_failure_surfaces_health(tmp_path: Path) -> None:
    """A managed external_repos entry whose clone fails must not be silent: the
    factory's ensure_checkout() (the runner's pre-probe seam) reports
    acquire_failed."""
    src, dest = _failing_managed_factory(tmp_path)
    health = src.ensure_checkout()
    assert health is not None
    assert health.status == "acquire_failed"
    assert "repo-a" in (health.reason or "")
    assert not (dest / ".git").exists()


def test_managed_clone_failure_not_silent_on_second_run(tmp_path: Path) -> None:
    """Regression for the review P1: a repeatedly-failing managed clone must keep
    surfacing acquire_failed on every run — the factory's ensure_checkout() runs
    before the probe, so it retries and reports each time (never goes silent once
    the config-only probe token stops changing)."""
    src, _dest = _failing_managed_factory(tmp_path)
    first = src.ensure_checkout()
    second = src.ensure_checkout()
    assert first is not None and first.status == "acquire_failed"
    assert second is not None and second.status == "acquire_failed"
