"""Profiling writes nothing into the folder okg runs from (issue #3480).

The default profile root was `./profiles`, so `okg install`, `okg deployment
add-source` and every publish left `profiles/<deployment>/_live/*.json` and
`profiles/.gitignore` in the operator's current folder -- their home folder,
in the macOS release test. The root is now okg's user data folder.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from okg.substrate.profiling import artifacts
from okg.substrate.profiling.context import profiling_context
from okg.substrate.profiling.policy import ProfilingPolicy


@pytest.fixture
def cwd_and_home(tmp_path, monkeypatch):
    cwd = tmp_path / "cwd"
    home = tmp_path / "user-home"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.delenv("OKG_PROFILE_DIR", raising=False)
    monkeypatch.setenv("OKG_USER_HOME", str(home))
    return cwd, home


def test_a_live_snapshot_lands_in_the_user_data_folder(cwd_and_home) -> None:
    cwd, home = cwd_and_home
    deployment = f"nif-{os.getpid()}-live"
    with profiling_context(
        deployment=deployment,
        target_kind="ingest",
        command="test",
        policy=ProfilingPolicy(),
    ):
        pass

    assert list(cwd.iterdir()) == [], "profiling wrote into the working folder"
    live = home / "profiles" / deployment / "_live" / f"ingest.{os.getpid()}.json"
    assert live.is_file()
    assert artifacts.profile_root() == (home / "profiles").resolve()


def test_okg_profile_dir_still_moves_the_root(cwd_and_home, tmp_path, monkeypatch) -> None:
    cwd, home = cwd_and_home
    chosen = tmp_path / "chosen"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(chosen))
    assert artifacts.ensure_profile_root() == chosen.resolve()
    assert chosen.is_dir()
    # A root the operator chose gets no .gitignore written into it.
    assert not (chosen / ".gitignore").exists()
    assert list(cwd.iterdir()) == []
    assert not (home / "profiles").exists()


def test_the_default_root_is_never_relative(cwd_and_home) -> None:
    cwd, _home = cwd_and_home
    root = artifacts.profile_root()
    assert root.is_absolute()
    assert Path(cwd) not in (root, *root.parents)


def test_the_bench_reads_snapshots_from_the_profile_root(cwd_and_home) -> None:
    """The per-source bench read `<checkout>/profiles/<dep>/_live`, which
    profiling no longer writes; it must read the profile root."""
    pytest.importorskip("jinja2")
    from okg.bench.per_source import single_commit_breakdown as bench

    cwd, home = cwd_and_home
    deployment = f"nif-{os.getpid()}-bench"
    assert bench.read_profile_snapshot(deployment) == {}
    stale = cwd / "profiles" / deployment / "_live"
    stale.mkdir(parents=True)
    (stale / "source_sync.1.json").write_text('{"stage_p95_ms": {"old": 1.0}}')
    live = home / "profiles" / deployment / "_live"
    live.mkdir(parents=True)
    (live / "source_sync.1.json").write_text('{"stage_p95_ms": {"fetch": 2.5}}')
    assert bench.read_profile_snapshot(deployment) == {"fetch": 2.5}
