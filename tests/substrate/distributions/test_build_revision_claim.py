"""setup.py records the commit a build with no .git names, and nothing more.

The runtime image builds okg's wheel from a context without .git, so setup.py
cannot read the commit. The Dockerfile passes VCS_REF as
OKG_BUILD_SOURCE_REVISION. R4's release workflow will also pass the tree and
clean/dirty state it computes from its checkout (OKG_BUILD_SOURCE_TREE,
OKG_BUILD_SOURCE_DIRTY); until then CI images record those as unknown. The
wheel claims what was supplied and records the rest as unknown (null), never
as clean.
"""

from __future__ import annotations

import json
import runpy
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import setuptools

REPO_ROOT = Path(__file__).resolve().parents[3]
COMMIT = "0123456789abcdef0123456789abcdef01234567"
TREE = "89abcdef0123456789abcdef0123456789abcdef"
_BUILD_ENV = ("OKG_BUILD_SOURCE_REVISION", "OKG_BUILD_SOURCE_TREE", "OKG_BUILD_SOURCE_DIRTY")


@pytest.fixture(autouse=True)
def _no_inherited_build_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _BUILD_ENV:
        monkeypatch.delenv(name, raising=False)


def _load_setup(directory: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Run a copy of setup.py from ``directory`` with setup() stubbed out."""
    directory.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPO_ROOT / "setup.py", directory / "setup.py")
    monkeypatch.setattr(setuptools, "setup", lambda **_: None)
    return runpy.run_path(str(directory / "setup.py"))


def test_a_build_outside_git_claims_the_supplied_commit_and_no_more(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", COMMIT)
    setup = _load_setup(tmp_path / "context", monkeypatch)
    assert setup["_build_info"]() == {
        "source_revision": COMMIT,
        "source_tree": None,
        "source_dirty": None,
    }


@pytest.mark.parametrize(("dirty", "recorded"), [("false", False), ("true", True)])
def test_a_build_outside_git_records_a_supplied_tree_and_dirty_state(
    dirty: str, recorded: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", COMMIT)
    monkeypatch.setenv("OKG_BUILD_SOURCE_TREE", TREE)
    monkeypatch.setenv("OKG_BUILD_SOURCE_DIRTY", dirty)
    setup = _load_setup(tmp_path / "context", monkeypatch)
    assert setup["_build_info"]() == {
        "source_revision": COMMIT,
        "source_tree": TREE,
        "source_dirty": recorded,
    }


@pytest.mark.parametrize(
    ("env", "value", "message"),
    [
        ("OKG_BUILD_SOURCE_TREE", "abc123", "full lowercase Git object id"),
        ("OKG_BUILD_SOURCE_DIRTY", "no", "'true', 'false' or 'unknown'"),
        ("OKG_BUILD_SOURCE_DIRTY", "False", "'true', 'false' or 'unknown'"),
    ],
)
def test_a_malformed_tree_or_dirty_state_stops_the_build(
    env: str, value: str, message: str,
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", COMMIT)
    monkeypatch.setenv(env, value)
    setup = _load_setup(tmp_path / "context", monkeypatch)
    with pytest.raises(ValueError, match=message):
        setup["_build_info"]()


def test_a_tree_or_dirty_state_without_a_commit_stops_the_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OKG_BUILD_SOURCE_REVISION", raising=False)
    monkeypatch.setenv("OKG_BUILD_SOURCE_DIRTY", "false")
    setup = _load_setup(tmp_path / "context", monkeypatch)
    with pytest.raises(ValueError, match="set OKG_BUILD_SOURCE_REVISION beside them"):
        setup["_build_info"]()


@pytest.mark.parametrize("value", [None, "", "unknown", "  unknown  "])
def test_no_supplied_commit_is_an_unknown_build(
    value: str | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    if value is None:
        monkeypatch.delenv("OKG_BUILD_SOURCE_REVISION", raising=False)
    else:
        monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", value)
    setup = _load_setup(tmp_path / "context", monkeypatch)
    assert setup["_build_info"]() == {
        "source_revision": None,
        "source_tree": None,
        "source_dirty": None,
    }


@pytest.mark.parametrize("value", ["0123456", COMMIT.upper(), COMMIT + "0", "HEAD"])
def test_a_malformed_commit_stops_the_build(
    value: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", value)
    setup = _load_setup(tmp_path / "context", monkeypatch)
    with pytest.raises(ValueError, match="full lowercase Git object id"):
        setup["_build_info"]()


def test_the_sdist_record_wins_and_must_agree_with_a_supplied_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    carried = {"source_revision": COMMIT, "source_tree": "f" * 40, "source_dirty": False}
    context = tmp_path / "sdist"
    context.mkdir()
    (context / "okg-build-info.json").write_text(json.dumps(carried))
    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", COMMIT)
    setup = _load_setup(context, monkeypatch)
    assert setup["_build_info"]() == carried

    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", "a" * 40)
    with pytest.raises(ValueError, match="disagrees with the sdist"):
        setup["_build_info"]()


def test_inside_git_head_wins_and_a_different_supplied_commit_stops_the_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    if shutil.which("git") is None:
        pytest.fail("git is required to build the checkout this test inspects")
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    env = {
        "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    shutil.copy2(REPO_ROOT / "setup.py", checkout / "setup.py")
    subprocess.run(["git", "-C", str(checkout), "add", "setup.py"], check=True)
    subprocess.run(["git", "-C", str(checkout), "-c", "commit.gpgsign=false", "commit", "-qm", "c"], check=True)
    head = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", head)
    setup = _load_setup(checkout, monkeypatch)
    info = setup["_build_info"]()
    assert info["source_revision"] == head
    assert info["source_dirty"] is False

    tree = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD^{tree}"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    monkeypatch.setenv("OKG_BUILD_SOURCE_TREE", tree)
    monkeypatch.setenv("OKG_BUILD_SOURCE_DIRTY", "false")
    assert setup["_build_info"]() == {
        "source_revision": head, "source_tree": tree, "source_dirty": False,
    }

    monkeypatch.setenv("OKG_BUILD_SOURCE_DIRTY", "true")
    with pytest.raises(ValueError, match="OKG_BUILD_SOURCE_DIRTY=True disagrees"):
        setup["_build_info"]()
    monkeypatch.setenv("OKG_BUILD_SOURCE_DIRTY", "false")
    monkeypatch.setenv("OKG_BUILD_SOURCE_TREE", TREE)
    with pytest.raises(ValueError, match="OKG_BUILD_SOURCE_TREE=.* disagrees"):
        setup["_build_info"]()

    monkeypatch.setenv("OKG_BUILD_SOURCE_REVISION", COMMIT)
    monkeypatch.delenv("OKG_BUILD_SOURCE_TREE")
    with pytest.raises(ValueError, match="disagrees with this checkout"):
        setup["_build_info"]()
