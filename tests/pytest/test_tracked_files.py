"""`tracked_files` lists what git tracks, never what merely sits on disk.

The scans that use it went red when a parallel test's scratch file was listed
by a disk walk and deleted before the scan read it (dev Tests run
36370525406). These cases pin the property that closes that race: an
untracked file is never in the universe, whether or not it still exists.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.pytest.tracked_files import tracked_files


def _git(repo: Path, *args: str) -> None:
    subprocess.run(  # noqa: S603
        ["git", "-C", str(repo), *args],  # noqa: S607
        check=True,
        capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "pkg" / "sub").mkdir(parents=True)
    (root / "pkg" / "a.py").write_text("a = 1\n", encoding="utf-8")
    (root / "pkg" / "sub" / "test_b.py").write_text("b = 1\n", encoding="utf-8")
    (root / "pkg" / "sub" / "notes.md").write_text("x\n", encoding="utf-8")
    (root / "pkg" / "gone.py").write_text("g = 1\n", encoding="utf-8")
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(
        root,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "commit",
        "-q",
        "-m",
        "seed",
    )
    return root


def test_untracked_and_vanished_files_are_outside_the_universe(repo: Path) -> None:
    # The fast-linux-3 shape: a scratch file another test creates in the tree.
    scratch = repo / "pkg" / "zz_probe" / "test_probe.py"
    scratch.parent.mkdir()
    scratch.write_text("def test_x():\n    pass\n", encoding="utf-8")
    # A tracked file deleted from disk but not yet staged.
    (repo / "pkg" / "gone.py").unlink()

    listed = tracked_files(repo / "pkg", "*.py")

    assert listed == [repo / "pkg" / "a.py", repo / "pkg" / "sub" / "test_b.py"]
    # A disk walk sees the scratch file; that difference is the whole fix.
    assert scratch in set((repo / "pkg").rglob("*.py"))
    # Every listed path is readable: nothing listed can vanish from under it.
    for path in listed:
        path.read_text(encoding="utf-8")


def test_the_pattern_matches_the_file_name_like_rglob(repo: Path) -> None:
    assert tracked_files(repo / "pkg", "test_*.py") == [
        repo / "pkg" / "sub" / "test_b.py"
    ]
    assert tracked_files(repo / "pkg" / "sub") == [
        repo / "pkg" / "sub" / "notes.md",
        repo / "pkg" / "sub" / "test_b.py",
    ]


def test_a_root_outside_a_checkout_is_an_error_not_an_empty_scan(
    tmp_path: Path,
) -> None:
    loose = tmp_path / "loose"
    loose.mkdir()
    (loose / "a.py").write_text("a = 1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="git"):
        tracked_files(loose, "*.py")
