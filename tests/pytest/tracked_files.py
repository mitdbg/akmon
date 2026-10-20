"""Git-tracked files for tests that scan the repository's own source.

A scan that walks the working tree with ``rglob`` or ``os.walk`` sees every
file on disk, including one another test creates for a few seconds and then
deletes. Under ``pytest -n`` that other test runs in a parallel worker, so the
scan can list the file and then fail to open it: dev Tests run 36370525406
(fast-linux-3) went red that way, with ``FileNotFoundError`` on
``tests/live/zz_unlisted_probe/test_probe.py``.

The universe these scans mean is the code the repository authors, which is the
set of files git tracks. Walking that set instead of the disk makes a scan
independent of whatever other tests, editors, or build steps put in the tree
while it runs.

A tracked file that is missing from disk (deleted, not yet staged) is left out:
there is nothing to read, and the deletion is what the next commit carries.

Usage::

    from tests.pytest.tracked_files import tracked_files

    for path in tracked_files(REPO_ROOT / "tests", "*.py"):
        ...
"""

from __future__ import annotations

import fnmatch
import functools
import subprocess
from pathlib import Path


def _git(root: Path, *args: str) -> bytes:
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        ["git", "-C", str(root), *args],  # noqa: S607
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        # A scan over zero files would pass for the wrong reason, so a root
        # outside a checkout is an error, never an empty universe.
        raise RuntimeError(
            f"git {' '.join(args)} failed under {root}: "
            f"{completed.stderr.decode('utf-8', 'replace').strip()}"
        )
    return completed.stdout


@functools.lru_cache(maxsize=None)
def _tracked_under(root: Path) -> tuple[Path, ...]:
    """Every path git tracks under ``root`` (absolute, resolved, sorted)."""
    toplevel = Path(_git(root, "rev-parse", "--show-toplevel").decode().strip())
    listing = _git(root, "ls-files", "-z", "--full-name", "--", ".")
    return tuple(
        sorted(
            toplevel.resolve() / entry
            for entry in listing.decode("utf-8").split("\0")
            if entry
        )
    )


def tracked_files(root: Path, pattern: str = "*") -> list[Path]:
    """Tracked files under ``root`` whose name matches ``pattern``, sorted.

    ``pattern`` is matched against the file name, as ``Path.rglob`` does for a
    plain name pattern such as ``"*.py"`` or ``"test_*.py"``. Paths come back
    under ``root`` as the caller spelled it, so ``path.relative_to(root)`` and
    ``path.relative_to(REPO_ROOT)`` keep working. Files missing from disk are
    skipped.
    """
    root = Path(root)
    resolved = root.resolve()
    matches: list[Path] = []
    for path in _tracked_under(resolved):
        if not fnmatch.fnmatchcase(path.name, pattern):
            continue
        if not path.is_file():
            continue
        matches.append(root / path.relative_to(resolved))
    return matches
