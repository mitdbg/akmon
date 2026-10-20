"""Directories okg writes at run time inside the directory it runs from.

okg writes upgrade state (`.okg/up/`) and backups (`.okg/backups/`)
relative to the current working directory. (Profiles moved to okg's user
data folder; see `okg.substrate.profiling.artifacts.profile_root`.) okg's own repository gitignores them, but a deployment kept in its
own Git repository does not, and publish refuses an uncommitted tree
(`repository provenance inputs are not committed`). Each such directory
therefore ignores itself with a one-line `.gitignore`, the pattern
`.okg/source-add/` already uses (sources/guided_add.py).
"""

from __future__ import annotations

from pathlib import Path

#: `.okg/<name>` directories okg creates for its own run-time state.
OKG_RUNTIME_SUBDIRS = frozenset({"up", "backups"})


def mark_runtime_dir(directory: Path) -> Path:
    """Create ``directory`` and make Git ignore everything inside it."""
    directory.mkdir(parents=True, exist_ok=True)
    ignore = directory / ".gitignore"
    if not ignore.exists():
        ignore.write_text("*\n", encoding="utf-8")
    return directory


def mark_okg_runtime_ancestor(path: Path) -> None:
    """Mark the `.okg/up` or `.okg/backups` directory that contains ``path``.

    Paths elsewhere (an explicit `--state-file` or `--backup-dir` the operator
    chose) are left alone: okg ignores only directories it chose itself.
    """
    parts = path.parts
    for index in range(len(parts) - 1):
        if parts[index] == ".okg" and parts[index + 1] in OKG_RUNTIME_SUBDIRS:
            mark_runtime_dir(Path(*parts[: index + 2]))
            return
