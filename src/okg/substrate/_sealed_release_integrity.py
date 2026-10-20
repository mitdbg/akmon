"""Filesystem integrity and immutability helpers for sealed releases."""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import stat
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from okg.substrate.errors import SealedReleaseInvalid


_IGNORED_TREE_PARTS = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
    }
)

_LOG = logging.getLogger(__name__)
SEALED_BYTECODE_REASON = "sealed_runtime_bytecode_present"
_BYTECODE_REPORT_LIMIT = 20


def is_bytecode_relative_name(relative_name: str) -> bool:
    """Report whether a release-relative name is Python bytecode (#2857).

    Case-folded so a case-insensitive filesystem cannot hide ``X.PYC`` from
    the check while CPython's finder still matches it.
    """

    parts = PurePosixPath(relative_name).parts
    return any(part.casefold() == "__pycache__" for part in parts) or (
        relative_name.casefold().endswith((".pyc", ".pyo"))
    )


def sealed_bytecode_refusal(relative_names: list[str]) -> SealedReleaseInvalid:
    """The refusal for bytecode inside a sealed executable tree (#2857).

    The tree digest never covered bytecode, so a planted ``.pyc`` would load
    unverified. A sealed tree therefore holds none: sealing removes it and
    every verification refuses any that appears later.
    """

    ordered = sorted(relative_names)
    return SealedReleaseInvalid(
        "sealed release executable tree contains Python bytecode",
        data={
            "code": SEALED_BYTECODE_REASON,
            "observed": ordered[:_BYTECODE_REPORT_LIMIT],
            "observed_count": len(ordered),
        },
    )


@contextmanager
def private_pycache_prefix(*, outside_fds: Sequence[int] = ()) -> Iterator[list[str]]:
    """Yield interpreter flags that stop bytecode reads from the sealed tree.

    ``-B`` and ``PYTHONDONTWRITEBYTECODE`` stop writes, not reads. With
    ``-X pycache_prefix`` CPython looks for cached bytecode only under that
    directory, so a fresh, empty, private directory per launch means no
    ``__pycache__`` file inside the sealed tree is ever read (#2857).

    The directory must lie outside every tree in ``outside_fds``. The caller
    keeps the context open until the interpreter has exited; the directory must
    then still be empty, so a plant during the run is refused, not ignored.
    """

    directory = Path(tempfile.mkdtemp(prefix="okg-sealed-pycache-")).resolve()
    completed = False
    try:
        os.chmod(directory, 0o700)
        ancestors = [
            (observed.st_dev, observed.st_ino)
            for observed in (item.stat() for item in (directory, *directory.parents))
        ]
        for outside_fd in outside_fds:
            root = os.fstat(outside_fd)
            if (root.st_dev, root.st_ino) in ancestors:
                raise SealedReleaseInvalid(
                    "private bytecode cache directory is inside the sealed tree",
                    data={"code": SEALED_BYTECODE_REASON, "observed": str(directory)},
                )
        if any(directory.iterdir()):
            raise SealedReleaseInvalid(
                "private bytecode cache directory is not empty",
                data={"code": SEALED_BYTECODE_REASON, "observed": str(directory)},
            )
        yield ["-X", f"pycache_prefix={directory}"]
        leftovers = sorted(item.name for item in directory.iterdir())
        if leftovers:
            raise SealedReleaseInvalid(
                "private bytecode cache directory was written during the launch",
                data={"code": SEALED_BYTECODE_REASON, "observed": leftovers},
            )
        completed = True
    finally:
        try:
            shutil.rmtree(directory)
        except FileNotFoundError:
            pass
        except OSError as exc:
            if completed:
                raise SealedReleaseInvalid(
                    "private bytecode cache directory could not be removed",
                    data={"code": SEALED_BYTECODE_REASON, "observed": str(directory)},
                ) from exc
            # Another error is already propagating; report, do not mask it.
            _LOG.warning(
                "private bytecode cache directory %s could not be removed: %s",
                directory,
                exc,
            )


def tree_digest(path: str | Path) -> str:
    root = Path(path).resolve()
    if not root.exists():
        raise SealedReleaseInvalid(
            "release tree does not exist",
            data={"observed": str(root)},
        )
    if root.is_file():
        return _tree_entries_digest(root.parent, [root])
    entries = [
        child
        for child in root.rglob("*")
        if not _ignored_tree_path(child.relative_to(root))
    ]
    return _tree_entries_digest(root, entries)


def executable_tree_digest(artifact_root: Path) -> str:
    roots = [artifact_root / "repo", artifact_root / "venv"]
    missing = [str(path) for path in roots if not path.is_dir()]
    if missing:
        raise SealedReleaseInvalid(
            "sealed release executable layout is incomplete",
            data={"expected": ["repo", "venv"], "observed": missing},
        )
    entries: list[Path] = []
    bytecode: list[str] = []
    for root in roots:
        entries.append(root)
        for child in root.rglob("*"):
            relative = child.relative_to(artifact_root)
            if is_bytecode_relative_name(
                relative.as_posix()
            ) and not _live_profile_path(child, artifact_root=artifact_root):
                bytecode.append(relative.as_posix())
            elif not _ignored_executable_tree_path(child, artifact_root=artifact_root):
                entries.append(child)
    if bytecode:
        raise sealed_bytecode_refusal(bytecode)
    return _tree_entries_digest(artifact_root, entries)


def _tree_entries_digest(root: Path, entries: list[Path]) -> str:
    digest = hashlib.sha256(b"okg-executable-tree-v1\0")
    for path in sorted(entries, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISLNK(metadata.st_mode):
            kind = "symlink"
            content = os.readlink(path).encode("utf-8")
        elif stat.S_ISDIR(metadata.st_mode):
            kind = "directory"
            content = b""
        elif stat.S_ISREG(metadata.st_mode):
            kind = "file"
            content = path.read_bytes()
        else:
            raise SealedReleaseInvalid(
                "release tree contains an unsupported filesystem entry",
                data={"observed": relative},
            )
        digest.update(kind.encode("ascii"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode("ascii"))
        digest.update(b"\0")
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def validate_contained_symlinks(root: Path) -> None:
    resolved_root = root.resolve()
    for path in root.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(resolved_root)
        except (OSError, ValueError) as exc:
            raise SealedReleaseInvalid(
                "sealed executable tree contains an escaping symlink",
                data={"expected": str(resolved_root), "observed": str(path)},
            ) from exc


def remove_runtime_caches(root: Path) -> None:
    """Delete tool caches and all bytecode before sealing.

    Bytecode uses the same case-insensitive predicate that verification
    refuses with, including a ``__pycache__`` that is a file or symlink, so
    sealing never produces a tree its own verification rejects (#2857).
    """

    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink() or not path.is_dir():
            if is_bytecode_relative_name(path.name):
                path.unlink(missing_ok=True)
        elif path.name in {
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
        } or is_bytecode_relative_name(path.name):
            shutil.rmtree(path, ignore_errors=True)


def seal_permissions(root: Path) -> None:
    for path in [*sorted(root.rglob("*"), reverse=True), root]:
        if path.is_symlink():
            continue
        path.chmod(stat.S_IMODE(path.stat().st_mode) & ~0o222)


def remove_artifact_tree(root: Path) -> None:
    if not root.exists():
        return
    for path in [root, *root.rglob("*")]:
        if path.is_symlink():
            continue
        try:
            path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o700)
        except OSError:
            pass
    shutil.rmtree(root, ignore_errors=True)


def _ignored_tree_path(relative: Path) -> bool:
    if any(part in _IGNORED_TREE_PARTS for part in relative.parts):
        return True
    return relative.suffix in {".pyc", ".pyo"}


def _ignored_executable_tree_path(path: Path, *, artifact_root: Path) -> bool:
    relative = path.relative_to(artifact_root)
    if _ignored_tree_path(relative):
        return True
    return _live_profile_path(path, artifact_root=artifact_root)


def _live_profile_path(path: Path, *, artifact_root: Path) -> bool:
    """Mutable profile state under ``repo/profiles``: not executable content."""

    parts = path.relative_to(artifact_root).parts
    if len(parts) < 2 or parts[:2] != ("repo", "profiles"):
        return False
    if len(parts) <= 3:
        return path.is_dir() and not path.is_symlink()
    if parts[2] == "_live" or parts[3] != "_live":
        return False
    live_root = artifact_root.joinpath(*parts[:4])
    return live_root.is_dir() and not live_root.is_symlink()
