"""Immutable local-file snapshots for exact source-run identity binding."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_COPY_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class CapturedContentItem:
    key: str
    original_path: Path
    snapshot_path: Path
    sha256: str
    size: int


class CapturedContentSnapshot:
    """A temporary immutable copy of one membership-complete file scope."""

    def __init__(
        self,
        items: Iterable[tuple[str, Path]],
        *,
        mirror_relative_paths: bool = False,
    ) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="okg-content-snapshot-"))
        self._closed = False
        captured: list[CapturedContentItem] = []
        seen: set[str] = set()
        for index, (raw_key, raw_path) in enumerate(items):
            key = str(raw_key)
            if key in seen:
                self.close()
                raise ValueError(f"duplicate content snapshot key: {key}")
            seen.add(key)
            original = Path(raw_path)
            relative = Path(key) if mirror_relative_paths else Path(
                "items"
            ) / f"{index:08d}"
            if mirror_relative_paths and (
                relative.is_absolute() or ".." in relative.parts
            ):
                self.close()
                raise ValueError(f"unsafe mirrored content key: {key}")
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                source_stat = original.stat()
                digest = hashlib.sha256()
                size = 0
                with original.open("rb") as source, destination.open("wb") as target:
                    while True:
                        chunk = source.read(_COPY_CHUNK_BYTES)
                        if not chunk:
                            break
                        digest.update(chunk)
                        target.write(chunk)
                        size += len(chunk)
                os.chmod(destination, source_stat.st_mode)
                os.utime(
                    destination,
                    ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
                )
            except Exception:
                self.close()
                raise
            captured.append(
                CapturedContentItem(
                    key=key,
                    original_path=original,
                    snapshot_path=destination,
                    sha256=digest.hexdigest(),
                    size=size,
                )
            )
        self.items = tuple(captured)
        self.by_key = {item.key: item for item in captured}
        self.by_original = {
            item.original_path.resolve(): item
            for item in captured
        }

    @property
    def item_hashes(self) -> dict[str, str]:
        return {item.key: item.sha256 for item in self.items}

    @property
    def item_sizes(self) -> dict[str, int]:
        return {item.key: item.size for item in self.items}

    def close(self) -> None:
        if getattr(self, "_closed", True):
            return
        _remove_tree(self.root)
        self._closed = True

    def __enter__(self) -> "CapturedContentSnapshot":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


def _remove_tree(path: Path) -> None:
    """Remove a snapshot without fd-based scandir assumptions."""
    if not path.exists():
        return
    for entry in os.scandir(str(path)):
        child = Path(entry.path)
        if entry.is_dir(follow_symlinks=False):
            _remove_tree(child)
        else:
            child.unlink(missing_ok=True)
    path.rmdir()
