"""Shared `FileWalk` helper for delta-proportional source runs.

Sources whose per-file work includes AST parsing, tree-sitter
extraction, or document parsing get slow at scale when they
re-process every file every cycle. The fix is the same in all
three: walk the tree, fast-path past files whose filesystem
mtime hasn't moved since the prior cursor, and for files that
DID move (or are new), compute SHA-256 and compare against the
prior cursor's hash before doing any heavy per-file work.

This module factors that pattern into one place so AST sources
don't reimplement it three times (with different bugs each time).
Sources only have to write:

    walk = FileWalk(prior_cursor=cursor, repo_root=self.repo_root)
    for event in walk.iter(self._walk_paths()):
        if event.is_changed:
            # event.content is the file bytes/text, ready to parse
            facts.extend(self._extract_one_file(event.rel_path,
                                                event.content))
        # else: skip — file is unchanged since the prior cursor
    next_cursor = walk.next_cursor(run_id=run_id, **other_fields)

Per the graph-ingestion spec requirements
"AST-Extracting Sources Skip Unchanged Files" and the mtime fast-
path scenarios (see `shrink-source-walk-cost`).

The cursor's `files` map shape:

    {
      "<rel_path>": {"sha256": "<hex>", "mtime_ns": <int>},
      ...
    }

Forward-compat: a worker reading a pre-mtime cursor sees string-
valued entries (the old `<rel_path>: <sha>` shape). Those are
treated as `{"sha256": <sha>, "mtime_ns": None}` — the mtime
fast-path is inert for them, but the content-hash short-circuit
still fires. After one run on the new code, all entries upgrade
to the dict shape.
"""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Union


__all__ = [
    "ContainedFileReader", "FileContainmentError", "FileReadLimitError",
    "FileWalk", "FileWalkEvent",
]


class FileContainmentError(OSError):
    """A corpus file could not be proven beneath its anchored root."""


class FileReadLimitError(FileContainmentError):
    """A securely opened corpus file exceeded its configured read bound."""


class ContainedFileReader:
    """Open corpus files relative to a held root fd without path races."""

    _MAX_SYMLINKS = 40

    def __init__(self, root: Path) -> None:
        required = (os.open, os.stat, os.readlink)
        if not all(fn in os.supports_dir_fd for fn in required):
            raise FileContainmentError(
                "secure corpus reads require open/stat/readlink dir_fd support"
            )
        if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise FileContainmentError(
                "secure corpus reads require O_NOFOLLOW and O_DIRECTORY"
            )
        self.root = Path(root).expanduser().resolve()
        self._root_fd = os.open(
            self.root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )

    def close(self) -> None:
        if self._root_fd >= 0:
            os.close(self._root_fd)
            self._root_fd = -1

    def __enter__(self) -> "ContainedFileReader":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def lexical_relative(self, path: Path) -> str:
        try:
            rel = Path(path).absolute().relative_to(self.root)
        except ValueError as exc:
            raise FileContainmentError(f"candidate escapes corpus root: {path}") from exc
        parts = self._normalize(list(rel.parts))
        if not parts:
            raise FileContainmentError(f"invalid corpus-relative path: {path}")
        return Path(*parts).as_posix()

    def _normalize(self, parts: list[str]) -> list[str]:
        out: list[str] = []
        for part in parts:
            if part in {"", "."}:
                continue
            if part == "..":
                if not out:
                    raise FileContainmentError("path escapes corpus root")
                out.pop()
            else:
                out.append(part)
        return out

    def _target_parts(self, target: str, parent: list[str]) -> list[str]:
        target_path = Path(target)
        if target_path.is_absolute():
            try:
                rel = target_path.relative_to(self.root)
            except ValueError as exc:
                raise FileContainmentError(
                    f"symlink target escapes corpus root: {target}"
                ) from exc
            return self._normalize(list(rel.parts))
        return self._normalize(parent + list(target_path.parts))

    def _directory_is_beneath_root(self, directory_fd: int) -> bool:
        root_key = (os.fstat(self._root_fd).st_dev, os.fstat(self._root_fd).st_ino)
        current = os.dup(directory_fd)
        try:
            for _ in range(256):
                current_stat = os.fstat(current)
                current_key = (current_stat.st_dev, current_stat.st_ino)
                if current_key == root_key:
                    return True
                parent = os.open(
                    "..",
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=current,
                )
                parent_stat = os.fstat(parent)
                parent_key = (parent_stat.st_dev, parent_stat.st_ino)
                os.close(current)
                current = parent
                if parent_key == current_key:
                    return False
            raise FileContainmentError("corpus directory ancestry exceeds 256 levels")
        finally:
            os.close(current)

    def open(self, path: Path, *, directory: bool = False) -> tuple[int, str]:
        lexical_rel = self.lexical_relative(path)
        pending = list(Path(lexical_rel).parts)
        symlinks = 0
        while True:
            current = os.dup(self._root_fd)
            resolved: list[str] = []
            restart = False
            try:
                for index, part in enumerate(pending):
                    final = index == len(pending) - 1
                    st = os.stat(part, dir_fd=current, follow_symlinks=False)
                    if stat.S_ISLNK(st.st_mode):
                        target = os.readlink(part, dir_fd=current)
                        symlinks += 1
                        if symlinks > self._MAX_SYMLINKS:
                            raise FileContainmentError("too many corpus symlinks")
                        pending = (
                            self._target_parts(target, resolved)
                            + pending[index + 1 :]
                        )
                        restart = True
                        break
                    flags = os.O_RDONLY | os.O_NOFOLLOW
                    if final:
                        flags |= os.O_NONBLOCK
                    if not final or directory:
                        flags |= os.O_DIRECTORY
                    next_fd = os.open(part, flags, dir_fd=current)
                    if final and not self._directory_is_beneath_root(current):
                        os.close(next_fd)
                        raise FileContainmentError(
                            f"opened corpus ancestor moved outside root: {lexical_rel}"
                        )
                    os.close(current)
                    current = next_fd
                    resolved.append(part)
                if restart:
                    continue
                if not pending:
                    raise FileContainmentError("empty corpus path")
                mode = os.fstat(current).st_mode
                if directory and not stat.S_ISDIR(mode):
                    raise FileContainmentError(
                        f"corpus candidate is not a directory: {lexical_rel}"
                    )
                if not directory and not stat.S_ISREG(mode):
                    raise FileContainmentError(
                        f"corpus candidate is not a regular file: {lexical_rel}"
                    )
                result = current
                current = -1
                return result, lexical_rel
            finally:
                if current >= 0:
                    os.close(current)

    def read_bytes(
        self, path: Path, *, max_bytes: int | None = None
    ) -> tuple[os.stat_result, bytes, str]:
        fd, rel = self.open(path)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise FileContainmentError(f"corpus candidate is not a regular file: {rel}")
            if max_bytes is not None and st.st_size > max_bytes:
                raise FileReadLimitError(
                    f"corpus candidate exceeds max_bytes={max_bytes}: {rel}"
                )
            chunks: list[bytes] = []
            total = 0
            while chunk := os.read(fd, 1024 * 1024):
                chunks.append(chunk)
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise FileReadLimitError(
                        f"corpus candidate exceeds max_bytes={max_bytes}: {rel}"
                    )
            return st, b"".join(chunks), rel
        finally:
            os.close(fd)

    def sha256(self, path: Path) -> tuple[os.stat_result, str, str]:
        """Hash a contained regular file without keeping its bytes.

        Used for a file over the read limit: the caller records it (size and
        content hash) without loading it into memory.
        """
        fd, rel = self.open(path)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise FileContainmentError(f"corpus candidate is not a regular file: {rel}")
            digest = hashlib.sha256()
            while chunk := os.read(fd, 1024 * 1024):
                digest.update(chunk)
            return st, digest.hexdigest(), rel
        finally:
            os.close(fd)

    def stat(self, path: Path, *, directory: bool = False) -> os.stat_result:
        if directory and Path(path).absolute() == self.root:
            return os.fstat(self._root_fd)
        fd, _rel = self.open(path, directory=directory)
        try:
            return os.fstat(fd)
        finally:
            os.close(fd)


# When set, the mtime fast-path is disabled and every walked file
# is read + hashed (current pre-mtime behavior). Useful on exotic
# filesystems where mtime is unreliable (some FUSE mounts).
_USE_MTIME_ENV = "OKG_FILE_WALK_USE_MTIME"


def _mtime_fast_path_enabled() -> bool:
    """Return False only when the operator explicitly disables it
    via `OKG_FILE_WALK_USE_MTIME=0`."""
    val = os.environ.get(_USE_MTIME_ENV)
    if val is None:
        return True
    return val.strip() not in ("0", "false", "False", "no", "NO")


@dataclass(frozen=True)
class FileWalkEvent:
    """One walked file.

    `is_changed=False` means the source SHOULD skip per-file
    extraction work for `rel_path`. The cursor is still being
    populated so the next run sees the same hash and mtime and
    can short-circuit again.

    `content` is the file's bytes (when `read_as="bytes"`) or
    text (when `read_as="text"`) **only when the file was read**.
    When the mtime fast-path fires (mtime matches prior cursor),
    the source did NOT read the file and `content` is `None`.
    Sources that produce facts from `event.content` MUST gate on
    `event.is_changed` — the fast-path only emits unchanged
    events, and consumers should be skipping them anyway.

    `content_hash` is always populated (with the prior cursor's
    sha when fast-path, or with the freshly computed sha
    otherwise).
    """

    rel_path: str
    content: Optional[Union[bytes, str]]
    content_hash: str
    is_changed: bool
    #: Set only by a walk built with ``oversize="record"``: the file's size
    #: when it exceeds ``max_bytes``. Its bytes are then never loaded
    #: (``content`` is ``None``); ``content_hash`` is still the SHA-256 of
    #: the whole file, computed by streaming it.
    oversize_bytes: Optional[int] = None
    #: The file's first ``OVERSIZE_HEAD_BYTES`` bytes when ``oversize_bytes``
    #: is set, so a caller can still sniff its format.
    head: Optional[bytes] = None


#: How many leading bytes an oversize event carries in ``FileWalkEvent.head``.
OVERSIZE_HEAD_BYTES = 64


class FileWalk:
    """Stateful driver for the delta-proportional file-walk pattern.

    Construct one per `run()` call. Iterate `walk.iter(paths)` to
    receive `FileWalkEvent`s, then read `walk.next_cursor(**extra)`
    to assemble the next-run cursor.

    The helper owns: the walk loop, per-file stat-and-mtime
    comparison, per-file SHA-256 hashing (only when needed), the
    `files` cursor map, and the change-decision. It does NOT know
    about node-id minting, AST parsing, or record_set
    construction — those are the source's concern.
    """

    def __init__(
        self,
        *,
        prior_cursor: Optional[dict[str, Any]],
        repo_root: Path,
        read_as: str = "text",
        strict_io: bool = False,
        contained_io: bool = False,
        contained_reader: ContainedFileReader | None = None,
        max_bytes: int | None = None,
        oversize: str = "raise",
    ) -> None:
        if read_as not in ("text", "bytes"):
            raise ValueError(
                f"FileWalk(read_as={read_as!r}): expected 'text' or 'bytes'"
            )
        # What a contained walk does with a file larger than `max_bytes`:
        # "raise" stops the walk with FileReadLimitError; "record" yields an
        # event with `oversize_bytes` set and no content, so the caller can
        # record the file with a warning and keep going. Either way the
        # file's bytes are never loaded into memory.
        if oversize not in ("raise", "record"):
            raise ValueError(
                f"FileWalk(oversize={oversize!r}): expected 'raise' or 'record'"
            )
        self._oversize = oversize
        self._repo_root: Path = Path(repo_root).resolve()
        self._read_as: str = read_as
        self._strict_io = strict_io
        self._contained_io = contained_io
        self._contained_reader = contained_reader
        self._max_bytes = max_bytes
        # The prior cursor's `files` map. Each entry is either
        # the new dict shape ({"sha256": ..., "mtime_ns": ...}) or
        # a bare sha256 string (forward-compat for cursors written
        # by the pre-mtime code). We don't normalize here — the
        # callsite branches on type to keep the fast-path tight.
        prior_files: Any = (prior_cursor or {}).get("files")
        self._prior_files: dict[str, Any] = (
            dict(prior_files) if isinstance(prior_files, dict) else {}
        )
        # Populated as we walk. Always written in the new dict
        # shape, regardless of what the prior cursor used.
        self._next_files: dict[str, dict[str, Any]] = {}
        self._use_mtime: bool = _mtime_fast_path_enabled()

    @staticmethod
    def _prior_entry(
        entry: Any,
    ) -> tuple[Optional[str], Optional[int]]:
        """Decode a cursor entry to `(sha256, mtime_ns)`.

        Tolerates both shapes:
          - new: `{"sha256": "<hex>", "mtime_ns": <int>}`
          - old: `"<hex>"` (a bare sha; mtime_ns is None and the
                  fast-path is inert for this entry)
        """
        if isinstance(entry, dict):
            sha = entry.get("sha256")
            mtime = entry.get("mtime_ns")
            return (
                sha if isinstance(sha, str) else None,
                mtime if isinstance(mtime, int) else None,
            )
        if isinstance(entry, str):
            return entry, None
        return None, None

    def iter(
        self,
        walker_paths: Iterable[Path],
    ) -> Iterator[FileWalkEvent]:
        """Yield one event per walked path.

        Hot path:
          1. `stat()` the path (cheap, ~1–50 μs).
          2. If the prior cursor has an mtime for this path AND
             stat's mtime matches AND fast-path is enabled, carry
             the prior hash forward. Yield an unchanged event
             without reading the file.
          3. Otherwise, read the file, compute SHA-256, compare
             to the prior cursor's hash. Set `is_changed`
             accordingly. Yield the event.

        Files that fail to `stat` or `read` are silently dropped —
        callers who care about read failures should log themselves.
        Critically, dropped files do NOT appear in `next_cursor`
        so a transient I/O hiccup doesn't cause the source to
        "forget" the file's prior state.
        """
        owns_reader = self._contained_io and self._contained_reader is None
        reader = (
            self._contained_reader
            if self._contained_io
            else None
        ) or (ContainedFileReader(self._repo_root) if owns_reader else None)
        try:
            yield from self._iter_paths(walker_paths, reader)
        finally:
            if owns_reader and reader is not None:
                reader.close()

    def _iter_paths(
        self,
        walker_paths: Iterable[Path],
        reader: ContainedFileReader | None,
    ) -> Iterator[FileWalkEvent]:
        for path in walker_paths:
            fd = -1
            try:
                if reader is None:
                    stat_result = path.stat()
                else:
                    fd, rel_path = reader.open(path)
                    stat_result = os.fstat(fd)
            except OSError:
                if fd >= 0:
                    os.close(fd)
                if self._strict_io or reader is not None:
                    raise
                # Walker produced a path we can't stat (deleted
                # between walk and iter, permission revoked,
                # broken symlink). Skip silently; don't poison
                # next_cursor.
                continue

            current_mtime_ns: int = stat_result.st_mtime_ns
            if (
                reader is not None
                and self._max_bytes is not None
                and stat_result.st_size > self._max_bytes
            ):
                if self._oversize == "record":
                    yield self._oversize_event(
                        fd, rel_path, stat_result.st_size, current_mtime_ns,
                    )
                    continue
                os.close(fd)
                raise FileReadLimitError(
                    f"corpus candidate exceeds max_bytes={self._max_bytes}: {rel_path}"
                )
            # Per `shrink-source-walk-cost` per-thread profile: a
            # previous `path.resolve()` here did one lstat per file
            # (~1ms × N files = hundreds of ms across sources). The
            # walker (e.g. `_walk_files` / `rglob`) emits absolute
            # paths under `repo_root`; resolving is redundant for
            # correctness because `relative_to` matches by string
            # equality once both paths are normalized.
            #
            # We instead try `relative_to` directly. If the walker
            # produced an absolute path under repo_root, this works
            # in O(string-len). If the walker produces a path
            # outside repo_root (e.g. via a symlinked include glob),
            # we fall back to `resolve` — rare, but correctness
            # demands it.
            if reader is None:
                try:
                    rel_path = str(path.relative_to(self._repo_root)).replace(
                        "\\", "/"
                    )
                except ValueError:
                    rel_path = str(
                        path.resolve().relative_to(self._repo_root)
                    ).replace("\\", "/")
            prior_entry = self._prior_files.get(rel_path)
            prior_sha, prior_mtime = self._prior_entry(prior_entry)

            # mtime fast-path: stat matched, prior sha known.
            # Skip the read entirely; carry the prior sha + new
            # mtime forward.
            if (
                self._use_mtime
                and prior_sha is not None
                and prior_mtime is not None
                and prior_mtime == current_mtime_ns
                and current_mtime_ns != 0  # exotic FS guard
            ):
                if fd >= 0:
                    os.close(fd)
                self._next_files[rel_path] = {
                    "sha256": prior_sha,
                    "mtime_ns": current_mtime_ns,
                }
                yield FileWalkEvent(
                    rel_path=rel_path,
                    content=None,
                    content_hash=prior_sha,
                    is_changed=False,
                )
                continue

            # Cold path: read, hash, decide.
            try:
                if fd >= 0:
                    chunks: list[bytes] = []
                    total = 0
                    while chunk := os.read(fd, 1024 * 1024):
                        chunks.append(chunk)
                        total += len(chunk)
                        if self._max_bytes is not None and total > self._max_bytes:
                            raise FileReadLimitError(
                                "corpus candidate exceeds "
                                f"max_bytes={self._max_bytes}: {rel_path}"
                            )
                    content_bytes = b"".join(chunks)
                else:
                    content_bytes = path.read_bytes()
                if self._read_as == "bytes":
                    content: Union[bytes, str] = content_bytes
                    hash_input: bytes = content_bytes
                else:
                    content_text = content_bytes.decode("utf-8", errors="replace").replace(
                        "\r\n", "\n"
                    ).replace("\r", "\n")
                    content = content_text
                    hash_input = content_text.encode("utf-8")
            except OSError:
                if self._strict_io or reader is not None:
                    raise
                # Read failed; treat as "skip this file from the
                # cursor" so the prior entry (if any) remains the
                # authoritative state next run.
                continue
            finally:
                if fd >= 0:
                    os.close(fd)

            content_hash: str = hashlib.sha256(hash_input).hexdigest()
            self._next_files[rel_path] = {
                "sha256": content_hash,
                "mtime_ns": current_mtime_ns,
            }
            yield FileWalkEvent(
                rel_path=rel_path,
                content=content,
                content_hash=content_hash,
                is_changed=(prior_sha != content_hash),
            )

    def _oversize_event(
        self, fd: int, rel_path: str, size: int, mtime_ns: int,
    ) -> FileWalkEvent:
        """Record one file over `max_bytes` without loading it.

        Reads only its first `OVERSIZE_HEAD_BYTES` bytes. The content hash is
        carried from the prior cursor when the mtime still matches, and is
        otherwise computed by streaming the file through SHA-256. Closes `fd`.
        """
        try:
            head = os.read(fd, OVERSIZE_HEAD_BYTES)
            prior_sha, prior_mtime = self._prior_entry(
                self._prior_files.get(rel_path)
            )
            if (
                self._use_mtime
                and prior_sha is not None
                and prior_mtime is not None
                and prior_mtime == mtime_ns
                and mtime_ns != 0
            ):
                content_hash = prior_sha
            else:
                digest = hashlib.sha256(head)
                while chunk := os.read(fd, 1024 * 1024):
                    digest.update(chunk)
                content_hash = digest.hexdigest()
        finally:
            os.close(fd)
        self._next_files[rel_path] = {"sha256": content_hash, "mtime_ns": mtime_ns}
        return FileWalkEvent(
            rel_path=rel_path,
            content=None,
            content_hash=content_hash,
            is_changed=(prior_sha != content_hash),
            oversize_bytes=size,
            head=head,
        )

    def next_cursor(self, **extra_fields: Any) -> dict[str, Any]:
        """Return the next cursor dict for the source's `SourceRun`.

        Always includes a `files` map keyed by repo-relative
        path. Each entry is a dict `{"sha256": ..., "mtime_ns":
        ...}`. Sources merge in their own keys via kwargs (e.g.
        `run_id=run_id, interface_endpoint_stable_ids_v6=True`).
        """
        out: dict[str, Any] = {"files": dict(self._next_files)}
        out.update(extra_fields)
        return out

    @property
    def prior_files(self) -> dict[str, Any]:
        """The prior cursor's `files` map (raw, mixed shape).
        Read-only convenience for sources that need to detect
        "paths I knew about before but didn't see this walk"
        (deletions). Returns a defensive copy."""
        return dict(self._prior_files)

    @property
    def next_files(self) -> dict[str, dict[str, Any]]:
        """The in-progress new `files` map (mutates as `iter`
        yields). Read-only convenience for code that wants to
        inspect post-state before calling `next_cursor`."""
        return dict(self._next_files)
