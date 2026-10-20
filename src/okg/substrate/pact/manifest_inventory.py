"""Fresh, digest-keyed snapshots of active PACT manifests.

The inventory is deliberately a lower-level boundary than validation or
lifecycle state.  Every call discovers the active change directories again and
reads each present ``pact.yaml`` once.  Only successful Pydantic parses are
memoized; the cached model is an internal template and callers receive a deep
copy so a consumer can safely mutate its own manifest.
"""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import stat
from threading import RLock
from types import MappingProxyType
from typing import Literal, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.models import ID_RE, PactManifest
from okg.substrate.pact.store import load_manifest_bytes, state_digest


MAX_PARSE_CACHE_ENTRIES = 256
"""Maximum number of successful parsed snapshots retained by this module."""

MAX_PARSE_CACHE_BYTES = 8 * 1024 * 1024
"""Maximum aggregate source-byte weight represented by cached parse models."""


InventoryStatus = Literal["ok", "missing", "unreadable", "invalid"]
ManifestCollection = Literal["changes", "archive"]


@dataclass(frozen=True, slots=True)
class ResolvedManifest:
    """One securely resolved active or archived manifest for a read operation."""

    manifest: PactManifest
    path: Path
    collection: ManifestCollection

    @property
    def archived(self) -> bool:
        return self.collection == "archive"


@dataclass(frozen=True, slots=True)
class ManifestInventoryEntry:
    """One deterministically ordered active-change directory observation."""

    change_id: str
    path: Path
    relative_path: str
    payload: bytes | None
    content_digest: str | None
    state_digest: str | None
    manifest: PactManifest | None
    status: InventoryStatus
    error: SubstrateError | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    @property
    def digest(self) -> str | None:
        """Short alias used by callers that treat this as a content snapshot."""
        return self.content_digest

    @property
    def missing(self) -> bool:
        return self.status == "missing"


@dataclass(frozen=True, slots=True)
class ManifestInventory:
    """The fresh result of one active-manifest inventory operation."""

    repo_root: Path
    entries: tuple[ManifestInventoryEntry, ...]

    def __iter__(self):
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    @property
    def valid_entries(self) -> tuple[ManifestInventoryEntry, ...]:
        return tuple(entry for entry in self.entries if entry.ok)

    @property
    def errors(self) -> tuple[ManifestInventoryEntry, ...]:
        return tuple(entry for entry in self.entries if not entry.ok)

    @property
    def by_change(self) -> Mapping[str, ManifestInventoryEntry]:
        return MappingProxyType({entry.change_id: entry for entry in self.entries})


@dataclass(frozen=True, slots=True)
class _ParseKey:
    repo_identity: str
    relative_path: str
    content_digest: str


_PARSE_CACHE: OrderedDict[_ParseKey, tuple[PactManifest, int]] = OrderedDict()
_PARSE_CACHE_BYTES = 0
_CACHE_LOCK = RLock()


def _deep_copy_manifest(manifest: PactManifest) -> PactManifest:
    """Return an exclusive model, including all nested Pydantic containers."""
    # ``model_copy(deep=True)`` is the native Pydantic operation, while the
    # deepcopy fallback keeps this boundary robust if a future model adds a
    # non-standard nested value that Pydantic does not copy itself.
    try:
        return manifest.model_copy(deep=True)
    except (AttributeError, TypeError):
        return deepcopy(manifest)


def _normalized_repo_root(repo_root: Path | str) -> Path:
    candidate = Path(repo_root).expanduser()
    try:
        return candidate.resolve(strict=False)
    except OSError:
        return Path(os.path.abspath(os.fspath(candidate)))


def _repo_identity(repo_root: Path) -> str:
    # normcase prevents spelling/case aliases from creating distinct cache
    # entries on case-insensitive hosts, while resolve collapses symlink and
    # ``..`` aliases to the same repository identity.
    return os.path.normcase(os.fspath(repo_root))


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _cache_get(key: _ParseKey) -> PactManifest | None:
    with _CACHE_LOCK:
        cached = _PARSE_CACHE.pop(key, None)
        if cached is None:
            return None
        template, source_bytes = cached
        _PARSE_CACHE[key] = (template, source_bytes)
        return _deep_copy_manifest(template)


def _cache_put(
    key: _ParseKey,
    manifest: PactManifest,
    *,
    source_bytes: int,
) -> PactManifest:
    global _PARSE_CACHE_BYTES
    with _CACHE_LOCK:
        replaced = _PARSE_CACHE.pop(key, None)
        if replaced is not None:
            _PARSE_CACHE_BYTES -= replaced[1]
        if source_bytes <= MAX_PARSE_CACHE_BYTES:
            _PARSE_CACHE[key] = (_deep_copy_manifest(manifest), source_bytes)
            _PARSE_CACHE_BYTES += source_bytes
        while (
            len(_PARSE_CACHE) > MAX_PARSE_CACHE_ENTRIES
            or _PARSE_CACHE_BYTES > MAX_PARSE_CACHE_BYTES
        ):
            _, (_, evicted_bytes) = _PARSE_CACHE.popitem(last=False)
            _PARSE_CACHE_BYTES -= evicted_bytes
    return _deep_copy_manifest(manifest)


def clear_manifest_inventory_cache() -> None:
    """Clear successful parse templates (primarily useful for focused tests)."""
    global _PARSE_CACHE_BYTES
    with _CACHE_LOCK:
        _PARSE_CACHE.clear()
        _PARSE_CACHE_BYTES = 0


def read_manifest_payload(
    repo_root: Path,
    change_id: str,
    path: Path,
    *,
    collection: str = "changes",
) -> bytes:
    """Read through no-follow directory handles when the platform supports it."""
    if collection not in {"changes", "archive"}:
        raise ValueError(f"unsupported PACT manifest collection: {collection}")
    if os.name != "posix":  # pragma: no cover - exercised by Windows CI
        components = (
            repo_root / "pact",
            repo_root / "pact" / collection,
            path.parent,
            path,
        )
        if any(component.is_symlink() for component in components):
            raise OSError(f"symlinked PACT manifest path is not allowed: {path}")
        return path.read_bytes()

    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    repo_fd = os.open(repo_root, directory_flags)
    try:
        pact_fd = os.open("pact", directory_flags, dir_fd=repo_fd)
        try:
            collection_fd = os.open(collection, directory_flags, dir_fd=pact_fd)
            try:
                change_fd = os.open(change_id, directory_flags, dir_fd=collection_fd)
                try:
                    manifest_fd = os.open("pact.yaml", file_flags, dir_fd=change_fd)
                    try:
                        if not stat.S_ISREG(os.fstat(manifest_fd).st_mode):
                            raise OSError(f"PACT manifest is not a regular file: {path}")
                        chunks: list[bytes] = []
                        while True:
                            chunk = os.read(manifest_fd, 1024 * 1024)
                            if not chunk:
                                return b"".join(chunks)
                            chunks.append(chunk)
                    finally:
                        os.close(manifest_fd)
                finally:
                    os.close(change_fd)
            finally:
                os.close(collection_fd)
        finally:
            os.close(pact_fd)
    finally:
        os.close(repo_fd)


def _validate_lookup_change_id(change_id: str) -> None:
    """Reject path-shaped ids before any lifecycle candidate is constructed."""
    if not isinstance(change_id, str) or ID_RE.fullmatch(change_id) is None:
        raise SubstrateError(
            f"invalid PACT change id: {change_id!r}",
            data={
                "change": change_id,
                "rule": (
                    "start with an alphanumeric character and contain only "
                    "letters, numbers, '_', '-', '.', ':'"
                ),
            },
        )


def load_manifest_for_read(
    repo_root: Path | str,
    change_id: str,
) -> ResolvedManifest:
    """Load exactly one active/archive manifest through no-follow reads.

    This is deliberately not the store's ``load_manifest`` replacement: mutation
    continues to resolve only ``pact/changes`` and to hold that manifest's writer
    lock. Read-only commands use this boundary when terminal archived changes are
    valid subjects.
    """
    _validate_lookup_change_id(change_id)
    normalized_root = _normalized_repo_root(repo_root)
    candidates: list[tuple[ManifestCollection, Path, bytes]] = []
    unreadable: list[dict[str, str]] = []
    for collection in ("changes", "archive"):
        candidate = (
            normalized_root / "pact" / collection / change_id / "pact.yaml"
        )
        try:
            payload = read_manifest_payload(
                normalized_root,
                change_id,
                candidate,
                collection=collection,
            )
        except FileNotFoundError:
            continue
        except OSError as exc:
            unreadable.append(
                {
                    "collection": collection,
                    "path": str(candidate),
                    "error": str(exc),
                }
            )
            continue
        candidates.append((collection, candidate, payload))

    if unreadable:
        raise SubstrateError(
            f"unable to read PACT manifest candidate for {change_id}",
            data={"change": change_id, "candidates": unreadable},
        )
    if not candidates:
        raise SubstrateError(
            f"PACT change not found: {change_id}",
            data={
                "change": change_id,
                "paths": [
                    str(
                        normalized_root
                        / "pact"
                        / collection
                        / change_id
                        / "pact.yaml"
                    )
                    for collection in ("changes", "archive")
                ],
            },
        )
    if len(candidates) != 1:
        raise SubstrateError(
            f"duplicate active and archived PACT manifests for {change_id}",
            data={
                "change": change_id,
                "paths": [str(path) for _, path, _ in candidates],
            },
        )

    collection, path, payload = candidates[0]
    manifest = load_manifest_bytes(payload, source_path=path)
    if manifest.change.id != change_id:
        raise SubstrateError(
            f"PACT manifest id does not match resolved path: {path}",
            data={
                "change": change_id,
                "manifest_change": manifest.change.id,
                "path": str(path),
            },
        )
    return ResolvedManifest(
        manifest=manifest,
        path=path,
        collection=collection,
    )


def _path_error(
    message: str,
    path: Path,
    *,
    cause: BaseException | None = None,
) -> SubstrateError:
    data = {"path": str(path)}
    if cause is not None:
        data["error"] = str(cause)
    return SubstrateError(message, data=data)


def _entry_for_path(
    repo_root: Path,
    repo_identity: str,
    path: Path,
) -> ManifestInventoryEntry:
    relative_path = path.relative_to(repo_root).as_posix()
    change_id = path.parent.name
    try:
        # This is the sole file read for an entry.  Parsing consumes the bytes
        # directly, so cache hits still observe fresh bytes and same-mtime edits.
        payload = read_manifest_payload(repo_root, change_id, path)
    except FileNotFoundError as exc:
        return ManifestInventoryEntry(
            change_id=change_id,
            path=path,
            relative_path=relative_path,
            payload=None,
            content_digest=None,
            state_digest=None,
            manifest=None,
            status="missing",
            error=_path_error(
                f"PACT manifest missing: {path}",
                path,
                cause=exc,
            ),
        )
    except OSError as exc:
        return ManifestInventoryEntry(
            change_id=change_id,
            path=path,
            relative_path=relative_path,
            payload=None,
            content_digest=None,
            state_digest=None,
            manifest=None,
            status="unreadable",
            error=_path_error(
                f"unable to read PACT manifest: {path}",
                path,
                cause=exc,
            ),
        )

    content_digest = _digest(payload)
    key = _ParseKey(repo_identity, relative_path, content_digest)
    manifest = _cache_get(key)
    if manifest is None:
        try:
            parsed = load_manifest_bytes(payload, source_path=path)
        except SubstrateError as exc:
            # Invalid entries are intentionally not memoized: a corrected
            # write must recover on the next operation without cache clearing.
            return ManifestInventoryEntry(
                change_id=change_id,
                path=path,
                relative_path=relative_path,
                payload=payload,
                content_digest=content_digest,
                state_digest=None,
                manifest=None,
                status="invalid",
                error=exc,
            )
        except Exception as exc:  # noqa: BLE001 - isolate one bad row
            return ManifestInventoryEntry(
                change_id=change_id,
                path=path,
                relative_path=relative_path,
                payload=payload,
                content_digest=content_digest,
                state_digest=None,
                manifest=None,
                status="invalid",
                error=_path_error(
                    f"failed to parse PACT manifest: {path}",
                    path,
                    cause=exc,
                ),
            )
        manifest = _cache_put(key, parsed, source_bytes=len(payload))

    return ManifestInventoryEntry(
        change_id=change_id,
        path=path,
        relative_path=relative_path,
        payload=payload,
        content_digest=content_digest,
        state_digest=state_digest(manifest),
        manifest=manifest,
        status="ok",
    )


def capture_manifest_inventory(repo_root: Path | str) -> ManifestInventory:
    """Capture all direct active-change directories in stable name order.

    Discovery is intentionally uncached.  A child directory without a
    ``pact.yaml`` remains an explicit ``missing`` entry, while one malformed
    row cannot hide later valid rows.
    """
    normalized_root = _normalized_repo_root(repo_root)
    changes_root = normalized_root / "pact" / "changes"
    try:
        directories = sorted(
            (
                path
                for path in changes_root.iterdir()
                if path.is_dir() or path.is_symlink()
            ),
            key=lambda path: path.name,
        )
    except FileNotFoundError:
        directories = []
    except OSError as exc:
        raise _path_error(
            f"unable to enumerate active PACT changes: {changes_root}",
            changes_root,
            cause=exc,
        ) from exc

    repo_identity = _repo_identity(normalized_root)
    entries = tuple(
        _entry_for_path(
            normalized_root,
            repo_identity,
            directory / "pact.yaml",
        )
        for directory in directories
    )
    return ManifestInventory(repo_root=normalized_root, entries=entries)


# Concise aliases for callers that already use ``inventory`` terminology.
capture_inventory = capture_manifest_inventory
clear_inventory_cache = clear_manifest_inventory_cache


__all__ = [
    "MAX_PARSE_CACHE_BYTES",
    "MAX_PARSE_CACHE_ENTRIES",
    "ManifestInventory",
    "ManifestInventoryEntry",
    "ResolvedManifest",
    "capture_inventory",
    "capture_manifest_inventory",
    "clear_inventory_cache",
    "clear_manifest_inventory_cache",
    "load_manifest_for_read",
    "read_manifest_payload",
]
