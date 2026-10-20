"""ContentHashProbe — the general O(content) floor probe (issue #323).

When no cheaper churn-immune probe exists — a non-git directory, CI-generated
artifacts that are not git-tracked, a mutable payload — a source falls to this
floor: read + hash the CURRENT in-scope content. The token is a sorted,
**membership-sensitive** digest of ``(key, sha256(bytes))`` over the current
scope plus an item count, so all three mutation kinds bust it:

  - an **edit** changes a content sha;
  - an **add** introduces a new ``(key, sha)`` entry (and bumps the count);
  - a **delete** removes a ``(key, sha)`` entry (and drops the count).

It is content-based, so it is immune to mtime churn. It is O(content)
(read+hash) but the source still skips parse/extraction on no-change — that is
the floor's whole value.

General across sources: supply a ``content_items`` callable that yields the
CURRENT scope as ``(key, source)`` pairs, where ``source`` is bytes, a Path
(read on demand), or a zero-arg callable returning bytes. ``file_content_items``
covers the common file-glob case (a deleted file is simply absent from the next
enumeration, which is exactly how the delete is detected).
"""
from __future__ import annotations

import fnmatch
import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ChangeProbeError,
    ProbeVerdict,
    compose_token,
    cursor_has_outstanding_work,
    emit_version_for,
    tokens_equal,
)


__all__ = [
    "ContentHashProbe",
    "file_content_items",
    "file_prefix_sha256",
    "CURSOR_TOKEN_KEY",
]

ContentItem = Union[bytes, str, Path, Callable[[], bytes]]
_DEFAULT_READ_CHUNK_BYTES = 1024 * 1024
# A just-created or just-modified file can be rewritten within one filesystem
# timestamp tick. In that window, unchanged stat metadata is not enough to
# prove that a streamed digest came from one stable content version.
_STAT_TIMESTAMP_COLLISION_WINDOW_NS = 2_000_000_000
_ITEM_MANIFEST_SCHEMA = "okg.source.content_hash.item_manifest.v1"


@dataclass(frozen=True)
class _FileDigestPass:
    digest: str
    bytes_read: int
    expected_bytes: int
    before_identity: tuple[int, int, int, int, int]
    after_identity: tuple[int, int, int, int, int]
    current_identity: tuple[int, int, int, int, int]
    before_ctime_ns: int
    started_ns: int


class ContentHashProbe:
    """A content-identity floor probe for a source's current scope.

    Parameters
    ----------
    content_items:
        Zero-arg callable returning the CURRENT scope as an iterable of
        ``(key, source)`` pairs. MUST re-enumerate live state on each call so a
        deletion is observed as a dropped entry.
    config:
        The adapter's effective configuration (hashed into the token).
    emit_targets:
        Emission callables/modules (machine-derived emit-version).
    max_bytes_per_item:
        Optional cap on bytes read per item (None = read all). A cap keeps the
        work bounded on huge items. Only set it when bytes beyond the cap
        cannot affect emitted facts; otherwise truncation would permit a
        false-unchanged verdict.
    read_chunk_bytes:
        Maximum bytes requested by each read of a path-backed item. The default
        streams complete multi-GB files in 1 MiB chunks, bounding probe memory
        without truncating content identity.
    """

    def __init__(
        self,
        *,
        content_items: Callable[[], Iterable[tuple[Any, ContentItem]]],
        config: Mapping[str, Any],
        emit_targets: Any,
        max_bytes_per_item: Optional[int] = None,
        read_chunk_bytes: int = _DEFAULT_READ_CHUNK_BYTES,
    ) -> None:
        self._content_items = content_items
        self._config_hash = _hash_config(config)
        self._emit_version = emit_version_for(*_as_tuple(emit_targets))
        self._max_bytes = max_bytes_per_item
        self._read_chunk_bytes = max(1, int(read_chunk_bytes))
        self._last_item_hashes: Optional[dict[str, str]] = None
        self._last_item_sizes: Optional[dict[str, int]] = None

    @property
    def emit_version(self) -> str:
        """Return the machine-derived emit version cached at construction."""
        return self._emit_version

    def _content_component(self) -> dict[str, Any]:
        # A failed rebuild must never leave the previous successful manifest
        # available to a composite probe's adapter integration.
        self._last_item_hashes = None
        self._last_item_sizes = None
        items = sorted(
            ((str(key), src) for key, src in self._content_items()),
            key=lambda kv: kv[0],
        )
        digest = hashlib.sha256()
        item_hashes: dict[str, str] = {}
        item_sizes: dict[str, int] = {}
        # Encode membership (count) so an add/delete is caught even if some other
        # change happened to keep the per-item shas the same.
        digest.update(f"n={len(items)}".encode("utf-8"))
        for key, src in items:
            if key in item_hashes:
                self._last_item_hashes = None
                self._last_item_sizes = None
                raise ChangeProbeError(
                    "content item keys must be unique within the probe scope"
                )
            item_hash, item_size = _item_sha_and_size(
                src,
                max_bytes=self._max_bytes,
                read_chunk_bytes=self._read_chunk_bytes,
            )
            item_hashes[key] = item_hash
            item_sizes[key] = item_size
            digest.update(b"\x00")
            digest.update(key.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(item_hash.encode("utf-8"))
        self._last_item_hashes = item_hashes
        self._last_item_sizes = item_sizes
        return {"kind": "content_hash", "value": digest.hexdigest()}

    def last_item_hashes(self) -> Optional[dict[str, str]]:
        """Return the per-item identities from the most recent successful build.

        The mapping is deliberately not embedded in the persisted probe token:
        the aggregate digest is sufficient for equality, while source adapters
        can use this ephemeral detail to invalidate only changed reader cursors.
        ``None`` means no successful complete read is available, so callers
        must conservatively invalidate rather than assume an unchanged item.
        """
        if self._last_item_hashes is None:
            return None
        return dict(self._last_item_hashes)

    def last_item_manifest(self) -> Optional[dict[str, Any]]:
        """Return source-owned cursor detail for selective reader invalidation."""
        if self._last_item_hashes is None:
            return None
        return {
            "schema": _ITEM_MANIFEST_SCHEMA,
            "config_sha256": self._config_hash,
            "emit_version_sha256": self._emit_version,
            "items": dict(self._last_item_hashes),
            "sizes": dict(self._last_item_sizes or {}),
        }

    def manifest_from_item_hashes(
        self,
        item_hashes: Mapping[str, str],
        *,
        item_sizes: Optional[Mapping[str, int]] = None,
    ) -> dict[str, Any]:
        """Build cursor detail from the exact item bytes an adapter captured."""
        return {
            "schema": _ITEM_MANIFEST_SCHEMA,
            "config_sha256": self._config_hash,
            "emit_version_sha256": self._emit_version,
            "items": {
                str(key): str(value)
                for key, value in item_hashes.items()
            },
            "sizes": {
                str(key): int(value)
                for key, value in (item_sizes or {}).items()
            },
        }

    def build_token(self) -> dict[str, Any]:
        return compose_token(
            content=self._content_component(),
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def token_from_item_hashes(
        self,
        item_hashes: Mapping[str, str],
    ) -> dict[str, Any]:
        """Compose a token from the exact bytes an adapter consumed.

        Adapters use this after parsing their captured bytes, avoiding a
        second filesystem read and its pre/post ABA gap.
        """
        items = sorted((str(key), str(value)) for key, value in item_hashes.items())
        digest = hashlib.sha256()
        digest.update(f"n={len(items)}".encode("utf-8"))
        for key, item_hash in items:
            digest.update(b"\x00")
            digest.update(key.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(item_hash.encode("utf-8"))
        return compose_token(
            content={"kind": "content_hash", "value": digest.hexdigest()},
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def probe(
        self,
        *,
        cursor: Optional[Mapping[str, Any]],
        sync_scope: Optional[Mapping[str, Any]],
    ) -> ProbeVerdict:
        prior = (
            cursor.get(CURSOR_TOKEN_KEY)
            if isinstance(cursor, Mapping)
            else None
        )
        try:
            token = self.build_token()
        except Exception as exc:  # noqa: BLE001 — probe errors force a full read
            self._last_item_hashes = None
            self._last_item_sizes = None
            return ProbeVerdict(
                changed=True,
                token={},
                reason=f"probe_error:{type(exc).__name__}",
            )
        if prior is None:
            return ProbeVerdict(changed=True, token=token, reason="no_prior_token")
        if cursor_has_outstanding_work(cursor):
            return ProbeVerdict(
                changed=True,
                token=token,
                reason="outstanding_backlog",
            )
        changed = not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="content_changed" if changed else "content_unchanged",
        )


def file_content_items(
    root: Any,
    *,
    include_globs: Iterable[str] = ("*",),
    exclude_globs: Iterable[str] = (),
) -> list[tuple[str, Path]]:
    """Enumerate the CURRENT files under ``root`` matching the globs.

    Returns ``(rel_path, abs_path)`` pairs. Globs match the repo-relative posix
    path via fnmatch, whose ``*`` spans ``/`` (so ``*.py`` matches nested files).
    A deleted file is simply absent from the next call — that absence is how the
    ContentHashProbe detects the deletion.
    """
    root = Path(root)
    include = tuple(include_globs) or ("*",)
    exclude = tuple(exclude_globs)
    out: list[tuple[str, Path]] = []
    if not root.is_dir():
        return out
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if not any(fnmatch.fnmatch(rel, g) for g in include):
            continue
        if any(fnmatch.fnmatch(rel, g) for g in exclude):
            continue
        out.append((rel, path))
    return out


def _as_tuple(targets: Any) -> tuple[Any, ...]:
    if isinstance(targets, (list, tuple)):
        return tuple(targets)
    return (targets,)


def _hash_config(config: Mapping[str, Any]) -> str:
    import json

    payload = json.dumps(dict(config), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _item_sha(
    src: ContentItem,
    *,
    max_bytes: Optional[int],
    read_chunk_bytes: int,
) -> str:
    return _item_sha_and_size(
        src,
        max_bytes=max_bytes,
        read_chunk_bytes=read_chunk_bytes,
    )[0]


def _item_sha_and_size(
    src: ContentItem,
    *,
    max_bytes: Optional[int],
    read_chunk_bytes: int,
) -> tuple[str, int]:
    if callable(src):
        try:
            data = src()
        except Exception as exc:  # noqa: BLE001 — unknown identity must force a read
            raise ChangeProbeError(
                "content item callable could not be read"
            ) from exc
    elif isinstance(src, (str, Path)):
        return _stream_file_sha_and_size(
            src,
            max_bytes=max_bytes,
            read_chunk_bytes=read_chunk_bytes,
        )
    else:
        data = src
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise ChangeProbeError("content item did not provide bytes")
    view = memoryview(data)
    if max_bytes is not None:
        view = view[:max_bytes]
    return hashlib.sha256(view).hexdigest(), len(view)


def _stream_file_sha(
    path: Union[str, Path],
    *,
    max_bytes: Optional[int],
    read_chunk_bytes: int,
) -> str:
    return _stream_file_sha_and_size(
        path,
        max_bytes=max_bytes,
        read_chunk_bytes=read_chunk_bytes,
    )[0]


def _stream_file_sha_and_size(
    path: Union[str, Path],
    *,
    max_bytes: Optional[int],
    read_chunk_bytes: int,
) -> tuple[str, int]:
    """Hash a path completely using bounded reads.

    A read failure is not an ``absent`` content identity: the path was in the
    enumerated membership, but its bytes are unknown. Raising a sanitized
    ``ChangeProbeError`` lets ``run_probe`` conservatively force the full
    reader without leaking the path or exception payload into its reason.
    """
    try:
        first = _read_file_digest_pass(
            path,
            max_bytes=max_bytes,
            read_chunk_bytes=read_chunk_bytes,
        )
        _require_consistent_file_pass(first)
        _verify_recent_file_pass(
            path,
            first,
            max_bytes=max_bytes,
            read_chunk_bytes=read_chunk_bytes,
        )
    except ChangeProbeError:
        raise
    except Exception as exc:  # noqa: BLE001 — unknown identity must force a read
        raise ChangeProbeError("content item path could not be read") from exc
    return first.digest, first.bytes_read


def _read_file_digest_pass(
    path: Union[str, Path],
    *,
    max_bytes: Optional[int],
    read_chunk_bytes: int,
) -> _FileDigestPass:
    digest = hashlib.sha256()
    remaining = max_bytes
    bytes_read = 0
    started_ns = time.time_ns()
    with open(path, "rb") as handle:
        before = os.fstat(handle.fileno())
        while remaining is None or remaining > 0:
            request_bytes = (
                read_chunk_bytes
                if remaining is None
                else min(read_chunk_bytes, remaining)
            )
            chunk = handle.read(request_bytes)
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise ChangeProbeError(
                    "content item path did not provide bytes"
                )
            digest.update(chunk)
            bytes_read += len(chunk)
            if remaining is not None:
                remaining -= len(chunk)
        after = os.fstat(handle.fileno())
    current = os.stat(path)
    expected_bytes = (
        before.st_size
        if max_bytes is None else min(before.st_size, max_bytes)
    )
    return _FileDigestPass(
        digest=digest.hexdigest(),
        bytes_read=bytes_read,
        expected_bytes=int(expected_bytes),
        before_identity=_file_stat_identity(before),
        after_identity=_file_stat_identity(after),
        current_identity=_file_stat_identity(current),
        before_ctime_ns=int(before.st_ctime_ns),
        started_ns=started_ns,
    )


def _require_consistent_file_pass(scan: _FileDigestPass) -> None:
    if (
        scan.before_identity != scan.after_identity
        or scan.after_identity != scan.current_identity
        or scan.bytes_read != scan.expected_bytes
    ):
        raise ChangeProbeError(
            "content item changed while identity was being read"
        )


def _verify_recent_file_pass(
    path: Union[str, Path],
    first: _FileDigestPass,
    *,
    max_bytes: Optional[int],
    read_chunk_bytes: int,
) -> None:
    """Re-read only when a same-tick metadata collision is possible."""
    if (
        first.before_ctime_ns
        < first.started_ns - _STAT_TIMESTAMP_COLLISION_WINDOW_NS
    ):
        return
    second = _read_file_digest_pass(
        path,
        max_bytes=max_bytes,
        read_chunk_bytes=read_chunk_bytes,
    )
    _require_consistent_file_pass(second)
    if (
        first.current_identity != second.before_identity
        or first.bytes_read != second.bytes_read
        or first.digest != second.digest
    ):
        raise ChangeProbeError(
            "content item changed while identity was being read"
        )


def _file_stat_identity(stat: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        int(stat.st_dev),
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
        int(stat.st_ctime_ns),
    )


def file_prefix_sha256(
    path: Union[str, Path],
    size: int,
    *,
    read_chunk_bytes: int = _DEFAULT_READ_CHUNK_BYTES,
) -> Optional[str]:
    """Hash exactly ``size`` prefix bytes with bounded memory.

    ``None`` means the prefix could not be read completely, including when the
    file is shorter than the requested prefix. Callers use that as a
    conservative "cannot resume" result.
    """
    prefix_size = max(0, int(size))
    try:
        first = _read_file_digest_pass(
            path,
            max_bytes=prefix_size,
            read_chunk_bytes=max(1, int(read_chunk_bytes)),
        )
        _require_consistent_file_pass(first)
        if first.bytes_read != prefix_size:
            return None
        _verify_recent_file_pass(
            path,
            first,
            max_bytes=prefix_size,
            read_chunk_bytes=max(1, int(read_chunk_bytes)),
        )
    except Exception:  # noqa: BLE001 — callers conservatively decline resume
        return None
    return first.digest
