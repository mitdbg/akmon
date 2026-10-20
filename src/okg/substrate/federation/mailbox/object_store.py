"""Provider-neutral detached-object storage for Poros mailbox transport."""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import unquote, urlparse

from okg.substrate.federation.mailbox.models import (
    DetachedObjectPointer,
    MailboxValidationError,
)

#: Default ceiling on a single detached-object fetch (64 MiB). A malicious
#: pointer cannot cause an unbounded read; stores raise before materializing a
#: payload larger than this (per enforce-mailbox-poros-authz).
DEFAULT_MAX_OBJECT_BYTES = 64 * 1024 * 1024


def _assert_key_in_prefix(key: str, prefix: str, *, kind: str, scope: str) -> None:
    """Raise unless `key` is within the configured `prefix` (empty prefix => any
    key in the already-scoped container/bucket)."""
    if not prefix:
        return
    allowed = f"{prefix}/"
    if key != prefix and not key.startswith(allowed):
        raise MailboxValidationError(
            f"detached object key {key!r} is outside the configured {kind} "
            f"prefix {prefix!r} for {scope!r}"
        )


@dataclass(frozen=True)
class StoredObject:
    uri: str
    sha256: str
    size_bytes: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_pointer_fields(self) -> dict[str, Any]:
        return {
            "object_uri": self.uri,
            "object_sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "metadata": dict(self.metadata),
        }


class ObjectStore(Protocol):
    """Minimal object-store contract for detached Poros mailbox objects."""

    def put_bytes(
        self,
        object_id: str,
        data: bytes,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoredObject:
        """Store bytes and return a content-addressable pointer."""

    def fetch_bytes(self, uri: str) -> bytes:
        """Fetch bytes by URI. Callers must verify size and hash."""


class MemoryObjectStore:
    """In-process object store for tests and local demos."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}
        self._metadata: dict[str, dict[str, Any]] = {}

    def put_bytes(
        self,
        object_id: str,
        data: bytes,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoredObject:
        digest = _sha256_b64url(data)
        uri = f"memory://mailbox/{_safe_object_name(object_id, digest)}"
        self._objects[uri] = bytes(data)
        self._metadata[uri] = dict(metadata or {})
        return StoredObject(
            uri=uri,
            sha256=digest,
            size_bytes=len(data),
            metadata=dict(metadata or {}),
        )

    def fetch_bytes(self, uri: str) -> bytes:
        try:
            return self._objects[uri]
        except KeyError as exc:
            raise MailboxValidationError(
                f"detached object unavailable: {uri}"
            ) from exc


class FileObjectStore:
    """Filesystem-backed object store for single-node demos."""

    def __init__(
        self,
        root: Path | str,
        *,
        max_object_bytes: int = DEFAULT_MAX_OBJECT_BYTES,
    ) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        # Cap reads so a large (or symlink-swapped) on-disk object cannot be
        # slurped unbounded. This is the store the CLI serve path wires by
        # default, so it must carry the same byte limit as S3/Swift.
        self.max_object_bytes = max_object_bytes

    def put_bytes(
        self,
        object_id: str,
        data: bytes,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoredObject:
        digest = _sha256_b64url(data)
        object_name = _safe_object_name(object_id, digest)
        path = (self.root / object_name).resolve()
        if not _is_under(path, self.root):
            raise MailboxValidationError("detached object path escapes store root")
        path.write_bytes(data)
        return StoredObject(
            uri=f"poros-file://{object_name}",
            sha256=digest,
            size_bytes=len(data),
            metadata=dict(metadata or {}),
        )

    def fetch_bytes(self, uri: str) -> bytes:
        parsed = urlparse(uri)
        if parsed.scheme == "file":
            path = Path(unquote(parsed.path)).resolve()
        elif parsed.scheme == "poros-file":
            path = (self.root / unquote(parsed.netloc + parsed.path)).resolve()
        else:
            raise MailboxValidationError(
                f"unsupported file object URI scheme: {parsed.scheme}"
            )
        if not _is_under(path, self.root):
            raise MailboxValidationError("detached object path escapes store root")
        try:
            # Reject by stat() first (cheap), then read with a hard cap so the
            # size cannot grow between the stat and the read (TOCTOU).
            if path.stat().st_size > self.max_object_bytes:
                raise MailboxValidationError(
                    f"detached object exceeds max_object_bytes "
                    f"({self.max_object_bytes}): {uri}"
                )
            with path.open("rb") as fh:
                data = fh.read(self.max_object_bytes + 1)
        except OSError as exc:
            raise MailboxValidationError(
                f"detached object unavailable: {uri}"
            ) from exc
        if len(data) > self.max_object_bytes:
            raise MailboxValidationError(
                f"detached object exceeds max_object_bytes "
                f"({self.max_object_bytes}): {uri}"
            )
        return data


class S3ObjectStore:
    """S3-compatible object-store adapter.

    Works with AWS S3, Ceph RGW, MinIO, and OpenStack deployments that expose
    an S3-compatible endpoint. ``boto3`` is imported lazily.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        client: Any | None = None,
        max_object_bytes: int = DEFAULT_MAX_OBJECT_BYTES,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.max_object_bytes = max_object_bytes
        if client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - optional extra.
                raise RuntimeError(
                    "boto3 is required for S3ObjectStore; install okg[mailbox]"
                ) from exc
            client = boto3.client("s3")
        self.client = client

    def put_bytes(
        self,
        object_id: str,
        data: bytes,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoredObject:
        digest = _sha256_b64url(data)
        key = self._key(object_id, digest)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            Metadata={str(k): str(v) for k, v in dict(metadata or {}).items()},
        )
        return StoredObject(
            uri=f"s3://{self.bucket}/{key}",
            sha256=digest,
            size_bytes=len(data),
            metadata=dict(metadata or {}),
        )

    def fetch_bytes(self, uri: str) -> bytes:
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise MailboxValidationError(
                f"unsupported S3 object URI scheme: {parsed.scheme}"
            )
        bucket = parsed.netloc
        key = unquote(parsed.path.lstrip("/"))
        if not bucket or not key:
            raise MailboxValidationError("S3 object URI must include bucket and key")
        # Scope the fetch to the configured bucket + prefix: a crafted pointer
        # naming another bucket (or a key outside the prefix) must be rejected
        # BEFORE any credentialed get_object, so a malicious pointer cannot read
        # arbitrary buckets the store's credentials can reach.
        if bucket != self.bucket:
            raise MailboxValidationError(
                f"detached object bucket {bucket!r} is outside the configured "
                f"store bucket {self.bucket!r}"
            )
        _assert_key_in_prefix(key, self.prefix, kind="bucket", scope=self.bucket)
        response = self.client.get_object(Bucket=bucket, Key=key)
        _assert_object_size(
            response.get("ContentLength"), self.max_object_bytes, uri=uri,
        )
        body = response["Body"].read(self.max_object_bytes + 1)
        if len(body) > self.max_object_bytes:
            raise MailboxValidationError(
                f"detached object exceeds max_object_bytes "
                f"({self.max_object_bytes}): {uri}"
            )
        return body

    def _key(self, object_id: str, digest: str) -> str:
        name = _safe_object_name(object_id, digest)
        return f"{self.prefix}/{name}" if self.prefix else name


class SwiftObjectStore:
    """OpenStack Swift object-store adapter.

    The adapter accepts an already-configured swiftclient connection or creates
    one lazily from environment/auth kwargs. The provider library is optional.
    """

    def __init__(
        self,
        *,
        container: str,
        prefix: str = "",
        connection: Any | None = None,
        max_object_bytes: int = DEFAULT_MAX_OBJECT_BYTES,
        **auth_kwargs: Any,
    ) -> None:
        self.container = container
        self.prefix = prefix.strip("/")
        self.max_object_bytes = max_object_bytes
        if connection is None:
            try:
                from swiftclient.client import Connection
            except ImportError as exc:  # pragma: no cover - optional extra.
                raise RuntimeError(
                    "python-swiftclient is required for SwiftObjectStore; "
                    "install okg[mailbox]"
                ) from exc
            connection = Connection(**auth_kwargs)
        self.connection = connection

    def put_bytes(
        self,
        object_id: str,
        data: bytes,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> StoredObject:
        digest = _sha256_b64url(data)
        key = self._key(object_id, digest)
        headers = {
            f"X-Object-Meta-{k}": str(v)
            for k, v in dict(metadata or {}).items()
        }
        self.connection.put_object(
            self.container,
            key,
            contents=data,
            headers=headers,
        )
        return StoredObject(
            uri=f"swift://{self.container}/{key}",
            sha256=digest,
            size_bytes=len(data),
            metadata=dict(metadata or {}),
        )

    def fetch_bytes(self, uri: str) -> bytes:
        parsed = urlparse(uri)
        if parsed.scheme != "swift":
            raise MailboxValidationError(
                f"unsupported Swift object URI scheme: {parsed.scheme}"
            )
        container = parsed.netloc
        key = unquote(parsed.path.lstrip("/"))
        if not container or not key:
            raise MailboxValidationError(
                "Swift object URI must include container and object name"
            )
        # Scope to the configured container + prefix before any credentialed read.
        if container != self.container:
            raise MailboxValidationError(
                f"detached object container {container!r} is outside the "
                f"configured store container {self.container!r}"
            )
        _assert_key_in_prefix(
            key, self.prefix, kind="container", scope=self.container,
        )
        # Stream with resp_chunk_size so swiftclient does NOT buffer the whole
        # body before we can bound it; the cap is then enforced on the actual
        # bytes read, not the attacker-influenceable content-length header (which
        # is only an early-reject advisory). A test stub that ignores the kwarg
        # and returns bytes is handled by _read_capped too.
        headers, body = self.connection.get_object(
            container, key, resp_chunk_size=self.max_object_bytes + 1,
        )
        _assert_object_size(
            headers.get("content-length") if isinstance(headers, dict) else None,
            self.max_object_bytes,
            uri=uri,
        )
        return _read_capped(body, self.max_object_bytes, uri=uri)

    def _key(self, object_id: str, digest: str) -> str:
        name = _safe_object_name(object_id, digest)
        return f"{self.prefix}/{name}" if self.prefix else name


def fetch_and_verify_pointer(
    pointer: DetachedObjectPointer,
    object_store: ObjectStore,
) -> bytes:
    data = object_store.fetch_bytes(pointer.object_uri)
    verify_pointer_bytes(pointer, data)
    return data


def verify_pointer_bytes(pointer: DetachedObjectPointer, data: bytes) -> None:
    if len(data) != pointer.size_bytes:
        raise MailboxValidationError(
            "detached object size does not match mailbox pointer"
        )
    actual = _sha256_b64url(data)
    expected = _normalize_sha256(pointer.object_sha256)
    if actual != expected:
        raise MailboxValidationError(
            "detached object hash does not match mailbox pointer"
        )


def _assert_object_size(content_length: Any, max_bytes: int, *, uri: str) -> None:
    """Raise if a declared content length already exceeds the cap, before the
    body is materialized/read."""
    if content_length is None:
        return
    try:
        declared = int(content_length)
    except (TypeError, ValueError):
        return
    if declared > max_bytes:
        raise MailboxValidationError(
            f"detached object declares {declared} bytes, exceeding "
            f"max_object_bytes ({max_bytes}): {uri}"
        )


def _read_capped(body: Any, max_bytes: int, *, uri: str) -> bytes:
    """Materialize an object body without exceeding `max_bytes`.

    `body` may be a bytes blob (already in memory) or a streaming iterator of
    chunks (swiftclient with `resp_chunk_size`). The cap is enforced on the
    ACTUAL bytes — never trusting the attacker-influenceable content-length
    header — so a lying header cannot force an unbounded read."""
    if isinstance(body, (bytes, bytearray, memoryview)):
        data = bytes(body)
        if len(data) > max_bytes:
            raise MailboxValidationError(
                f"detached object exceeds max_object_bytes ({max_bytes}): {uri}"
            )
        return data
    buf = bytearray()
    for chunk in body:
        if not chunk:
            continue
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise MailboxValidationError(
                f"detached object exceeds max_object_bytes ({max_bytes}): {uri}"
            )
    return bytes(buf)


def _normalize_sha256(value: str) -> str:
    return value.removeprefix("sha256:")


def _sha256_b64url(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _safe_object_name(object_id: str, digest: str) -> str:
    stem = "".join(c if c.isalnum() or c in {"-", "_", "."} else "_" for c in object_id)
    stem = stem.strip("._") or "object"
    return f"{stem}-{digest}.bin"


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False
