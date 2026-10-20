"""Detached-object store scope + size limits (enforce-mailbox-poros-authz).

A detached-object pointer is attacker-influenceable. S3/Swift fetch_bytes must
constrain the fetch to the store's configured bucket/container + prefix and
refuse an oversized object before materializing it, instead of re-parsing the
pointer URI and reading any bucket/key the service credentials can reach.
"""
from __future__ import annotations

import pytest

from okg.substrate.federation.mailbox.models import MailboxValidationError
from okg.substrate.federation.mailbox.object_store import (
    FileObjectStore,
    S3ObjectStore,
    SwiftObjectStore,
)


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            d, self._data = self._data, b""
            return d
        d, self._data = self._data[:n], self._data[n:]
        return d


class _FakeS3:
    def __init__(self, data: bytes, *, content_length: int | None = None) -> None:
        self.data = data
        self.content_length = (
            content_length if content_length is not None else len(data)
        )
        self.calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803 - boto3 kwargs
        self.calls.append((Bucket, Key))
        return {"Body": _FakeBody(self.data), "ContentLength": self.content_length}


class _FakeSwift:
    def __init__(self, data: bytes, *, content_length: int | None = None) -> None:
        self.data = data
        self.content_length = (
            content_length if content_length is not None else len(data)
        )
        self.calls: list[tuple[str, str]] = []

    def get_object(self, container: str, key: str, *, resp_chunk_size=None):
        self.calls.append((container, key))
        headers = {"content-length": str(self.content_length)}
        if resp_chunk_size:
            # swiftclient streams chunks when resp_chunk_size is set — exercise
            # the production streaming path so the cap is enforced on real bytes.
            def _chunks():
                for i in range(0, len(self.data), resp_chunk_size):
                    yield self.data[i:i + resp_chunk_size]
            return (headers, _chunks())
        return (headers, self.data)


# ---- S3 scope ----


def test_s3_in_scope_uri_reads_once():
    c = _FakeS3(b"ok-bytes")
    s = S3ObjectStore(bucket="okg-mailbox", prefix="poros", client=c)
    assert s.fetch_bytes("s3://okg-mailbox/poros/ok.bin") == b"ok-bytes"
    assert len(c.calls) == 1


def test_s3_out_of_bucket_rejected_without_read():
    c = _FakeS3(b"secret")
    s = S3ObjectStore(bucket="okg-mailbox", prefix="poros", client=c)
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("s3://other-bucket/poros/secret.bin")
    assert c.calls == []


def test_s3_out_of_prefix_rejected_without_read():
    c = _FakeS3(b"x")
    s = S3ObjectStore(bucket="okg-mailbox", prefix="poros", client=c)
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("s3://okg-mailbox/elsewhere/x.bin")
    assert c.calls == []


def test_s3_oversized_detached_object_rejected():
    c = _FakeS3(b"x" * 100, content_length=100)
    s = S3ObjectStore(
        bucket="okg-mailbox", prefix="poros", client=c, max_object_bytes=10,
    )
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("s3://okg-mailbox/poros/big.bin")


# ---- Swift scope ----


def test_swift_in_scope_uri_reads_once():
    c = _FakeSwift(b"ok")
    s = SwiftObjectStore(container="okg-mailbox", prefix="poros", connection=c)
    assert s.fetch_bytes("swift://okg-mailbox/poros/ok.bin") == b"ok"
    assert len(c.calls) == 1


def test_swift_out_of_container_rejected_without_read():
    c = _FakeSwift(b"secret")
    s = SwiftObjectStore(container="okg-mailbox", prefix="poros", connection=c)
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("swift://other-container/poros/secret.bin")
    assert c.calls == []


def test_swift_out_of_prefix_rejected_without_read():
    c = _FakeSwift(b"x")
    s = SwiftObjectStore(container="okg-mailbox", prefix="poros", connection=c)
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("swift://okg-mailbox/elsewhere/x.bin")
    assert c.calls == []


def test_swift_oversized_detached_object_rejected():
    c = _FakeSwift(b"x" * 100, content_length=100)
    s = SwiftObjectStore(
        container="okg-mailbox", prefix="poros", connection=c, max_object_bytes=10,
    )
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("swift://okg-mailbox/poros/big.bin")


def test_swift_lying_content_length_is_still_bounded():
    # A LYING small content-length header must not let a large body through:
    # the cap is enforced on the actual streamed bytes, not the header (the prior
    # code read the whole body before checking len()).
    c = _FakeSwift(b"x" * 100, content_length=5)  # header lies: says 5, sends 100
    s = SwiftObjectStore(
        container="okg-mailbox", prefix="poros", connection=c, max_object_bytes=10,
    )
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes("swift://okg-mailbox/poros/big.bin")


def test_swift_in_scope_streams_small_object():
    c = _FakeSwift(b"hello")
    s = SwiftObjectStore(container="okg-mailbox", prefix="poros", connection=c)
    assert s.fetch_bytes("swift://okg-mailbox/poros/ok.bin") == b"hello"


# ---- FileObjectStore byte cap (the CLI-wired default store) ----


def test_file_store_reads_in_scope_object(tmp_path):
    s = FileObjectStore(tmp_path)
    stored = s.put_bytes("obj-1", b"payload")
    assert s.fetch_bytes(stored.uri) == b"payload"


def test_file_store_oversized_object_rejected(tmp_path):
    s = FileObjectStore(tmp_path, max_object_bytes=8)
    stored = s.put_bytes("big", b"x" * 64)
    with pytest.raises(MailboxValidationError):
        s.fetch_bytes(stored.uri)
