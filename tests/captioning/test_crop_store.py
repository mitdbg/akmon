"""Tests for the content-addressed figure-crop blob store
(``okg.visual_crop_blobs``), task.crop-store of the visual-caption-enricher
PACT (req.crop-persistence).

The store lets the source stage a figure crop at ingest (Phase A) and the
VisualCaptionEnricher read the exact bytes back at enrich (Phase B) without
re-opening the PDF. These are real-DB round-trip proofs against the migrated
test schema.
"""
from __future__ import annotations

import hashlib

import psycopg
import pytest

from okg.substrate.library.captioning import crop_store

# Real-DB round trips through `clean_runtime`: the fast lane runs
# tests/captioning with `not postgres` and has no Postgres service.
pytestmark = pytest.mark.postgres


def _png_like(seed: bytes) -> bytes:
    # Not a real PNG — the store is byte-agnostic; content-addressing is all
    # that matters here.
    return b"\x89PNG\r\n\x1a\n" + seed


def test_put_get_roundtrip_and_ensure_idempotent(clean_runtime: str) -> None:
    data = _png_like(b"figure-one")
    digest = hashlib.sha256(data).hexdigest()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # ensure_table is idempotent: calling it twice must not raise.
        crop_store.ensure_table(conn)
        crop_store.ensure_table(conn)

        crop_store.put_crop(conn, sha256=digest, data=data)
        # Re-staging identical bytes is a no-op, not an error.
        crop_store.put_crop(conn, sha256=digest, data=data)

        got = crop_store.get_crop(conn, digest)
        assert got == data
        # The retrieved bytes provably hash to the key the node would carry
        # as render_sha256.
        assert hashlib.sha256(got).hexdigest() == digest


def test_get_missing_raises_loud(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        crop_store.ensure_table(conn)
        with pytest.raises(crop_store.CropBlobMissing):
            crop_store.get_crop(conn, "0" * 64)


def test_put_rejects_content_address_mismatch(clean_runtime: str) -> None:
    data = _png_like(b"real-bytes")
    wrong_key = hashlib.sha256(b"other").hexdigest()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        crop_store.ensure_table(conn)
        with pytest.raises(ValueError):
            crop_store.put_crop(conn, sha256=wrong_key, data=data)
