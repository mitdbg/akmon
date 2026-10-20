"""Content-addressed byte store for figure crops (``okg.visual_crop_blobs``).

Why this exists: the :class:`VisualCaptionEnricher` captions published
``pdf_figure`` ``visual_artifact`` nodes in Phase B, but the substrate never
persisted the crop bytes — a crop was cropped at ingest, sent to the VLM, and
discarded. To decouple figure *discovery* (the source, Phase A) from
*captioning* (the enricher, Phase B) WITHOUT re-opening the PDF in the enricher,
``DocCorpusSource`` stages each crop PNG here at discovery time, keyed by the
crop's own ``render_sha256``; the enricher reads the bytes back by that key over
its own connection.

The store is content-addressed: the primary key is ``sha256(bytes)``. This is
the same shape as ``okg.source_cursor_blobs`` (content-addressed source cursor
state) and lets :func:`put_crop` self-verify — a caller cannot register bytes
under a key that is not their hash, so a later ``get_crop`` provably returns the
exact crop whose hash the ``pdf_figure`` node recorded as ``render_sha256``.

The table is DECLARED in ``schema.sql`` (alongside ``okg.source_cursor_blobs``)
and granted to ``app_rw`` there — so it is part of the canonical schema, visible
to schema checks / backup tooling, and accessible when the discovery source and
the caption enricher run as different database roles. :func:`ensure_table` still
issues an idempotent ``CREATE TABLE IF NOT EXISTS`` as a belt-and-suspenders for
databases provisioned before this table was added to the schema; it is a no-op
where the schema already declares the table. GC of orphaned crops (a superseded
visual leaves its blob behind) is out of scope here and tracked in the
``automate-source-blob-gc`` family.
"""
from __future__ import annotations

import hashlib
from typing import Protocol

import psycopg

__all__ = [
    "TABLE",
    "CropBlobMissing",
    "ensure_table",
    "put_crop",
    "get_crop",
    "CropStoreWriter",
    "DsnCropStore",
    "InMemoryCropStore",
]

TABLE = "okg.visual_crop_blobs"


class CropBlobMissing(Exception):
    """A crop referenced by a ``pdf_figure`` node is absent from
    ``okg.visual_crop_blobs``.

    Raised by :func:`get_crop`. This is a loud failure, never a silent
    empty caption: a ``crop_blob_ref`` on a published visual whose bytes
    are gone means the enricher cannot honor the caption it was asked to
    produce (project P0: no default-value-as-fallback).
    """

    def __init__(self, sha256: str) -> None:
        super().__init__(
            f"visual crop blob {sha256!r} is absent from {TABLE}; the "
            f"pdf_figure node references a crop that was never staged (or "
            f"was GC'd out from under an in-flight caption)."
        )
        self.sha256 = sha256


def ensure_table(conn: psycopg.Connection) -> None:
    """Create ``okg.visual_crop_blobs`` if it does not exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.visual_crop_blobs (
              sha256 TEXT PRIMARY KEY,
              bytes BYTEA NOT NULL,
              size_bytes BIGINT NOT NULL,
              mime_type TEXT NOT NULL DEFAULT 'image/png',
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )


def put_crop(
    conn: psycopg.Connection,
    *,
    sha256: str,
    data: bytes,
    mime_type: str = "image/png",
) -> None:
    """Stage one crop's bytes under its content hash.

    ``sha256`` MUST equal ``hashlib.sha256(data).hexdigest()`` — the store
    is content-addressed and refuses a mismatched key loudly rather than
    persisting bytes the enricher would later fetch and mis-attribute.
    Re-staging identical bytes is a no-op (``ON CONFLICT DO NOTHING``).
    Callers are expected to :func:`ensure_table` first.
    """
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        raise ValueError(
            f"crop_store.put_crop: content-address mismatch — declared "
            f"sha256={sha256!r} but sha256(data)={actual!r}. The store key "
            f"must be the bytes' own hash."
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.visual_crop_blobs
              (sha256, bytes, size_bytes, mime_type, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (sha256) DO NOTHING
            """,
            (sha256, data, len(data), mime_type),
        )


def get_crop(conn: psycopg.Connection, sha256: str) -> bytes:
    """Return the crop bytes for ``sha256``; raise :class:`CropBlobMissing`
    if absent. Never returns a placeholder (project P0: no silent fallback)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT bytes FROM okg.visual_crop_blobs WHERE sha256 = %s",
            (sha256,),
        )
        row = cur.fetchone()
    if row is None:
        raise CropBlobMissing(sha256)
    return bytes(row[0])


# ---------------------------------------------------------------------------
# Source-side writers
# ---------------------------------------------------------------------------
# DocCorpusSource stages crops through one of these at discovery. The writer
# abstraction mirrors the injectable `figure_detector`: production uses a
# `DsnCropStore` bound to the source dsn; unit tests inject an
# `InMemoryCropStore` so they need no Postgres.


class CropStoreWriter(Protocol):
    """A sink DocCorpusSource stages figure crops into at discovery time."""

    def put(self, sha256: str, data: bytes) -> None: ...


class DsnCropStore:
    """Content-addressed crop writer backed by ``okg.visual_crop_blobs`` over a
    pooled connection to ``dsn``.

    A falsy ``dsn`` is a loud construction error: captioning-via-enricher
    cannot stage crops without a database, and a silent no-op would leave the
    published visuals uncaptionable (project P0: no default-value-as-fallback).
    """

    def __init__(self, dsn: str | None) -> None:
        if not dsn:
            raise ValueError(
                "DsnCropStore requires a source dsn to stage figure crops for "
                "the caption enricher; none was configured."
            )
        self._dsn = dsn
        self._ensured = False

    def put(self, sha256: str, data: bytes) -> None:
        from okg.substrate.db.pool import GENERAL, pooled_connection

        with pooled_connection(self._dsn, purpose=GENERAL) as conn:
            if not self._ensured:
                ensure_table(conn)
                self._ensured = True
            put_crop(conn, sha256=sha256, data=data)


class InMemoryCropStore:
    """Dict-backed crop store for unit tests (no Postgres).

    Enforces the same content-addressing invariant as :func:`put_crop` so a
    test cannot register bytes under the wrong key.
    """

    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}

    def put(self, sha256: str, data: bytes) -> None:
        actual = hashlib.sha256(data).hexdigest()
        if actual != sha256:
            raise ValueError(
                f"InMemoryCropStore: content-address mismatch — declared "
                f"{sha256!r} but sha256(data)={actual!r}."
            )
        self._blobs[sha256] = data

    def get(self, sha256: str) -> bytes:
        if sha256 not in self._blobs:
            raise CropBlobMissing(sha256)
        return self._blobs[sha256]
