"""Stream a source record set as JSON and store it in bounded pieces.

Issue #3008. The record-set authority used to be one JSON value in one
INSERT; past about 1 GB PostgreSQL refuses the message. This module holds the
pure parts of the pieced form so they can be tested without a database:

- ``iter_record_set_json`` yields the exact text
  ``json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))``
  would produce, a record at a time, without building the whole string or
  the whole ``RecordSet.to_cursor()`` dict;
- ``iter_pieces`` groups those chunks into consecutive pieces of at most a
  byte bound;
- ``reassemble_pieces`` joins stored pieces back and refuses anything but the
  exact pieces the header describes.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import RecordSet

#: Marker stored in a pieced header's ``record_set`` JSONB. It is not a valid
#: record-set cursor, so code that predates pieces and reads the JSONB column
#: refuses the row as invalid instead of reading an empty prior record set.
PIECED_RECORD_SET_FORMAT = "okg.source_runner.record_set_pieces.v1"
PIECED_RECORD_SET_MARKER: dict[str, str] = {
    "pieced_record_set": PIECED_RECORD_SET_FORMAT,
}

DEFAULT_PIECE_MAX_BYTES = 64 * 1024 * 1024

_JSON_KW: dict[str, Any] = {
    "sort_keys": True,
    "default": str,
    "separators": (",", ":"),
}


def dumps_record_set_json(value: Any) -> str:
    """The one-shot form the streamed text must equal byte for byte."""
    return json.dumps(value, **_JSON_KW)


def iter_record_set_json(value: RecordSet | Mapping[str, Any]) -> Iterator[str]:
    """Yield the canonical JSON text of a record set in record-sized chunks.

    The concatenation equals ``dumps_record_set_json`` of the cursor form
    (``RecordSet.to_cursor()`` for a RecordSet). Only str-keyed dicts are
    streamed; any other value is emitted with the one-shot encoder, so the
    output can never drift from ``json.dumps``.
    """
    if isinstance(value, RecordSet):
        records = value.records
        if all(isinstance(key, str) for key in records):
            yield '{"records":{'
            first = True
            for key in sorted(records):
                yield (
                    ("" if first else ",")
                    + json.dumps(key)
                    + ":"
                    + dumps_record_set_json(records[key].to_cursor())
                )
                first = False
            yield "}}"
            return
        value = value.to_cursor()
    yield from _iter_json(value, depth=2)


def _iter_json(value: Any, *, depth: int) -> Iterator[str]:
    if depth > 0 and isinstance(value, dict) and all(
        isinstance(key, str) for key in value
    ):
        yield "{"
        first = True
        for key in sorted(value):
            yield ("" if first else ",") + json.dumps(key) + ":"
            yield from _iter_json(value[key], depth=depth - 1)
            first = False
        yield "}"
        return
    yield dumps_record_set_json(value)


def iter_pieces(
    chunks: Iterable[tuple[str, int]],
    piece_max_bytes: int,
) -> Iterator[tuple[str, int]]:
    """Group ``(text, utf8_byte_count)`` chunks into ``(piece, bytes)`` pieces.

    Pieces are consecutive, keep the chunk order, and each encodes to at most
    ``piece_max_bytes`` UTF-8 bytes. A chunk larger than the bound is cut at
    code-point boundaries (never inside a character).
    """
    if piece_max_bytes < 4:
        raise ValueError("piece_max_bytes must be at least 4")
    parts: list[str] = []
    size = 0
    for text, nbytes in chunks:
        if not text:
            continue
        if size + nbytes > piece_max_bytes and parts:
            yield "".join(parts), size
            parts, size = [], 0
        if nbytes > piece_max_bytes:
            yield from _split_oversized(text, piece_max_bytes)
            continue
        parts.append(text)
        size += nbytes
    if parts:
        yield "".join(parts), size


def _split_oversized(
    text: str, piece_max_bytes: int,
) -> Iterator[tuple[str, int]]:
    # A code point is at most 4 UTF-8 bytes; the canonical JSON is ASCII in
    # practice (ensure_ascii), so this is conservative, never wrong.
    step = max(1, piece_max_bytes // 4)
    for start in range(0, len(text), step):
        piece = text[start:start + step]
        yield piece, len(piece.encode("utf-8"))


class RecordSetPiecesError(SubstrateError):
    """A pieced record-set authority is incomplete or does not match its header."""


def reassemble_pieces(
    pieces: Iterable[tuple[int, str]],
    *,
    source: str,
    sha256: str,
    piece_count: int,
    piece_bytes: int,
) -> bytearray:
    """Join ``(piece_no, text)`` rows in order and verify them against the header.

    Returns the joined UTF-8 bytes as the bytearray it built, not a copy:
    ``json.loads`` accepts it directly, and a copy would cost another full
    record set of memory.

    Refuses, with ``RecordSetPiecesError``, any gap, repeat, reordering or
    extra piece, a byte total different from ``piece_bytes`` and a SHA-256
    different from ``sha256``. Never returns a partial value.
    """
    hasher = hashlib.sha256()
    buffer = bytearray()
    expected_no = 0
    for piece_no, text in pieces:
        if piece_no != expected_no or expected_no >= piece_count:
            raise _pieces_error(
                "record-set authority pieces are missing, extra or out of order",
                source=source, sha256=sha256, piece_count=piece_count,
                expected_piece_no=expected_no, found_piece_no=piece_no,
            )
        data = str(text).encode("utf-8")
        hasher.update(data)
        buffer += data
        expected_no += 1
    if expected_no != piece_count:
        raise _pieces_error(
            "record-set authority pieces are missing",
            source=source, sha256=sha256, piece_count=piece_count,
            found_piece_count=expected_no,
        )
    if len(buffer) != piece_bytes:
        raise _pieces_error(
            "record-set authority pieces do not add up to the recorded size",
            source=source, sha256=sha256, piece_bytes=piece_bytes,
            found_piece_bytes=len(buffer),
        )
    if hasher.hexdigest() != sha256:
        raise _pieces_error(
            "record-set authority pieces do not match the recorded digest",
            source=source, sha256=sha256,
        )
    return buffer


def _pieces_error(message: str, **data: Any) -> RecordSetPiecesError:
    return RecordSetPiecesError(
        f"SourceRunner: {message}",
        data=data,
        fix_hint=(
            "the stored record set cannot be trusted; do not hand-edit "
            "okg.source_record_set_blob_pieces. Re-run the source with a "
            "full read (reset its watermark) to rebuild the authority"
        ),
    )


#: Runtime bootstrap for deployments whose schema predates migration 0035.
#: The same definitions live in schema.sql and
#: migrations/0035_record_set_blob_pieces.sql; keep the three in step.
RECORD_SET_PIECES_DDL = """
ALTER TABLE okg.source_record_set_blobs
  ADD COLUMN IF NOT EXISTS piece_write_id UUID,
  ADD COLUMN IF NOT EXISTS piece_count INTEGER,
  ADD COLUMN IF NOT EXISTS piece_bytes BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.source_record_set_blobs'::regclass
       AND conname = 'source_record_set_blobs_piece_write_id_key'
  ) THEN
    ALTER TABLE okg.source_record_set_blobs
      ADD CONSTRAINT source_record_set_blobs_piece_write_id_key
      UNIQUE (piece_write_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.source_record_set_blobs'::regclass
       AND conname = 'source_record_set_blobs_pieces_shape'
  ) THEN
    ALTER TABLE okg.source_record_set_blobs
      ADD CONSTRAINT source_record_set_blobs_pieces_shape CHECK (
        (piece_write_id IS NULL AND piece_count IS NULL
          AND piece_bytes IS NULL)
        OR (piece_write_id IS NOT NULL AND piece_count > 0
          AND piece_bytes >= 0 AND record_set_text IS NULL)
      );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS okg.source_record_set_blob_pieces (
  write_id  UUID    NOT NULL,
  piece_no  INTEGER NOT NULL CHECK (piece_no >= 0),
  source    TEXT    NOT NULL,
  piece     TEXT    NOT NULL,
  PRIMARY KEY (write_id, piece_no),
  CONSTRAINT source_record_set_blob_pieces_header_fkey
    FOREIGN KEY (write_id)
    REFERENCES okg.source_record_set_blobs (piece_write_id)
    ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED
);
"""
