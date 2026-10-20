"""Read and write rows of the general blob store ``okg.blobs``.

PACT first-class-images (issue #3046). One row per opaque ``id``; ``kind`` is
a closed set (``image`` only for now) enforced both here, before any SQL, and
by the table's ``blobs_kind_known`` check. A row holds either bytes (with
their sha256, MIME type and size) or a named ``error``, never both; the
table's ``blobs_payload_shape`` check enforces that too.

For images the id is the render id, so the table is also the render cache.
The race guard against cleanup lives in :func:`touch`: a cache hit refreshes
``last_used_at`` *before* the caller writes a reference to the row, and
cleanup only deletes rows older than its grace window. :func:`put` and
:func:`put_error` refresh it the same way when the id already exists.

The id is deterministic for a given input, so an existing row wins: a second
``put`` or ``put_error`` under the same id leaves the stored content as it is
and returns it, with ``inserted`` false. Callers build their reference from
the returned row, never from what they tried to write. An id stored under another kind is refused.

Cleanup is not here; ``okg db prune-blobs`` owns it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from typing import Any, Mapping

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError

__all__ = [
    "BLOB_KINDS",
    "BlobRow",
    "get",
    "put",
    "put_error",
    "touch",
]

#: The kinds the table's ``blobs_kind_known`` check accepts. Adding one takes
#: a migration that widens that check, and its own read view if any role but
#: app_rw reads it.
BLOB_KINDS = frozenset({"image"})

# Every column but the bytes. touch/put/put_error return this; get adds bytes.
_FACT_COLUMNS = (
    "id, kind, sha256, mime_type, size_bytes, error, meta, "
    "created_at, last_used_at"
)


@dataclass(frozen=True)
class BlobRow:
    """One ``okg.blobs`` row.

    ``data`` is filled only by :func:`get`. The writers return the row's
    facts without its bytes: the caller either just supplied them or needs
    only the facts. Use ``error is None`` to tell a byte row from a failure
    row, not ``data``.
    """

    id: str
    kind: str
    sha256: str | None
    mime_type: str | None
    size_bytes: int | None
    error: str | None
    meta: Mapping[str, Any]
    created_at: datetime
    last_used_at: datetime
    data: bytes | None = None
    #: True only on a row :func:`put` or :func:`put_error` returned because
    #: that statement inserted it, rather than finding the id already stored.
    inserted: bool = False


def _require_kind(kind: str) -> None:
    if kind not in BLOB_KINDS:
        raise SubstrateError(
            f"unknown blob kind {kind!r}; okg.blobs accepts {sorted(BLOB_KINDS)}",
            data={"reason": "blob_kind_unknown", "kind": kind},
        )


def _row(
    record: tuple[Any, ...], *, data: bytes | None = None, inserted: bool = False,
) -> BlobRow:
    (
        id_, kind, sha256, mime_type, size_bytes, error, meta,
        created_at, last_used_at,
    ) = record
    return BlobRow(
        id=id_,
        kind=kind,
        sha256=sha256,
        mime_type=mime_type,
        size_bytes=size_bytes,
        error=error,
        meta=dict(meta),
        created_at=created_at,
        last_used_at=last_used_at,
        data=data,
        inserted=inserted,
    )


def touch(conn: psycopg.Connection, *, kind: str, id: str) -> BlobRow | None:
    """Refresh ``last_used_at`` on an existing row and return it.

    Returns ``None`` when no row of ``kind`` has this id. Call this on a
    cache hit before writing a reference to the row, so cleanup cannot
    delete it in between.
    """
    _require_kind(kind)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.blobs SET last_used_at = now() "
            "WHERE kind = %s AND id = %s "
            f"RETURNING {_FACT_COLUMNS}",
            (kind, id),
        )
        record = cur.fetchone()
    return None if record is None else _row(record)


def get(conn: psycopg.Connection, *, kind: str, id: str) -> BlobRow | None:
    """Read one row with its bytes, without refreshing ``last_used_at``."""
    _require_kind(kind)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_FACT_COLUMNS}, bytes FROM okg.blobs "
            "WHERE kind = %s AND id = %s",
            (kind, id),
        )
        record = cur.fetchone()
    if record is None:
        return None
    data = record[-1]
    return _row(record[:-1], data=None if data is None else bytes(data))


def put(
    conn: psycopg.Connection,
    *,
    kind: str,
    id: str,
    data: bytes,
    mime_type: str,
    meta: Mapping[str, Any],
) -> BlobRow:
    """Store bytes under ``id`` and return the stored row.

    The sha256 and size are computed here. When ``id`` already exists the
    stored row is kept, its ``last_used_at`` is refreshed, and it is returned
    as stored (possibly a failure row).
    """
    _require_kind(kind)
    data = bytes(data)
    return _upsert(
        conn,
        kind=kind,
        id=id,
        values=(
            data,
            hashlib.sha256(data).hexdigest(),
            mime_type,
            len(data),
            None,
        ),
        meta=meta,
    )


def put_error(
    conn: psycopg.Connection,
    *,
    kind: str,
    id: str,
    error: str,
    meta: Mapping[str, Any],
) -> BlobRow:
    """Store a named failure under ``id`` and return the stored row.

    Same conflict rule as :func:`put`: an existing row is kept and returned.
    """
    _require_kind(kind)
    if not error:
        raise SubstrateError(
            "a blob failure row needs a named error",
            data={"reason": "blob_error_empty", "kind": kind, "id": id},
        )
    return _upsert(
        conn,
        kind=kind,
        id=id,
        values=(None, None, None, None, error),
        meta=meta,
    )


def _upsert(
    conn: psycopg.Connection,
    *,
    kind: str,
    id: str,
    values: tuple[Any, ...],
    meta: Mapping[str, Any],
) -> BlobRow:
    with conn.cursor() as cur:
        # The conditional DO UPDATE returns no row when the existing id has a
        # different kind; that is refused below rather than overwritten.
        # `xmax = 0` holds only for a tuple this statement inserted; the
        # conflict branch's UPDATE stamps xmax with this transaction's id.
        cur.execute(
            "INSERT INTO okg.blobs "
            "(id, kind, bytes, sha256, mime_type, size_bytes, error, meta) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET last_used_at = now() "
            "WHERE okg.blobs.kind = EXCLUDED.kind "
            f"RETURNING {_FACT_COLUMNS}, (xmax = 0) AS inserted",
            (id, kind, *values, Jsonb(dict(meta))),
        )
        record = cur.fetchone()
        if record is not None:
            return _row(record[:-1], inserted=bool(record[-1]))
        cur.execute("SELECT kind FROM okg.blobs WHERE id = %s", (id,))
        existing = cur.fetchone()
    raise SubstrateError(
        f"blob id {id!r} is already stored as kind "
        f"{existing[0] if existing else None!r}, not {kind!r}",
        data={
            "reason": "blob_kind_mismatch",
            "id": id,
            "kind": kind,
            "stored_kind": existing[0] if existing else None,
        },
    )
