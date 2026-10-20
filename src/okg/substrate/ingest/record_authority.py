"""Bounded indexed record authority primitives.

``install_prototype`` remains isolated from normal migrations.  The production
format uses the streaming canonical helpers below and is activated separately
by the source registry; legacy storage remains the default.
The logical digest v1 is the existing canonical RecordSet JSON SHA256, streamed
one record at a time. All prototype records and encoded write batches are bounded.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import contextmanager
from threading import Lock
from weakref import WeakKeyDictionary

from psycopg.errors import UniqueViolation
from psycopg.pq import TransactionStatus
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping

from okg.substrate.library.sources.base import RecordAuthorityStream

LIMIT = 1024 * 1024
PRODUCTION_FORMAT = 'okg.source_runner.record_authority.indexed.v2'
CANONICAL_CHUNK_BYTES = 64 * 1024
READ_PAGE = 16
MAX_FETCH_ROW_BYTES = 2 * LIMIT  # complete encoded SQL result fields, not just emission
# Identity fields are short scalars. Everything else is streamed as bytes and
# is never decoded into a Python object by this module.
IDENTITY_FIELD_MAX_BYTES = 4096
# A field at or below this size is returned with its component row instead
# of through its own round trip. It stays far below one chunk.
INLINE_FIELD_BYTES = 64 * 1024
IDENTITY_BATCH_ROWS = 512
# Identity-field rows carry at most one bounded field each, so this prefetch
# holds IDENTITY_FETCH_ROWS * IDENTITY_FIELD_MAX_BYTES: half of one chunk.
IDENTITY_FETCH_ROWS = 128
# The canonical sweep carries a field's bytes in its own row only up to this
# size, so SWEEP_FETCH_ROWS * SWEEP_INLINE_BYTES stays at one chunk. A larger
# field costs one extra round trip and is streamed instead of prefetched.
SWEEP_INLINE_BYTES = 4 * 1024
SWEEP_FETCH_ROWS = 256
# Canonical JSON sorts a record body as descendants, edges, primary, which is
# also the alphabetical order of the stored component kinds.
_KIND_ORDER = ('descendant', 'edge', 'primary')
_KIND_RANK = {kind: rank for rank, kind in enumerate(_KIND_ORDER)}
_PRIMARY_RANK = _KIND_RANK['primary']
_KIND_PREFIX = (b'"descendants":[', b'"edges":[', b'"primary":')
# One write batch carries at most this many encoded bytes before it is sent.
# Eight chunks amortize table setup and COPY round trips for wide records.
# Payload parameters and reader prefetches retain their separate 1 MiB bound.
STAGE_BATCH_BYTES = 8 * LIMIT
# Every buffered index row also costs this much of the batch bound. Bounding a
# batch by payload bytes alone would let a corpus of tiny fields buffer
# hundreds of thousands of rows, so each row pays a fixed share as well.
STAGE_ROW_OVERHEAD_BYTES = 256

_STAGE_TABLE_DDL = '''
CREATE TEMP TABLE IF NOT EXISTS authority_stage_payloads (
  sha256 TEXT NOT NULL, byte_count BIGINT NOT NULL, chunk_count BIGINT NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS authority_stage_chunks (
  sha256 TEXT NOT NULL, chunk_ordinal BIGINT NOT NULL, payload BYTEA NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS authority_stage_operations (
  ordinal BIGINT NOT NULL, record_id TEXT NOT NULL, operation TEXT NOT NULL,
  operation_sha256 TEXT NOT NULL, component_count BIGINT NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS authority_stage_components (
  operation_ordinal BIGINT NOT NULL, component_kind TEXT NOT NULL,
  component_ordinal BIGINT NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS authority_stage_fields (
  operation_ordinal BIGINT NOT NULL, component_kind TEXT NOT NULL,
  component_ordinal BIGINT NOT NULL, field_name TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS authority_stage_identities (
  operation_ordinal BIGINT NOT NULL, identity_kind TEXT NOT NULL,
  identity TEXT NOT NULL, ownership TEXT NOT NULL
) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS authority_stage_records (
  record_id TEXT NOT NULL, operation_ordinal BIGINT NOT NULL,
  retract BOOLEAN NOT NULL
) ON COMMIT DELETE ROWS;
'''
# ON COMMIT DELETE ROWS empties these only on a real COMMIT, and
# ``conn.transaction()`` is a SAVEPOINT when the caller already opened one.
# Every read of a staging table is unfiltered, so a row left behind would be
# attributed to whatever authority flushes next: empty them explicitly.
# DELETE rather than TRUNCATE: these tables are empty on every flush that
# followed a real commit, and DELETE over an empty table is nearly free, while
# TRUNCATE allocates a new relfilenode for each of them every time.
_STAGE_TABLE_RESET = ''.join(
    'DELETE FROM authority_stage_%s;' % name
    for name in (
        'payloads', 'chunks', 'operations', 'components', 'fields',
        'identities', 'records',
    )
)


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(',', ':'))


def digest(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class StreamingRecord:
    """One record whose repeated components may be consumed incrementally.

    Canonical JSON sorts keys as ``descendants``, ``edges``, ``primary``.
    Semantic callers may use another traversal order; that order never changes
    the legacy-v1 digest.
    """

    record_id: str
    primary: Mapping[str, Any] | None
    descendants: Iterable[Mapping[str, Any]]
    edges: Iterable[Mapping[str, Any]]


def _iter_json(value: Any) -> Iterator[bytes]:
    """Emit canonical JSON while bounding even a single escaped string.

    ``JSONEncoder.iterencode`` may return one complete string scalar. Splitting
    the source string first avoids allocating its complete escaped form.
    """

    if isinstance(value, str):
        yield b'"'
        step = CANONICAL_CHUNK_BYTES // 4
        for offset in range(0, len(value), step):
            encoded = json.dumps(
                value[offset:offset + step],
                ensure_ascii=True,
                separators=(',', ':'),
            ).encode('utf-8')
            body = encoded[1:-1]
            for start in range(0, len(body), CANONICAL_CHUNK_BYTES):
                yield body[start:start + CANONICAL_CHUNK_BYTES]
        yield b'"'
        return
    if isinstance(value, Mapping):
        yield b'{'
        first = True
        for key in sorted(value):
            if not isinstance(key, str):
                raise AuthorityError('canonical record mappings require string keys')
            if not first:
                yield b','
            first = False
            yield from _iter_json(key)
            yield b':'
            yield from _iter_json(value[key])
        yield b'}'
        return
    if isinstance(value, (list, tuple)):
        yield b'['
        for index, item in enumerate(value):
            if index:
                yield b','
            yield from _iter_json(item)
        yield b']'
        return
    encoded = json.dumps(
        value,
        sort_keys=True,
        default=str,
        separators=(',', ':'),
    ).encode('utf-8')
    for start in range(0, len(encoded), CANONICAL_CHUNK_BYTES):
        yield encoded[start:start + CANONICAL_CHUNK_BYTES]


def _iter_array(values: Iterable[Mapping[str, Any]]) -> Iterator[bytes]:
    yield b'['
    first = True
    for value in values:
        if not first:
            yield b','
        first = False
        yield from _iter_json(value)
    yield b']'


def iter_legacy_record_set_bytes(
    records: Iterable[StreamingRecord],
) -> Iterator[bytes]:
    """Emit exact canonical ``RecordSet.to_cursor`` bytes without collecting it.

    ``records`` must be strictly ordered by record id.  Rejecting disorder keeps
    digest construction deterministic without sorting a whole authority in
    memory.  Individual descendants and edges are streamed and have no fixed
    logical byte ceiling.
    """

    yield b'{"records":{'
    previous: str | None = None
    first = True
    for record in records:
        if not record.record_id or (
            previous is not None and record.record_id <= previous
        ):
            raise AuthorityError(
                'record authority ids must be strictly increasing'
            )
        previous = record.record_id
        if not first:
            yield b','
        first = False
        yield from _iter_json(record.record_id)
        yield b':{"descendants":'
        yield from _iter_array(record.descendants)
        yield b',"edges":'
        yield from _iter_array(record.edges)
        yield b',"primary":'
        yield from _iter_json(record.primary)
        yield b'}'
    yield b'}}'


def legacy_record_set_digest(records: Iterable[StreamingRecord]) -> str:
    """Return the legacy-v1 SHA-256 for an ordered streaming authority."""

    hasher = hashlib.sha256()
    for chunk in iter_legacy_record_set_bytes(records):
        hasher.update(chunk)
    return hasher.hexdigest()


def _coalesced_json_chunks(value: Any) -> Iterator[bytes]:
    """Coalesce encoder fragments into transport chunks bounded by ``LIMIT``."""

    pending = bytearray()
    for fragment in _iter_json(value):
        view = memoryview(fragment)
        while view:
            available = LIMIT - len(pending)
            pending.extend(view[:available])
            view = view[available:]
            if len(pending) == LIMIT:
                yield bytes(pending)
                pending.clear()
    if pending:
        yield bytes(pending)


def _require_bounded_identity(identity: tuple[str, str, str]) -> None:
    """Refuse an identity the verified reader could not rebuild in bounded memory.

    The seal-time index rebuild reads identity fields under
    ``IDENTITY_FIELD_MAX_BYTES``.  Applying the same bound at write time keeps
    staging and sealing from disagreeing about the same authority.
    """

    _, value, ownership = identity
    for field in (value, ownership):
        if len(field.encode('utf-8')) > IDENTITY_FIELD_MAX_BYTES:
            raise AuthorityError('authority identity field exceeds byte bound')


class AuthorityError(ValueError):
    """Invalid, incomplete, conflicting or inaccessible authority."""


# Readers borrow a connection, not a store instance. Weak keys avoid retaining
# closed psycopg connections. Wrappers are separate handles unless both stores
# use the same object; privileged raw SQL is outside this API's exclusion.
_READER_CONNECTIONS: WeakKeyDictionary[Any, object] = WeakKeyDictionary()
_READER_CONNECTIONS_LOCK = Lock()


@dataclass(frozen=True)
class PrivateAuthorityCandidate:
    """Transaction-local handoff for an unsealed v2 authority.

    This type is deliberately not cursor-serializable and carries no public
    record-set reference or summary semantics.
    """

    authority: str
    source: str
    incarnation: str
    attempt: str
    expected_sha256: str
    expected_records: int
    expected_operations: int
    expected_components: int


@dataclass(frozen=True)
class ProductionReference:
    authority: str
    source: str
    incarnation: str
    sha256: str
    count: int
    format: str = PRODUCTION_FORMAT

    def to_cursor(self) -> dict[str, Any]:
        return {
            'format': self.format,
            'authority_id': self.authority,
            'source': self.source,
            'incarnation': self.incarnation,
            'sha256': self.sha256,
            'record_count': self.count,
        }


class VerifiedProductionReader:
    """Authenticated byte-stream view held inside one stable DB snapshot.

    Construction is private to ``ProductionAuthorityStore.reader``.  The store
    verifies the complete logical digest and identity projection before handing
    this object to a caller, so abandoning a returned iterator cannot skip an
    integrity check.  This API deliberately has no mapped-record iterator:
    decoding a large JSON field would reintroduce whole-field materialization.
    """

    def __init__(self, store: 'ProductionAuthorityStore', reference: ProductionReference):
        self._store = store
        self.reference = reference
        self._active = True
        self._open: list[Iterator[Any]] = []

    def _check(self) -> None:
        if not self._active:
            raise AuthorityError('verified authority reader is closed')

    def _guarded(self, iterable) -> Iterator[Any]:
        """Bind a server-cursor generator to this snapshot.

        An iterator that escapes the ``reader`` context must not keep yielding
        rows it prefetched inside the verified snapshot.
        """

        self._check()
        iterator = iter(iterable)
        self._open.append(iterator)
        try:
            while True:
                self._check()
                try:
                    value = next(iterator)
                except StopIteration:
                    return
                self._check()
                yield value
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                close()
            if iterator in self._open:
                self._open.remove(iterator)

    def iterate_canonical_bytes(self) -> Iterator[bytes]:
        """Stream the exact legacy-v1 canonical record-set bytes.

        Peak memory is one payload chunk, not one field and not one record.
        """

        return self._guarded(
            self._store._iter_stored_record_set_bytes(self.reference.authority)
        )

    def iterate_record_ids(self) -> Iterator[str]:
        """Ordered live record ids for coverage and retraction consumers."""

        return self._guarded(
            self._store._iter_record_ids(self.reference.authority)
        )

    def iterate_record_bytes(self) -> Iterator[tuple[str, Iterator[bytes]]]:
        """Ordered ``(record_id, canonical byte stream)`` pairs.

        Merge and retraction consumers get one record at a time and still never
        hold a whole record: the second element is itself a chunk iterator.
        """

        return self._guarded(
            self._store._iter_record_byte_streams(self.reference.authority)
        )

    def iterate_identities(self) -> Iterator[tuple[str, str, str]]:
        """Ordered ``(kind, identity, ownership)`` rows from the verified index."""

        return self._guarded(
            self._store._iter_identities(self.reference.authority)
        )

    def missing_identities(
        self, expected: Iterable[tuple[str, str]],
    ) -> Iterator[tuple[str, str]]:
        """Anti-join expected identities against this authority in bounded batches."""

        return self._guarded(
            self._store._missing_production_identities(
                self.reference.authority, expected,
            )
        )

    def _close(self) -> None:
        self._active = False
        try:
            for iterator in reversed(self._open[:]):
                close = getattr(iterator, 'close', None)
                if close is not None:
                    close()
        finally:
            self._open.clear()



class VerifiedPrivateIdentityReader:
    """One-pass raw identity stream private to an unsealed candidate context.

    The store verifies the complete live authority and declares the cursor
    before this object escapes. No public reference or reusable validation
    token is exposed. Callers must not execute raw SQL or control transactions
    on the borrowed connection; this Python API does not sandbox that access.
    """

    def __init__(self, conn, cursor):
        self._conn = conn
        self._cursor = cursor
        self._rows = iter(cursor)
        self._active = True

    def _check(self) -> None:
        if (not self._active or self._cursor.closed
                or self._conn.info.transaction_status != TransactionStatus.INTRANS):
            raise AuthorityError('private authority reader is closed or transaction ended')

    def __iter__(self):
        return self

    def __next__(self) -> tuple[str, str, str]:
        self._check()
        row = next(self._rows)
        self._check()
        return str(row[0]), str(row[1]), str(row[2])

    def iterate_identities(self) -> Iterator[tuple[str, str, str]]:
        """Return the shared one-pass stream, preserving duplicate live rows."""

        self._check()
        return self

    def _close(self) -> None:
        self._active = False
        try:
            close = getattr(self._rows, 'close', None)
            if close is not None:
                close()
        finally:
            self._cursor.close()


class _StageBatch:
    """Bounded buffer of one authority's rows between COPY flushes.

    ``byte_cost`` counts encoded payload bytes plus a fixed share for every
    buffered index row, so the buffer is bounded by bytes rather than by a
    record count that one wide record could blow past. Only whole operations
    are buffered here: a flush that happens part way through a record carries
    that record's payload rows, which no operation row references, and leaves
    its index rows for the flush that follows.
    """

    def __init__(self, authority: str, *, resume: bool = True) -> None:
        self.authority = authority
        # A batch that may be restaging an attempt has to tolerate rows that
        # are already there; a batch staging an authority that had none can
        # COPY straight into the authority tables, where a duplicate is a
        # unique violation that aborts the batch instead of being skipped.
        self.resume = resume
        self.payloads: list[tuple[str, int, int]] = []
        self.chunks: list[tuple[str, int, bytes]] = []
        self.operations: list[tuple[int, str, str, str, int]] = []
        self.components: list[tuple[int, str, int]] = []
        self.fields: list[tuple[int, str, int, str, str]] = []
        self.identities: list[tuple[int, str, str, str]] = []
        self.records: dict[str, tuple[int, bool]] = {}
        self.payload_shas: set[str] = set()
        self.byte_cost = 0

    def empty(self) -> bool:
        return not (self.payloads or self.operations)

    def clear(self) -> None:
        self.payloads.clear()
        self.payload_shas.clear()
        self.chunks.clear()
        self.operations.clear()
        self.components.clear()
        self.fields.clear()
        self.identities.clear()
        self.records.clear()
        self.byte_cost = 0

    def add_single_chunk_payload(
        self, sha: str, byte_count: int, chunk: bytes,
    ) -> None:
        # A digest names immutable bytes: repeated fields in this bounded
        # batch need only one header and chunk, just as the SQL dedupe writes
        # them once across batches. Clear the set with the batch.
        if sha in self.payload_shas:
            return
        self.payload_shas.add(sha)
        self.payloads.append((sha, byte_count, 1))
        self.chunks.append((sha, 0, chunk))
        self.byte_cost += len(chunk) + 2 * STAGE_ROW_OVERHEAD_BYTES

    def add_operation(
        self, ordinal: int, record_id: str, operation: str,
        operation_sha: str, component_count: int,
    ) -> None:
        self.operations.append(
            (ordinal, record_id, operation, operation_sha, component_count)
        )
        # The last operation for a record id in this batch decides its row.
        self.records[record_id] = (ordinal, operation == 'retract')
        self.byte_cost += STAGE_ROW_OVERHEAD_BYTES + len(record_id)

    def add_component(
        self, ordinal: int, kind: str, index: int,
        fields: tuple[tuple[str, str], ...],
    ) -> None:
        self.components.append((ordinal, kind, index))
        self.byte_cost += STAGE_ROW_OVERHEAD_BYTES
        for name, sha in fields:
            self.fields.append((ordinal, kind, index, name, sha))
            self.byte_cost += STAGE_ROW_OVERHEAD_BYTES + len(name)

    def add_identity(
        self, ordinal: int, kind: str, identity: str, ownership: str,
    ) -> None:
        self.identities.append((ordinal, kind, identity, ownership))
        self.byte_cost += STAGE_ROW_OVERHEAD_BYTES + len(identity)


class ProductionAuthorityStore:
    """Bounded writer/reader for the migrated indexed-v2 tables.

    A write sends at most one payload chunk per SQL parameter. Logical records
    are represented by ordered component rows and are never serialized into a
    single database value.

    Callers serialize operations on a shared connection. Reader exclusion
    rejects reentrant store calls across instances; it does not make this
    multi-statement API safe for concurrent threads sharing one connection.
    """

    def __init__(self, conn):
        self.conn = conn
        self.max_parameter_bytes = 0

    def _require_available_connection(self) -> None:
        with _READER_CONNECTIONS_LOCK:
            if self.conn in _READER_CONNECTIONS:
                raise AuthorityError('authority connection has an active verified reader')

    @contextmanager
    def _reader_transaction(self):
        # Claim before verification SQL, including isolation inspection. The
        # exclusion spans stores using this handle and releases on every exit.
        with _READER_CONNECTIONS_LOCK:
            if self.conn in _READER_CONNECTIONS:
                raise AuthorityError('authority connection has an active verified reader')
            token = object()
            _READER_CONNECTIONS[self.conn] = token
        try:
            idle = self.conn.info.transaction_status == TransactionStatus.IDLE
            if not idle:
                isolation = self.conn.execute('SHOW transaction_isolation').fetchone()[0]
                if isolation not in ('repeatable read', 'serializable'):
                    raise AuthorityError('verified reader rejects an existing unstable transaction')
            with self.conn.transaction():
                if idle:
                    self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                yield
        finally:
            with _READER_CONNECTIONS_LOCK:
                if _READER_CONNECTIONS.get(self.conn) is token:
                    del _READER_CONNECTIONS[self.conn]

    def begin(self, source: str, incarnation: str, attempt: str) -> str:
        self._require_available_connection()
        if any(not isinstance(v, str) or not v or len(v.encode()) > 2048
               for v in (source, incarnation, attempt)):
            raise AuthorityError('invalid bounded authority identity')
        authority = str(uuid.uuid4())
        with self.conn.transaction():
            self.conn.execute(
                '''INSERT INTO okg.source_record_authorities
                   (authority_id,source,incarnation,attempt)
                   VALUES (%s,%s,%s,%s)
                   ON CONFLICT(source,incarnation,attempt) DO NOTHING''',
                (authority, source, incarnation, attempt),
            )
            row = self.conn.execute(
                '''SELECT authority_id,state FROM okg.source_record_authorities
                   WHERE source=%s AND incarnation=%s AND attempt=%s''',
                (source, incarnation, attempt),
            ).fetchone()
            if row is None:
                raise AuthorityError('authority attempt could not be created')
            return str(row[0])

    def stage_stream(
        self,
        source: str,
        incarnation: str,
        attempt: str,
        stream: RecordAuthorityStream,
    ) -> PrivateAuthorityCandidate:
        """Consume one operation at a time and return a non-public candidate."""

        self._require_available_connection()
        if (len(stream.logical_sha256) != 64
                or any(c not in '0123456789abcdef' for c in stream.logical_sha256)
                or min(stream.record_count, stream.operation_count,
                       stream.component_count) < 0):
            raise AuthorityError('invalid authority stream expectations')
        authority = self.begin(source, incarnation, attempt)
        # An attempt that already carries operations is being restaged after a
        # failure, so its batches must tolerate rows that are already there.
        staged = self.conn.execute(
            'SELECT 1 FROM okg.source_record_authority_operations '
            'WHERE authority_id=%s LIMIT 1', (authority,),
        ).fetchone()
        batch = _StageBatch(authority, resume=staged is not None)
        observed_operations = observed_components = 0
        for operation in stream.operations:
            if operation.ordinal != observed_operations:
                raise AuthorityError('authority operations must be contiguous')
            if operation.retract:
                sentinel = object()
                if (operation.primary is not None
                        or next(iter(operation.descendants), sentinel) is not sentinel
                        or next(iter(operation.edges), sentinel) is not sentinel):
                    raise AuthorityError('retraction operation carries components')
                self._buffer_retraction(
                    batch, operation.ordinal, operation.record_id
                )
            else:
                # Count while consuming the two one-shot component streams;
                # small iterator wrappers avoid copying either logical record.
                def counted(values):
                    nonlocal observed_components
                    for value in values:
                        observed_components += 1
                        yield value
                if operation.primary is not None:
                    observed_components += 1
                self._buffer_record(
                    batch, operation.ordinal, operation.record_id,
                    operation.primary, counted(operation.descendants),
                    counted(operation.edges),
                )
            observed_operations += 1
            self._flush_if_full(batch)
        self._flush_batch(batch)
        if (observed_operations != stream.operation_count
                or observed_components != stream.component_count):
            raise AuthorityError('authority stream count mismatch')
        self._refresh_authority_statistics(observed_operations)
        return PrivateAuthorityCandidate(
            authority=authority,
            source=source,
            incarnation=incarnation,
            attempt=attempt,
            expected_sha256=stream.logical_sha256,
            expected_records=stream.record_count,
            expected_operations=stream.operation_count,
            expected_components=stream.component_count,
        )

    def _refresh_authority_statistics(self, staged_operations: int) -> None:
        """Re-analyze the authority tables a bulk stage just loaded.

        The seal-time sweeps read this authority through server cursors, which
        the planner costs from table statistics. After a COPY-driven stage
        those statistics still describe the table as it was before the load,
        and the sweep plans chosen from them are catastrophically wrong: a
        20,000-record seal took 77 s on stale statistics and 8 s on current
        ones. Autovacuum would get there eventually; the seal runs now.
        """

        if staged_operations <= 0:
            return
        self.conn.execute(
            'ANALYZE okg.source_record_authority_operations,'
            'okg.source_record_authority_records,'
            'okg.source_record_authority_components,'
            'okg.source_record_authority_component_fields,'
            'okg.source_record_authority_identities,'
            'okg.source_record_authority_payloads'
        )

    def _header(self, authority: str, *, lock: bool = False):
        suffix = ' FOR UPDATE' if lock else ''
        row = self.conn.execute(
            '''SELECT source,incarnation,attempt,state,logical_sha256,
                      record_count,operation_count,component_count
               FROM okg.source_record_authorities WHERE authority_id=%s''' + suffix,
            (authority,),
        ).fetchone()
        if row is None:
            raise AuthorityError('missing authority')
        return row

    @staticmethod
    def _payload_parts(value: Mapping[str, Any]) -> tuple[dict[str, Any], Any]:
        if not isinstance(value, Mapping):
            raise AuthorityError('authority component must be a mapping')
        payload = dict(value)
        if 'source_revision' not in payload:
            raise AuthorityError('authority component has no source_revision')
        revision = payload.pop('source_revision')
        return payload, revision

    def _buffer_component_payloads(
        self, batch: '_StageBatch', value: Mapping[str, Any],
    ) -> tuple[tuple[str, str], ...]:
        """Hash every field of one component and buffer the single-chunk ones.

        A component is a handful of named fields and almost all of them encode
        to a single bounded chunk.  Those rows join the current write batch and
        reach PostgreSQL through one COPY per table, so a record costs no round
        trip of its own.  A field that needs more than one chunk still goes
        through ``_store_payload``, which keeps its header and its whole chunk
        sequence inside one transaction; splitting a chunk sequence across
        batches would let a crash leave a payload that a later writer reads as
        already present.
        """

        fields: list[tuple[str, str]] = []
        for field_name, field_value in sorted(value.items()):
            if not isinstance(field_name, str) or not field_name:
                raise AuthorityError('authority component fields require names')
            hasher = hashlib.sha256()
            byte_count = 0
            seen = 0
            single = b''
            for chunk in _coalesced_json_chunks(field_value):
                if len(chunk) > LIMIT:
                    raise AuthorityError('payload encoder exceeded SQL chunk bound')
                hasher.update(chunk)
                byte_count += len(chunk)
                seen += 1
                single = chunk if seen == 1 else b''
            sha = hasher.hexdigest()
            fields.append((field_name, sha))
            if seen != 1:
                self._store_payload(
                    field_value, sha=sha, byte_count=byte_count,
                    chunk_count=seen,
                )
                continue
            self.max_parameter_bytes = max(self.max_parameter_bytes, len(single))
            batch.add_single_chunk_payload(sha, byte_count, single)
            self._flush_if_full(batch)
        return tuple(fields)

    def _copy_rows(self, table: str, columns: str, types, rows) -> None:
        """Send one buffered table through a single binary COPY."""

        if not rows:
            return
        with self.conn.cursor() as cur:
            with cur.copy(
                'COPY ' + table + ' ' + columns + ' FROM STDIN (FORMAT BINARY)'
            ) as copy:
                copy.set_types(types)
                for row in rows:
                    copy.write_row(row)

    def _flush_if_full(self, batch: '_StageBatch') -> None:
        if batch.byte_cost >= STAGE_BATCH_BYTES:
            self._flush_batch(batch)

    def _flush_batch(self, batch: '_StageBatch') -> None:
        """Write one bounded batch: one COPY plus one INSERT per table.

        Every refusal the per-record writer raised is re-raised here as a set
        predicate over the batch: a content-addressed payload whose stored
        header disagrees with these bytes, a payload whose chunk sequence is
        incomplete, and an operation ordinal already staged with different
        content.  The batch is one transaction, so a failure part way through
        leaves the candidate short of its expected counts, which
        ``_verify_candidate`` refuses to seal, and a retry restages from
        ordinal zero over ``ON CONFLICT DO NOTHING``.
        """

        self._require_available_connection()
        if batch.empty():
            return
        authority = batch.authority
        with self.conn.transaction():
            self.conn.execute(_STAGE_TABLE_DDL + _STAGE_TABLE_RESET)
            if self._header(authority, lock=True)[3] != 'staging':
                raise AuthorityError('sealed authority is immutable')
            self._write_batch_payloads(batch)
            if batch.resume:
                self._stage_batch_operations(batch)
            else:
                try:
                    self._copy_batch_operations(authority, batch)
                except UniqueViolation as exc:
                    # Two writers can both find the attempt empty before either
                    # flushes; the header lock serializes them and the loser
                    # copies an ordinal that is already there. Callers catch
                    # AuthorityError, so this must not escape as a driver error.
                    raise AuthorityError('conflicting attempt retry') from exc
            self._copy_rows(
                'authority_stage_records',
                '(record_id,operation_ordinal,retract)',
                ('text', 'int8', 'bool'),
                [(record_id, ordinal, retract)
                 for record_id, (ordinal, retract) in batch.records.items()],
            )
            self._write_batch_operations(authority, batch)
            batch.clear()

    def _write_batch_payloads(self, batch: '_StageBatch') -> None:
        """Publish this batch's content-addressed payloads exactly once.

        Payload rows are shared between records, attempts and sources, so they
        go through a staging table and ``ON CONFLICT DO NOTHING``: a revision
        two records share is inserted once, and one already stored by an
        earlier batch or an earlier run is left alone.
        """

        if not batch.payloads:
            return
        self._copy_rows(
            'authority_stage_payloads', '(sha256,byte_count,chunk_count)',
            ('text', 'int8', 'int8'), batch.payloads,
        )
        self._copy_rows(
            'authority_stage_chunks', '(sha256,chunk_ordinal,payload)',
            ('text', 'int8', 'bytea'), batch.chunks,
        )
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_payloads
               (sha256,payload_kind,byte_count,chunk_count)
               SELECT DISTINCT sha256,'json',byte_count,chunk_count
                 FROM authority_stage_payloads
               ON CONFLICT DO NOTHING'''
        )
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_payload_chunks
               (sha256,chunk_ordinal,payload)
               SELECT DISTINCT ON (sha256,chunk_ordinal)
                      sha256,chunk_ordinal,payload
                 FROM authority_stage_chunks
                ORDER BY sha256,chunk_ordinal
               ON CONFLICT DO NOTHING'''
        )
        conflict = self.conn.execute(
            '''SELECT 1 FROM authority_stage_payloads s
                 LEFT JOIN okg.source_record_authority_payloads p
                   ON p.sha256=s.sha256
                WHERE p.sha256 IS NULL OR p.payload_kind<>'json'
                   OR p.byte_count<>s.byte_count
                   OR p.chunk_count<>s.chunk_count
                LIMIT 1'''
        ).fetchone()
        if conflict:
            raise AuthorityError('conflicting content-addressed payload')
        incomplete = self.conn.execute(
            '''SELECT 1
                 FROM (SELECT DISTINCT sha256 FROM authority_stage_payloads) s
                 JOIN okg.source_record_authority_payloads p
                   ON p.sha256=s.sha256
                WHERE p.chunk_count <> (
                        SELECT count(*)
                          FROM okg.source_record_authority_payload_chunks c
                         WHERE c.sha256=p.sha256)
                LIMIT 1'''
        ).fetchone()
        if incomplete:
            raise AuthorityError('incomplete shared authority payload')

    def _stage_batch_operations(self, batch: '_StageBatch') -> None:
        """Buffer this batch through staging tables so a retry can repeat it."""

        self._copy_rows(
            'authority_stage_operations',
            '(ordinal,record_id,operation,operation_sha256,component_count)',
            ('int8', 'text', 'text', 'text', 'int8'), batch.operations,
        )
        self._copy_rows(
            'authority_stage_components',
            '(operation_ordinal,component_kind,component_ordinal)',
            ('int8', 'text', 'int8'), batch.components,
        )
        self._copy_rows(
            'authority_stage_fields',
            '(operation_ordinal,component_kind,component_ordinal,'
            'field_name,payload_sha256)',
            ('int8', 'text', 'int8', 'text', 'text'), batch.fields,
        )
        self._copy_rows(
            'authority_stage_identities',
            '(operation_ordinal,identity_kind,identity,ownership)',
            ('int8', 'text', 'text', 'text'), batch.identities,
        )

    def _copy_batch_operations(self, authority: str, batch: '_StageBatch') -> None:
        """COPY straight into the authority tables, one pass instead of two.

        Only a stage that found the authority empty takes this path, and one
        stream never repeats an operation ordinal, a component index or a field
        name. A duplicate here is therefore a real conflicting retry, and it
        raises a unique violation that aborts the batch -- the same refusal the
        staging path spells as ``conflicting attempt retry``, reached without
        writing every row twice. Measured on this host, that second write was
        a third of the staging wall time.
        """

        key = uuid.UUID(authority)
        self._copy_rows(
            'okg.source_record_authority_operations',
            '(authority_id,ordinal,record_id,operation,operation_sha256,'
            'component_count)',
            ('uuid', 'int8', 'text', 'text', 'text', 'int8'),
            [(key,) + row for row in batch.operations],
        )
        self._copy_rows(
            'okg.source_record_authority_components',
            '(authority_id,operation_ordinal,component_kind,component_ordinal)',
            ('uuid', 'int8', 'text', 'int8'),
            [(key,) + row for row in batch.components],
        )
        self._copy_rows(
            'okg.source_record_authority_component_fields',
            '(authority_id,operation_ordinal,component_kind,component_ordinal,'
            'field_name,payload_sha256)',
            ('uuid', 'int8', 'text', 'int8', 'text', 'text'),
            [(key,) + row for row in batch.fields],
        )
        # Two components of one operation can carry the same node identity and
        # ownership, which is one row in the index, not two.
        self._copy_rows(
            'okg.source_record_authority_identities',
            '(authority_id,operation_ordinal,identity_kind,identity,ownership)',
            ('uuid', 'int8', 'text', 'text', 'text'),
            [(key,) + row for row in dict.fromkeys(batch.identities)],
        )

    def _write_batch_operations(self, authority: str, batch: '_StageBatch') -> None:
        if not batch.operations:
            return
        if not batch.resume:
            self._write_batch_records(authority)
            return
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_operations
               (authority_id,ordinal,record_id,operation,operation_sha256,
                component_count)
               SELECT %s::uuid,ordinal,record_id,operation,operation_sha256,
                      component_count
                 FROM authority_stage_operations
               ON CONFLICT DO NOTHING''',
            (authority,),
        )
        conflict = self.conn.execute(
            '''SELECT 1 FROM authority_stage_operations s
                 LEFT JOIN okg.source_record_authority_operations o
                   ON o.authority_id=%s AND o.ordinal=s.ordinal
                WHERE o.ordinal IS NULL
                   OR (o.record_id,o.operation,o.operation_sha256,
                       o.component_count)
                      IS DISTINCT FROM
                      (s.record_id,s.operation,s.operation_sha256,
                       s.component_count)
                LIMIT 1''',
            (authority,),
        ).fetchone()
        if conflict:
            raise AuthorityError('conflicting attempt retry')
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_components
               (authority_id,operation_ordinal,component_kind,component_ordinal)
               SELECT %s::uuid,operation_ordinal,component_kind,component_ordinal
                 FROM authority_stage_components
               ON CONFLICT DO NOTHING''',
            (authority,),
        )
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_component_fields
               (authority_id,operation_ordinal,component_kind,component_ordinal,
                field_name,payload_sha256)
               SELECT %s::uuid,operation_ordinal,component_kind,component_ordinal,
                      field_name,payload_sha256
                 FROM authority_stage_fields
               ON CONFLICT DO NOTHING''',
            (authority,),
        )
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_identities
               (authority_id,operation_ordinal,identity_kind,identity,ownership)
               SELECT DISTINCT %s::uuid,operation_ordinal,identity_kind,identity,
                      ownership
                 FROM authority_stage_identities
               ON CONFLICT DO NOTHING''',
            (authority,),
        )
        self._write_batch_records(authority)

    def _write_batch_records(self, authority: str) -> None:
        # The last operation of the batch for a record id decides its row, the
        # same way the per-record writer left it: an upsert carries the record
        # forward at its own ordinal, a retraction removes every earlier one.
        self.conn.execute(
            '''INSERT INTO okg.source_record_authority_records
               (authority_id,record_id,operation_ordinal)
               SELECT %s::uuid,record_id,operation_ordinal
                 FROM authority_stage_records WHERE NOT retract
               ON CONFLICT(authority_id,record_id) DO UPDATE
                 SET operation_ordinal=excluded.operation_ordinal
                 WHERE okg.source_record_authority_records.operation_ordinal
                       < excluded.operation_ordinal''',
            (authority,),
        )
        self.conn.execute(
            '''DELETE FROM okg.source_record_authority_records r
                 USING authority_stage_records s
                WHERE r.authority_id=%s AND s.retract
                  AND r.record_id=s.record_id
                  AND r.operation_ordinal < s.operation_ordinal''',
            (authority,),
        )

    def _store_payload(
        self, value: Any, *, sha: str | None = None,
        byte_count: int | None = None, chunk_count: int | None = None,
    ) -> str:
        """Write one whole content-addressed payload in its own transaction.

        This is the multi-chunk path: a field too large for a single bounded
        chunk.  Chunks are streamed into one COPY, so a field of any size costs
        one round trip and holds one chunk at a time.

        A caller that has already streamed the value once passes the digest and
        counts it measured, so a multi-megabyte field is encoded twice rather
        than three times. The COPY below still hashes nothing it does not
        write, and the stored header is still compared against what is there.
        """

        self._require_available_connection()
        if sha is None or byte_count is None or chunk_count is None:
            hasher = hashlib.sha256()
            byte_count = 0
            chunk_count = 0
            for chunk in _coalesced_json_chunks(value):
                hasher.update(chunk)
                byte_count += len(chunk)
                chunk_count += 1
            sha = hasher.hexdigest()
        with self.conn.transaction():
            inserted = self.conn.execute(
                '''INSERT INTO okg.source_record_authority_payloads
                   (sha256,payload_kind,byte_count,chunk_count)
                   VALUES (%s,'json',%s,%s) ON CONFLICT DO NOTHING
                   RETURNING sha256''',
                (sha, byte_count, chunk_count),
            ).fetchone()
            actual = self.conn.execute(
                '''SELECT payload_kind,byte_count,chunk_count
                   FROM okg.source_record_authority_payloads WHERE sha256=%s''',
                (sha,),
            ).fetchone()
            if actual != ('json', byte_count, chunk_count):
                raise AuthorityError('conflicting content-addressed payload')
            if inserted is None:
                stored_count = self.conn.execute(
                    '''SELECT count(*)
                       FROM okg.source_record_authority_payload_chunks
                       WHERE sha256=%s''', (sha,),
                ).fetchone()[0]
                if stored_count != chunk_count:
                    raise AuthorityError('incomplete shared authority payload')
                return sha
            with self.conn.cursor() as cur:
                with cur.copy(
                    'COPY okg.source_record_authority_payload_chunks '
                    '(sha256,chunk_ordinal,payload) FROM STDIN (FORMAT BINARY)'
                ) as copy:
                    copy.set_types(('text', 'int8', 'bytea'))
                    for ordinal, chunk in enumerate(_coalesced_json_chunks(value)):
                        if len(chunk) > LIMIT:
                            raise AuthorityError(
                                'payload encoder exceeded SQL chunk bound'
                            )
                        self.max_parameter_bytes = max(
                            self.max_parameter_bytes, len(chunk)
                        )
                        copy.write_row((sha, ordinal, chunk))
        return sha

    def _buffer_record(
        self,
        batch: '_StageBatch',
        ordinal: int,
        record_id: str,
        primary: Mapping[str, Any] | None,
        descendants: Iterable[Mapping[str, Any]],
        edges: Iterable[Mapping[str, Any]],
    ) -> None:
        if ordinal < 0 or not record_id or len(record_id.encode()) > 2048:
            raise AuthorityError('invalid ordinal or bounded record identity')
        components: list[tuple[str, int, tuple[tuple[str, str], ...]]] = []
        identities: list[tuple[str, str, str]] = []
        streams = (
            ('primary', () if primary is None else (primary,)),
            ('descendant', descendants),
            ('edge', edges),
        )
        for component_kind, values in streams:
            for component_ordinal, value in enumerate(values):
                if not isinstance(value, Mapping) or 'source_revision' not in value:
                    raise AuthorityError('authority component has no source_revision')
                fields = self._buffer_component_payloads(batch, value)
                if len(fields) != len(value):
                    raise AuthorityError('authority component fields require names')
                components.append((component_kind, component_ordinal, fields))
                payload = value
                if component_kind == 'edge':
                    identity = canonical([
                        payload.get('src'), payload.get('edge_type'), payload.get('dst')
                    ])
                    identities.append(('edge', identity, ''))
                else:
                    node_id = payload.get('node_id')
                    if not isinstance(node_id, str) or not node_id:
                        raise AuthorityError('node component has no identity')
                    identities.append((
                        'node', node_id,
                        str(payload.get('ownership_mode', 'authoritative')),
                    ))
                _require_bounded_identity(identities[-1])
        operation_value = {
            'ordinal': ordinal,
            'record_id': record_id,
            'operation': 'upsert',
            'components': components,
            'identities': identities,
        }
        batch.add_operation(
            ordinal, record_id, 'upsert',
            digest(canonical(operation_value)), len(components),
        )
        for kind, index, fields in components:
            batch.add_component(ordinal, kind, index, fields)
        for kind, identity, ownership in identities:
            batch.add_identity(ordinal, kind, identity, ownership)

    def _buffer_retraction(
        self, batch: '_StageBatch', ordinal: int, record_id: str,
    ) -> None:
        operation_sha = digest(canonical({
            'ordinal': ordinal, 'record_id': record_id,
            'operation': 'retract', 'components': [], 'identities': [],
        }))
        batch.add_operation(ordinal, record_id, 'retract', operation_sha, 0)

    def stage_record(
        self,
        authority: str,
        ordinal: int,
        record_id: str,
        primary: Mapping[str, Any] | None,
        descendants: Iterable[Mapping[str, Any]],
        edges: Iterable[Mapping[str, Any]],
    ) -> None:
        """Stage exactly one upsert operation as its own batch."""

        self._require_available_connection()
        batch = _StageBatch(authority)
        self._buffer_record(
            batch, ordinal, record_id, primary, descendants, edges
        )
        self._flush_batch(batch)

    def stage_retraction(self, authority: str, ordinal: int, record_id: str) -> None:
        """Stage exactly one retraction as its own batch."""

        self._require_available_connection()
        batch = _StageBatch(authority)
        self._buffer_retraction(batch, ordinal, record_id)
        self._flush_batch(batch)

    def _load_bounded_payload(self, sha: str, *, max_bytes: int) -> Any:
        """Decode one payload that MUST already be small.

        Only identity fields (node_id, ownership_mode, src, edge_type, dst)
        reach this path.  ``max_bytes`` is enforced while the chunk stream is
        still being consumed, so a hostile or oversized field is refused
        instead of being buffered.  There is no whole-record decoder: a large
        component is only ever streamed as bytes.
        """

        if max_bytes <= 0 or max_bytes > IDENTITY_FIELD_MAX_BYTES:
            raise AuthorityError('invalid bounded payload byte ceiling')
        encoded = bytearray()
        for chunk in self._iter_payload_bytes(sha):
            encoded.extend(chunk)
            if len(encoded) > max_bytes:
                raise AuthorityError('authority identity field exceeds byte bound')
        try:
            return json.loads(bytes(encoded))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityError('invalid authority payload JSON') from exc

    def _iter_payload_bytes(
        self, sha: str, header: tuple | None = None, inline: bytes | None = None,
    ) -> Iterator[bytes]:
        """Stream one content-addressed payload, one stored chunk at a time.

        ``header`` and ``inline`` let a caller that already selected the single
        chunk of a small payload skip two round trips. Every path still hashes
        what it yields and compares byte and chunk counts against the header,
        so a shortcut cannot skip an integrity check.
        """

        if header is None:
            header = self.conn.execute(
                """SELECT payload_kind,byte_count,chunk_count
                   FROM okg.source_record_authority_payloads WHERE sha256=%s""",
                (sha,),
            ).fetchone()
        if header is None or header[0] != 'json':
            raise AuthorityError('missing authority field payload')
        hasher = hashlib.sha256()
        count = size = 0
        for chunk in self._payload_chunks(sha, header, inline):
            if len(chunk) > LIMIT:
                raise AuthorityError('oversized authority chunk')
            hasher.update(chunk)
            size += len(chunk)
            count += 1
            yield chunk
        if count != header[2] or size != header[1] or hasher.hexdigest() != sha:
            raise AuthorityError('corrupt content-addressed payload')

    def _payload_chunks(
        self, sha: str, header: tuple, inline: bytes | None,
    ) -> Iterator[bytes]:
        if header[2] == 1:
            # One chunk is bounded by LIMIT by construction, so a plain
            # statement transfers no more than a server cursor would.
            if inline is not None:
                yield bytes(inline)
                return
            row = self.conn.execute(
                """SELECT payload FROM okg.source_record_authority_payload_chunks
                   WHERE sha256=%s AND chunk_ordinal=0""",
                (sha,),
            ).fetchone()
            if row is None:
                raise AuthorityError('missing or disordered authority chunk')
            yield bytes(row[0])
            return
        expected = 0
        with self.conn.cursor(name='authority_field_' + uuid.uuid4().hex) as cur:
            # One row at a time. Every other cursor in this module fetches
            # small rows, but a payload chunk is up to LIMIT bytes, so a
            # READ_PAGE prefetch would hold READ_PAGE * LIMIT bytes and
            # reintroduce whole-field memory for a large field.
            cur.itersize = 1
            cur.execute(
                """SELECT chunk_ordinal,payload
                   FROM okg.source_record_authority_payload_chunks
                   WHERE sha256=%s ORDER BY chunk_ordinal""", (sha,),
            )
            for ordinal, raw in cur:
                if ordinal != expected:
                    raise AuthorityError('missing or disordered authority chunk')
                expected += 1
                yield bytes(raw)

    def _iter_component_bytes(
        self, authority: str, operation: int, kind: str, component: int,
    ) -> Iterator[bytes]:
        """Stream one component object.

        A component holds a handful of named fields, so the field list itself
        is fetched with one plain statement. A field whose payload is a single
        chunk at or below ``INLINE_FIELD_BYTES`` comes back in that same row;
        anything larger is streamed chunk by chunk. Peak held bytes therefore
        stay proportional to the field count, never to the field size.
        """

        yield b'{'
        rows = self.conn.execute(
            """SELECT f.field_name,f.payload_sha256,p.payload_kind,
                      p.byte_count,p.chunk_count,
                      CASE WHEN p.chunk_count=1 AND p.byte_count<=%s
                           THEN (SELECT c.payload
                                   FROM okg.source_record_authority_payload_chunks c
                                  WHERE c.sha256=p.sha256 AND c.chunk_ordinal=0)
                      END
                 FROM okg.source_record_authority_component_fields f
                 JOIN okg.source_record_authority_payloads p
                   ON p.sha256=f.payload_sha256
                WHERE f.authority_id=%s AND f.operation_ordinal=%s
                  AND f.component_kind=%s AND f.component_ordinal=%s
                ORDER BY f.field_name COLLATE "C" """,
            (INLINE_FIELD_BYTES, authority, operation, kind, component),
        ).fetchall()
        seen = 0
        previous: str | None = None
        for field_name, payload_sha, kind_, byte_count, chunk_count, inline in rows:
            field_name = str(field_name)
            if previous is not None and field_name <= previous:
                raise AuthorityError('duplicate or disordered component field')
            if seen:
                yield b','
            yield from _iter_json(field_name)
            yield b':'
            yield from self._iter_payload_bytes(
                str(payload_sha),
                header=(kind_, byte_count, chunk_count),
                inline=None if inline is None else bytes(inline),
            )
            previous = field_name
            seen += 1
        if not seen:
            raise AuthorityError('authority component has no fields')
        yield b'}'

    def _iter_component_array_bytes(
        self, authority: str, operation: int, kind: str,
    ) -> Iterator[bytes]:
        """Stream one component array.

        Most records have a handful of descendants or edges, and a server
        cursor costs a DECLARE/FETCH/CLOSE round trip per record per kind. One
        bounded probe answers the common case; a record with more components
        than a page falls back to the cursor, so the memory bound is unchanged.
        """

        yield b'['
        probe = self.conn.execute(
            """SELECT component_ordinal
               FROM okg.source_record_authority_components
               WHERE authority_id=%s AND operation_ordinal=%s
                 AND component_kind=%s ORDER BY component_ordinal
               LIMIT %s""",
            (authority, operation, kind, READ_PAGE + 1),
        ).fetchall()
        if len(probe) <= READ_PAGE:
            ordinals: Iterable[tuple[int]] = probe
            expected = 0
            for (ordinal,) in ordinals:
                if ordinal != expected:
                    raise AuthorityError('missing or disordered authority component')
                if expected:
                    yield b','
                yield from self._iter_component_bytes(
                    authority, operation, kind, ordinal
                )
                expected += 1
            yield b']'
            return
        expected = 0
        with self.conn.cursor(
            name='authority_component_index_' + uuid.uuid4().hex,
        ) as cur:
            cur.itersize = READ_PAGE
            cur.execute(
                """SELECT component_ordinal
                   FROM okg.source_record_authority_components
                   WHERE authority_id=%s AND operation_ordinal=%s
                     AND component_kind=%s ORDER BY component_ordinal""",
                (authority, operation, kind),
            )
            for (ordinal,) in cur:
                if ordinal != expected:
                    raise AuthorityError('missing or disordered authority component')
                if expected:
                    yield b','
                yield from self._iter_component_bytes(
                    authority, operation, kind, ordinal
                )
                expected += 1
        yield b']'

    def _iter_stored_record_set_bytes(self, authority: str) -> Iterator[bytes]:
        """Emit the whole canonical authority from one ordered server cursor.

        The per-record reader below costs a round trip per component array and
        another per component; sealing 200k records paid that twice, which is
        where the seal time went.  This sweep asks for every component field of
        every record once, already in canonical order -- record id, then
        component kind (``descendant`` < ``edge`` < ``primary`` alphabetically,
        which is the canonical key order), then component ordinal, then field
        name -- and re-derives exactly the same bytes from it.

        Order and completeness are still checked here rather than trusted: a
        record id out of order, a gap in a component sequence, a repeated or
        disordered field name, a component with no fields and a primary at any
        ordinal but zero all refuse, the same as the per-record path.

        Memory: a row carries its field's bytes only when the field is at or
        under ``SWEEP_INLINE_BYTES``, so a prefetch of ``SWEEP_FETCH_ROWS``
        holds under one chunk.  A larger field arrives as a null and is
        streamed by ``_iter_payload_bytes`` a chunk at a time, so peak held
        payload bytes are unchanged.
        """

        yield b'{"records":{'
        record_id: str | None = None
        previous_record: str | None = None
        first_record = True
        open_rank = -1
        saw_component = False
        component_ordinal: int | None = None
        expected_ordinal = 0
        field_count = 0
        previous_field: str | None = None

        def close_component() -> Iterator[bytes]:
            nonlocal component_ordinal, field_count, previous_field
            if component_ordinal is None:
                return
            if not field_count:
                raise AuthorityError('authority component has no fields')
            component_ordinal = None
            field_count = 0
            previous_field = None
            yield b'}'

        def close_open_kind() -> Iterator[bytes]:
            if open_rank < 0:
                return
            if open_rank == _PRIMARY_RANK:
                if not saw_component:
                    yield b'null'
            else:
                yield b']'

        def open_through(rank: int) -> Iterator[bytes]:
            """Close the open kind and open every kind up to ``rank``."""

            nonlocal open_rank, saw_component, expected_ordinal
            while open_rank < rank:
                yield from close_open_kind()
                open_rank += 1
                saw_component = False
                expected_ordinal = 0
                if open_rank:
                    yield b','
                yield _KIND_PREFIX[open_rank]

        def finish_record() -> Iterator[bytes]:
            yield from close_component()
            yield from open_through(_PRIMARY_RANK)
            yield from close_open_kind()
            yield b'}'

        with self.conn.cursor(name='authority_canonical_' + uuid.uuid4().hex) as cur:
            cur.itersize = SWEEP_FETCH_ROWS
            cur.execute(
                '''SELECT r.record_id,c.component_kind,c.component_ordinal,
                          f.field_name,f.payload_sha256,p.payload_kind,
                          p.byte_count,p.chunk_count,
                          CASE WHEN p.chunk_count=1 AND p.byte_count<=%s
                               THEN (SELECT ch.payload
                                       FROM okg.source_record_authority_payload_chunks ch
                                      WHERE ch.sha256=p.sha256
                                        AND ch.chunk_ordinal=0)
                          END
                     FROM okg.source_record_authority_records r
                     JOIN okg.source_record_authority_operations o
                       ON o.authority_id=r.authority_id
                      AND o.ordinal=r.operation_ordinal
                     LEFT JOIN okg.source_record_authority_components c
                       ON c.authority_id=r.authority_id
                      AND c.operation_ordinal=r.operation_ordinal
                     LEFT JOIN okg.source_record_authority_component_fields f
                       ON f.authority_id=c.authority_id
                      AND f.operation_ordinal=c.operation_ordinal
                      AND f.component_kind=c.component_kind
                      AND f.component_ordinal=c.component_ordinal
                     LEFT JOIN okg.source_record_authority_payloads p
                       ON p.sha256=f.payload_sha256
                    WHERE r.authority_id=%s AND o.operation='upsert'
                    ORDER BY r.record_id COLLATE "C",c.component_kind,
                             c.component_ordinal,f.field_name COLLATE "C" ''',
                (SWEEP_INLINE_BYTES, authority),
            )
            for row in cur:
                if str(row[0]) != record_id:
                    if record_id is not None:
                        yield from finish_record()
                    record_id = str(row[0])
                    if previous_record is not None and record_id <= previous_record:
                        raise AuthorityError(
                            'duplicate or disordered authority record'
                        )
                    previous_record = record_id
                    if not first_record:
                        yield b','
                    first_record = False
                    yield from _iter_json(record_id)
                    yield b':{'
                    open_rank = -1
                    saw_component = False
                    component_ordinal = None
                    expected_ordinal = 0
                    field_count = 0
                    previous_field = None
                if row[1] is None:
                    continue
                rank = _KIND_RANK.get(str(row[1]))
                if rank is None:
                    raise AuthorityError('invalid authority component kind')
                if rank < open_rank:
                    raise AuthorityError('missing or disordered authority component')
                if rank != open_rank:
                    yield from close_component()
                    yield from open_through(rank)
                ordinal = int(row[2])
                if ordinal != component_ordinal:
                    yield from close_component()
                    if rank == _PRIMARY_RANK and ordinal != 0:
                        raise AuthorityError('invalid primary component index')
                    if ordinal != expected_ordinal:
                        raise AuthorityError(
                            'missing or disordered authority component'
                        )
                    expected_ordinal += 1
                    if saw_component:
                        yield b','
                    saw_component = True
                    component_ordinal = ordinal
                    yield b'{'
                if row[3] is None:
                    raise AuthorityError('authority component has no fields')
                field_name = str(row[3])
                if previous_field is not None and field_name <= previous_field:
                    raise AuthorityError('duplicate or disordered component field')
                if field_count:
                    yield b','
                yield from _iter_json(field_name)
                yield b':'
                yield from self._iter_payload_bytes(
                    str(row[4]),
                    header=(
                        None if row[5] is None else str(row[5]),
                        None if row[6] is None else int(row[6]),
                        None if row[7] is None else int(row[7]),
                    ),
                    inline=None if row[8] is None else bytes(row[8]),
                )
                previous_field = field_name
                field_count += 1
        if record_id is not None:
            yield from finish_record()
        yield b'}}'

    def _iter_record_ids(self, authority: str) -> Iterator[str]:
        previous: str | None = None
        with self.conn.cursor(name='authority_ids_' + uuid.uuid4().hex) as cur:
            cur.itersize = READ_PAGE
            cur.execute(
                """SELECT r.record_id
                   FROM okg.source_record_authority_records r
                   JOIN okg.source_record_authority_operations o
                     ON o.authority_id=r.authority_id
                    AND o.ordinal=r.operation_ordinal
                   WHERE r.authority_id=%s AND o.operation='upsert'
                   ORDER BY r.record_id COLLATE "C" """,
                (authority,),
            )
            for (record_id,) in cur:
                record_id = str(record_id)
                if previous is not None and record_id <= previous:
                    raise AuthorityError('duplicate or disordered authority record')
                previous = record_id
                yield record_id

    def _iter_record_byte_streams(
        self, authority: str,
    ) -> Iterator[tuple[str, Iterator[bytes]]]:
        """Yield one record id plus its own bounded canonical byte stream.

        The pairs come from a server cursor and each byte stream is a separate
        bounded generator, so a consumer that reads one multi-gigabyte record
        still only holds one chunk at a time.
        """

        previous: str | None = None
        with self.conn.cursor(
            name='authority_record_streams_' + uuid.uuid4().hex,
        ) as cur:
            cur.itersize = READ_PAGE
            cur.execute(
                """SELECT r.record_id,r.operation_ordinal
                   FROM okg.source_record_authority_records r
                   JOIN okg.source_record_authority_operations o
                     ON o.authority_id=r.authority_id
                    AND o.ordinal=r.operation_ordinal
                   WHERE r.authority_id=%s AND o.operation='upsert'
                   ORDER BY r.record_id COLLATE "C" """,
                (authority,),
            )
            for record_id, operation in cur:
                record_id = str(record_id)
                if previous is not None and record_id <= previous:
                    raise AuthorityError('duplicate or disordered authority record')
                previous = record_id
                yield record_id, self._iter_record_body_bytes(authority, operation)

    def _iter_record_body_bytes(
        self, authority: str, operation: int,
    ) -> Iterator[bytes]:
        yield b'{"descendants":'
        yield from self._iter_component_array_bytes(authority, operation, 'descendant')
        yield b',"edges":'
        yield from self._iter_component_array_bytes(authority, operation, 'edge')
        yield b',"primary":'
        yield from self._iter_primary_bytes(authority, operation)
        yield b'}'

    def _iter_primary_bytes(self, authority: str, operation: int) -> Iterator[bytes]:
        primary = list(self.conn.execute(
            """SELECT component_ordinal
               FROM okg.source_record_authority_components
               WHERE authority_id=%s AND operation_ordinal=%s
                 AND component_kind='primary' """,
            (authority, operation),
        ))
        if not primary:
            yield b'null'
        elif primary == [(0,)]:
            yield from self._iter_component_bytes(authority, operation, 'primary', 0)
        else:
            raise AuthorityError('invalid primary component index')

    @contextmanager
    def _identity_cursor(self, authority: str):
        with self.conn.cursor(
            name='authority_identity_rows_' + uuid.uuid4().hex,
        ) as cur:
            cur.itersize = READ_PAGE
            cur.execute(
                """SELECT i.identity_kind,i.identity,i.ownership
                   FROM okg.source_record_authority_identities i
                   JOIN okg.source_record_authority_records r
                     ON r.authority_id=i.authority_id
                    AND r.operation_ordinal=i.operation_ordinal
                   JOIN okg.source_record_authority_operations o
                     ON o.authority_id=r.authority_id
                    AND o.ordinal=r.operation_ordinal
                   WHERE i.authority_id=%s AND o.operation='upsert'
                   ORDER BY i.identity_kind,i.identity COLLATE "C",i.ownership""",
                (authority,),
            )
            yield cur

    def _iter_identities(self, authority: str) -> Iterator[tuple[str, str, str]]:
        with self._identity_cursor(authority) as cur:
            for kind, identity, ownership in cur:
                yield str(kind), str(identity), str(ownership)

    def _missing_production_identities(
        self, authority: str, expected: Iterable[tuple[str, str]],
    ) -> Iterator[tuple[str, str]]:
        """Anti-join without a Python set of the authority or of ``expected``."""

        self.conn.execute(
            """CREATE TEMP TABLE IF NOT EXISTS authority_expected_lookup (
                 identity_kind TEXT NOT NULL,
                 identity TEXT NOT NULL,
                 PRIMARY KEY(identity_kind,identity)
               ) ON COMMIT DROP"""
        )
        self.conn.execute('TRUNCATE authority_expected_lookup')
        batch: list[tuple[str, str]] = []
        for kind, identity in expected:
            if kind not in ('node', 'edge') or not isinstance(identity, str):
                raise AuthorityError('invalid expected identity')
            if len(identity.encode()) > IDENTITY_FIELD_MAX_BYTES:
                raise AuthorityError('expected identity exceeds byte bound')
            batch.append((kind, identity))
            if len(batch) >= IDENTITY_BATCH_ROWS:
                self._insert_expected_lookup(batch)
                batch.clear()
        if batch:
            self._insert_expected_lookup(batch)
        with self.conn.cursor(name='authority_missing_' + uuid.uuid4().hex) as cur:
            cur.itersize = READ_PAGE
            cur.execute(
                """SELECT e.identity_kind,e.identity
                   FROM authority_expected_lookup e
                   WHERE NOT EXISTS (
                     SELECT 1 FROM okg.source_record_authority_identities i
                     JOIN okg.source_record_authority_records r
                       ON r.authority_id=i.authority_id
                      AND r.operation_ordinal=i.operation_ordinal
                     JOIN okg.source_record_authority_operations o
                       ON o.authority_id=r.authority_id
                      AND o.ordinal=r.operation_ordinal
                     WHERE i.authority_id=%s AND o.operation='upsert'
                       AND i.identity_kind=e.identity_kind
                       AND i.identity=e.identity)
                   ORDER BY e.identity_kind,e.identity COLLATE "C" """,
                (authority,),
            )
            for kind, identity in cur:
                yield str(kind), str(identity)

    def _insert_expected_lookup(self, batch: list[tuple[str, str]]) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                'INSERT INTO authority_expected_lookup VALUES (%s,%s) '
                'ON CONFLICT DO NOTHING',
                batch,
            )

    @staticmethod
    def _validate_private_candidate(candidate: PrivateAuthorityCandidate) -> None:
        if not isinstance(candidate, PrivateAuthorityCandidate):
            raise AuthorityError('invalid private authority candidate')
        if not isinstance(candidate.authority, str):
            raise AuthorityError('invalid private authority UUID')
        try:
            uuid.UUID(candidate.authority)
        except ValueError as exc:
            raise AuthorityError('invalid private authority UUID') from exc
        if any(not isinstance(value, str) or not value or len(value.encode()) > 2048
               for value in (candidate.source, candidate.incarnation, candidate.attempt)):
            raise AuthorityError('invalid bounded authority identity')
        sha = candidate.expected_sha256
        if (not isinstance(sha, str) or len(sha) != 64
                or any(char not in '0123456789abcdef' for char in sha)
                or any(type(count) is not int or count < 0 for count in (
                    candidate.expected_records, candidate.expected_operations,
                    candidate.expected_components,
                ))):
            raise AuthorityError('invalid private authority expectations')

    def _verify_candidate(
        self, candidate: PrivateAuthorityCandidate,
    ) -> tuple[str, int, int]:
        self._validate_private_candidate(candidate)
        header = self._header(candidate.authority, lock=True)
        if header[:4] != (
            candidate.source, candidate.incarnation, candidate.attempt, 'staging'
        ):
            raise AuthorityError('stale, sealed or mismatched private candidate')
        stats = self.conn.execute(
            '''SELECT count(*),min(ordinal),max(ordinal),
                      COALESCE(sum(component_count),0)
               FROM okg.source_record_authority_operations
               WHERE authority_id=%s''',
            (candidate.authority,),
        ).fetchone()
        expected_sequence = (
            candidate.expected_operations,
            0 if candidate.expected_operations else None,
            candidate.expected_operations - 1 if candidate.expected_operations else None,
            candidate.expected_components,
        )
        if stats != expected_sequence:
            raise AuthorityError('incomplete authority operation sequence')
        # Authenticate structure for every operation, including superseded and
        # retracted history. Historical payload contents are not rehashed: the
        # canonical digest below covers the live record set only.
        component_mismatch = self.conn.execute(
            """SELECT 1
                 FROM (SELECT ordinal,operation,component_count
                         FROM okg.source_record_authority_operations
                        WHERE authority_id=%s) o
                 FULL JOIN (
                   SELECT operation_ordinal AS ordinal,count(*) AS physical_count
                     FROM okg.source_record_authority_components
                    WHERE authority_id=%s GROUP BY operation_ordinal
                 ) c USING (ordinal)
                WHERE o.component_count IS DISTINCT FROM coalesce(c.physical_count,0)
                   OR (o.operation='retract' AND o.component_count<>0)
                LIMIT 1""", (candidate.authority, candidate.authority),
        ).fetchone()
        if component_mismatch:
            raise AuthorityError('authority physical component count mismatch')
        self._verify_identity_index(candidate.authority)
        hasher = hashlib.sha256()
        observed_bytes = 0
        for chunk in self._iter_stored_record_set_bytes(candidate.authority):
            hasher.update(chunk)
            observed_bytes += len(chunk)
        observed_sha = hasher.hexdigest()
        observed_count = self.conn.execute(
            'SELECT count(*) FROM okg.source_record_authority_records '
            'WHERE authority_id=%s', (candidate.authority,),
        ).fetchone()[0]
        if (observed_sha, observed_count) != (
            candidate.expected_sha256, candidate.expected_records
        ):
            raise AuthorityError('authority logical digest or count mismatch')
        return observed_sha, observed_count, observed_bytes

    IDENTITY_FIELD_NAMES = ('dst', 'edge_type', 'node_id', 'ownership_mode', 'src')

    def _identity_field_value(
        self, sha: str, kind: str | None, byte_count: int | None,
        chunk_count: int | None, chunk: bytes | None,
    ) -> Any:
        """Decode one identity field from the row that already carries it.

        This is ``_load_bounded_payload`` without its two round trips: the same
        header check, the same byte ceiling, and the same content-address check
        against the stored bytes, applied to a chunk the identity sweep already
        fetched.
        """

        if kind is None or kind != 'json':
            raise AuthorityError('missing authority field payload')
        if chunk_count != 1 or byte_count is None:
            raise AuthorityError('authority identity field exceeds byte bound')
        if byte_count > IDENTITY_FIELD_MAX_BYTES:
            raise AuthorityError('authority identity field exceeds byte bound')
        if chunk is None:
            raise AuthorityError('missing or disordered authority chunk')
        encoded = bytes(chunk)
        if len(encoded) != byte_count or hashlib.sha256(encoded).hexdigest() != sha:
            raise AuthorityError('corrupt content-addressed payload')
        try:
            return json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityError('invalid authority payload JSON') from exc

    def _expected_identity(
        self, kind: str, values: Mapping[str, Any],
    ) -> tuple[str, str, str]:
        if kind == 'edge':
            wanted = ('src', 'edge_type', 'dst')
            if any(not isinstance(values.get(key), str) or not values[key]
                   for key in wanted):
                raise AuthorityError('edge component has invalid identity')
            return (
                'edge',
                canonical([values['src'], values['edge_type'], values['dst']]),
                '',
            )
        node_id = values.get('node_id')
        if not isinstance(node_id, str) or not node_id:
            raise AuthorityError('node component has invalid identity')
        return (
            'node', node_id, str(values.get('ownership_mode', 'authoritative')),
        )

    def _verify_identity_index(self, authority: str) -> None:
        """Rebuild only bounded identity fields and compare both index directions.

        The rebuild is one server cursor over every identity field of the
        authority, joined to the single stored chunk each one occupies, and one
        COPY per bounded batch of rebuilt identities. Every check the per-field
        rebuild made is still made, on the same stored bytes; what is gone is
        the round trip per component and the two round trips per field.
        """

        self.conn.execute(
            '''CREATE TEMP TABLE IF NOT EXISTS authority_expected_identities (
                 operation_ordinal BIGINT NOT NULL,
                 identity_kind TEXT NOT NULL,
                 identity TEXT NOT NULL,
                 ownership TEXT NOT NULL
               ) ON COMMIT DROP'''
        )
        self.conn.execute('TRUNCATE authority_expected_identities')
        batch: list[tuple[int, str, str, str]] = []
        current: tuple[int, str, int] | None = None
        values: dict[str, Any] = {}
        with self.conn.cursor(
            name='authority_identity_fields_' + uuid.uuid4().hex,
        ) as cur:
            # Each row carries at most one identity field, so the prefetch
            # holds IDENTITY_FETCH_ROWS * IDENTITY_FIELD_MAX_BYTES, well under
            # the one-chunk bound the reader promises.
            cur.itersize = IDENTITY_FETCH_ROWS
            cur.execute(
                '''SELECT c.operation_ordinal,c.component_kind,c.component_ordinal,
                          f.field_name,f.payload_sha256,p.payload_kind,
                          p.byte_count,p.chunk_count,
                          CASE WHEN p.chunk_count=1
                                AND p.byte_count<=%s
                               THEN (SELECT ch.payload
                                       FROM okg.source_record_authority_payload_chunks ch
                                      WHERE ch.sha256=p.sha256
                                        AND ch.chunk_ordinal=0)
                          END
                     FROM okg.source_record_authority_components c
                     JOIN okg.source_record_authority_records r
                       ON r.authority_id=c.authority_id
                      AND r.operation_ordinal=c.operation_ordinal
                     LEFT JOIN okg.source_record_authority_component_fields f
                       ON f.authority_id=c.authority_id
                      AND f.operation_ordinal=c.operation_ordinal
                      AND f.component_kind=c.component_kind
                      AND f.component_ordinal=c.component_ordinal
                      AND f.field_name=ANY(%s)
                     LEFT JOIN okg.source_record_authority_payloads p
                       ON p.sha256=f.payload_sha256
                    WHERE c.authority_id=%s
                    ORDER BY c.operation_ordinal,c.component_kind,
                             c.component_ordinal,f.field_name''',
                (IDENTITY_FIELD_MAX_BYTES, list(self.IDENTITY_FIELD_NAMES),
                 authority),
            )
            for row in cur:
                key = (int(row[0]), str(row[1]), int(row[2]))
                if current is not None and key != current:
                    batch.append(
                        (current[0],)
                        + self._expected_identity(current[1], values)
                    )
                    values = {}
                    if len(batch) >= IDENTITY_BATCH_ROWS:
                        self._copy_expected_identities(batch)
                        batch.clear()
                current = key
                if row[3] is None:
                    continue
                field_name = str(row[3])
                wanted = (
                    ('src', 'edge_type', 'dst')
                    if key[1] == 'edge' else ('node_id', 'ownership_mode')
                )
                if field_name not in wanted:
                    continue
                values[field_name] = self._identity_field_value(
                    str(row[4]), None if row[5] is None else str(row[5]),
                    None if row[6] is None else int(row[6]),
                    None if row[7] is None else int(row[7]), row[8],
                )
        if current is not None:
            batch.append((current[0],) + self._expected_identity(current[1], values))
        if batch:
            self._copy_expected_identities(batch)
        mismatch = self.conn.execute(
            '''WITH actual AS (
                 SELECT i.operation_ordinal,i.identity_kind,i.identity,i.ownership
                   FROM okg.source_record_authority_identities i
                   JOIN okg.source_record_authority_records r
                     ON r.authority_id=i.authority_id
                    AND r.operation_ordinal=i.operation_ordinal
                  WHERE i.authority_id=%s
               )
               SELECT 1 FROM (
                 (SELECT * FROM authority_expected_identities EXCEPT SELECT * FROM actual)
                 UNION ALL
                 (SELECT * FROM actual EXCEPT SELECT * FROM authority_expected_identities)
               ) difference LIMIT 1''', (authority,),
        ).fetchone()
        if mismatch:
            raise AuthorityError('corrupt authority identity index')

    def _copy_expected_identities(
        self, batch: list[tuple[int, str, str, str]],
    ) -> None:
        # EXCEPT below compares sets in both directions. Duplicate component
        # identities therefore need no indexed staging table or per-batch
        # dedupe INSERT: copy directly into the bounded reader's spill table.
        self._copy_rows(
            'authority_expected_identities',
            '(operation_ordinal,identity_kind,identity,ownership)',
            ('int8', 'text', 'text', 'text'), batch,
        )

    def seal_and_bind(
        self,
        candidate: PrivateAuthorityCandidate,
        *,
        bind_hook=None,
    ) -> ProductionReference:
        """Revalidate, seal and publish a v2 reference in one transaction.

        ``bind_hook`` runs inside the same transaction so the runner/service can
        compare-and-swap its watermark or terminal admission state. Any hook
        failure rolls back both the seal and public reference.
        """

        self._require_available_connection()
        with self.conn.transaction():
            sha, count, logical_bytes = self._verify_candidate(candidate)
            self.conn.execute(
                '''UPDATE okg.source_record_authorities
                   SET state='sealed',logical_sha256=%s,record_count=%s,
                       operation_count=%s,component_count=%s,
                       logical_bytes=%s,sealed_at=now()
                   WHERE authority_id=%s AND state='staging' ''',
                (sha, count, candidate.expected_operations,
                 candidate.expected_components, logical_bytes,
                 candidate.authority),
            )
            self.conn.execute(
                '''INSERT INTO okg.source_record_authority_refs
                   (source,authority_id,incarnation,logical_sha256,
                    record_count,authority_state)
                   VALUES (%s,%s,%s,%s,%s,'sealed')
                   ON CONFLICT(source) DO UPDATE SET
                     authority_id=excluded.authority_id,
                     incarnation=excluded.incarnation,
                     logical_sha256=excluded.logical_sha256,
                     record_count=excluded.record_count,
                     authority_state=excluded.authority_state,
                     bound_at=now()''',
                (candidate.source, candidate.authority,
                 candidate.incarnation, sha, count),
            )
            reference = ProductionReference(
                candidate.authority, candidate.source,
                candidate.incarnation, sha, count,
            )
            if bind_hook is not None:
                bind_hook(reference)
            return reference

    def _verify_current_reference(self, reference: ProductionReference) -> None:
        """Require the exact current public binding, never an orphaned seal."""

        row = self.conn.execute(
            '''SELECT a.source,a.incarnation,a.logical_sha256,a.record_count
               FROM okg.source_record_authority_refs r
               JOIN okg.source_record_authorities a
                 ON a.authority_id=r.authority_id
                AND a.state=r.authority_state
               WHERE r.source=%s AND r.authority_id=%s
                 AND r.authority_state='sealed' ''',
            (reference.source, reference.authority),
        ).fetchone()
        if reference.format != PRODUCTION_FORMAT or row != (
            reference.source, reference.incarnation,
            reference.sha256, reference.count,
        ):
            raise AuthorityError('reference is not the exact current sealed authority')

    def _preverify_reader(self, reference: ProductionReference) -> None:
        """Authenticate the whole authority before the caller reads any of it.

        This streams the complete canonical bytes once to recompute the digest,
        so a caller that then streams them again pays the read twice. That is
        deliberate: handing out an unverified stream would let a corrupt
        authority be consumed before the mismatch was noticed.
        """

        self._verify_current_reference(reference)
        hasher = hashlib.sha256()
        for chunk in self._iter_stored_record_set_bytes(reference.authority):
            hasher.update(chunk)
        if hasher.hexdigest() != reference.sha256:
            raise AuthorityError('corrupt production authority')
        observed_count = self.conn.execute(
            'SELECT count(*) FROM okg.source_record_authority_records '
            'WHERE authority_id=%s', (reference.authority,),
        ).fetchone()[0]
        if observed_count != reference.count:
            raise AuthorityError('corrupt production authority record count')
        self._verify_identity_index(reference.authority)

    @contextmanager
    def reader(self, reference: ProductionReference):
        """Open the exact current authority after complete bounded verification.

        Historical authorities require a separate retained-reference API tied
        to an explicit generation/recovery root.  Accepting any sealed UUID here
        would make stale and orphaned authorities publicly readable.
        """

        with self._reader_transaction():
            self._preverify_reader(reference)
            verified = VerifiedProductionReader(self, reference)
            try:
                yield verified
            finally:
                verified._close()

    @contextmanager
    def private_identity_reader(
        self,
        candidate: PrivateAuthorityCandidate,
        *,
        expected_source: str,
        expected_incarnation: str,
        expected_attempt: str,
    ):
        """Verify an unsealed candidate before exposing any private identity.

        Bindings must come independently from the caller's active run context.
        The header lock remains until the enclosing transaction ends, including
        when this context uses a savepoint inside an existing stable transaction.
        Supported store entrypoints share connection exclusion; callers must
        not bypass it with raw SQL, commit/rollback, or another wrapper handle.
        Closing this view conveys no authority to a later seal: seal revalidates.
        """

        self._validate_private_candidate(candidate)
        if (expected_source, expected_incarnation, expected_attempt) != (
            candidate.source, candidate.incarnation, candidate.attempt,
        ):
            raise AuthorityError('private authority run binding mismatch')
        with self._reader_transaction():
            self._verify_candidate(candidate)
            # DECLARE before yielding, even when the caller never iterates.
            with self._identity_cursor(candidate.authority) as cursor:
                verified = VerifiedPrivateIdentityReader(self.conn, cursor)
                try:
                    yield verified
                finally:
                    verified._close()

    def iterate_canonical_bytes(
        self, reference: ProductionReference,
    ) -> Iterator[bytes]:
        """Read and authenticate a logical authority without decoding objects."""

        with self.reader(reference) as verified:
            yield from verified.iterate_canonical_bytes()
