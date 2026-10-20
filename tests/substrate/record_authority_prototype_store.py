"""Isolated prototype authority store, kept in the test tree on purpose.

This is the scaffolding that proved the bounded indexed contract before the
migrated ``okg`` tables existed. It creates its own ``authority_prototype``
schema, which the namespace contract (``scripts/check_schema_namespace.py``)
rightly refuses inside ``src/okg``: an application relation must be ``okg.``
qualified. The prototype is not an application relation and never ships, so it
lives here, next to the only suite that runs it.

Production code is ``okg.substrate.ingest.record_authority``. Nothing in
``src/okg`` imports this module.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable, Iterator

from psycopg.pq import TransactionStatus

from okg.substrate.ingest.record_authority import (
    LIMIT,
    MAX_FETCH_ROW_BYTES,
    READ_PAGE,
    AuthorityError,
    canonical,
    digest,
)
from okg.substrate.library.sources.base import RecordEmission

FORMAT = 'record-authority-prototype.v1'


@dataclass(frozen=True)
class Reference:
    authority: str
    source: str
    incarnation: str
    sha256: str
    count: int
    format: str = FORMAT



def install_prototype(conn) -> None:
    """Explicit opt-in for a disposable test DB; never a normal migration."""
    conn.execute('''
    CREATE SCHEMA IF NOT EXISTS authority_prototype;
    CREATE TABLE IF NOT EXISTS authority_prototype.sources (
      source text PRIMARY KEY, incarnation text NOT NULL);
    CREATE TABLE IF NOT EXISTS authority_prototype.authorities (
      id uuid PRIMARY KEY, source text NOT NULL, incarnation text NOT NULL,
      attempt text NOT NULL, sealed boolean NOT NULL DEFAULT false,
      sha256 text, count bigint, UNIQUE(source, incarnation, attempt));
    ALTER TABLE authority_prototype.authorities ADD COLUMN IF NOT EXISTS operation_count bigint;
    CREATE TABLE IF NOT EXISTS authority_prototype.revisions (
      sha256 text PRIMARY KEY, payload text NOT NULL);
    CREATE TABLE IF NOT EXISTS authority_prototype.operations (
      authority uuid NOT NULL REFERENCES authority_prototype.authorities(id),
      ordinal bigint NOT NULL, record_id text NOT NULL, sha256 text NOT NULL,
      payload jsonb, revisions jsonb NOT NULL,
      PRIMARY KEY(authority, ordinal));
    ALTER TABLE authority_prototype.operations ADD COLUMN IF NOT EXISTS identities jsonb;
    CREATE TABLE IF NOT EXISTS authority_prototype.records (
      authority uuid NOT NULL, record_id text COLLATE "C" NOT NULL,
      ordinal bigint NOT NULL, PRIMARY KEY(authority, record_id),
      FOREIGN KEY(authority, ordinal) REFERENCES authority_prototype.operations);
    CREATE INDEX IF NOT EXISTS authority_record_ordinal ON
      authority_prototype.records(authority, ordinal);
    CREATE TABLE IF NOT EXISTS authority_prototype.identities (
      authority uuid NOT NULL, ordinal bigint NOT NULL, kind text NOT NULL,
      identity text NOT NULL, ownership text NOT NULL,
      PRIMARY KEY(authority, ordinal, kind, identity, ownership),
      FOREIGN KEY(authority, ordinal) REFERENCES authority_prototype.operations);
    CREATE INDEX IF NOT EXISTS authority_identity_lookup ON
      authority_prototype.identities(authority, kind, identity);
    CREATE TABLE IF NOT EXISTS authority_prototype.refs (
      source text PRIMARY KEY, authority uuid NOT NULL
      REFERENCES authority_prototype.authorities(id));
    ''')


def logical_size(primary, descendants, edges) -> int:
    """Bound expanded legacy bytes without constructing a complete JSON string."""
    size = len('{"descendants":[],"edges":[],"primary":null}')
    encoder = json.JSONEncoder(sort_keys=True, default=str, separators=(',', ':'))
    def count(value):
        nonlocal size
        def strings(v):
            if isinstance(v, str) and len(v) > LIMIT:
                raise AuthorityError('prototype logical record exceeds byte bound')
            if isinstance(v, dict):
                for k, child in v.items():
                    strings(k)
                    strings(child)
            elif isinstance(v, (list, tuple)):
                for child in v:
                    strings(child)
        strings(value)
        for fragment in encoder.iterencode(value):
            size += len(fragment.encode())
            if size > LIMIT:
                raise AuthorityError('prototype logical record exceeds byte bound')
    for items in (descendants, edges):
        for index, item in enumerate(items):
            size += bool(index)
            count(item)
    if primary is not None:
        size -= 4
        count(primary)
    return size


class AuthorityStore:
    def __init__(self, conn):
        self.conn = conn
        self.max_parameter_bytes = 0
        self.max_batch_bytes = 0
        self.batch_count = 0

    def begin(self, source: str, incarnation: str, attempt: str) -> str:
        if any(not x or len(x.encode()) > 512 for x in (source, incarnation, attempt)):
            raise AuthorityError('invalid bounded authority identity')
        with self.conn.transaction():
            self.conn.execute('INSERT INTO authority_prototype.sources VALUES (%s,%s) '
                              'ON CONFLICT DO NOTHING', (source, incarnation))
            actual = self.conn.execute('SELECT incarnation FROM authority_prototype.sources '
                                       'WHERE source=%s FOR UPDATE', (source,)).fetchone()[0]
            if actual != incarnation:
                raise AuthorityError('stale incarnation')
            self.conn.execute('INSERT INTO authority_prototype.authorities '
                              '(id,source,incarnation,attempt) VALUES (%s,%s,%s,%s) '
                              'ON CONFLICT(source,incarnation,attempt) DO NOTHING',
                              (uuid.uuid4(), source, incarnation, attempt))
            return str(self.conn.execute('SELECT id FROM authority_prototype.authorities '
                                         'WHERE source=%s AND incarnation=%s AND attempt=%s',
                                         (source, incarnation, attempt)).fetchone()[0])

    def _lock(self, authority: str, *, for_update: bool = True):
        row = self.conn.execute('''SELECT a.source,a.incarnation,a.sealed,a.sha256,a.count
          FROM authority_prototype.authorities a
          JOIN authority_prototype.sources s USING(source)
          WHERE a.id=%s AND a.incarnation=s.incarnation''' + (' FOR UPDATE OF a,s' if for_update else ''),
                                (authority,)).fetchone()
        if row is None:
            raise AuthorityError('missing authority or stale incarnation')
        return row

    @staticmethod
    def _encode(ordinal: int, record_id: str, emission: RecordEmission | None):
        if ordinal < 0 or not record_id or len(record_id.encode()) > 2048:
            raise AuthorityError('invalid ordinal or bounded record identity')
        if emission is not None:
            logical_size(emission.primary.to_cursor() if emission.primary else None,
                         (n.to_cursor() for n in emission.descendants),
                         (e.to_cursor() for e in emission.edges))
        payload = emission.to_cursor() if emission is not None else None
        revisions = {}
        identities = []
        if payload is not None:
            nodes = ([payload['primary']] if payload['primary'] else []) + payload['descendants']
            for node in nodes:
                identities.append(['node', node['node_id'], node.get('ownership_mode', 'authoritative')])
            for edge in payload['edges']:
                identities.append(['edge', canonical([edge['src'], edge['edge_type'], edge['dst']]), ''])
            for item in nodes + payload['edges']:
                revision = canonical(item.pop('source_revision'))
                key = digest(revision)
                revisions[key] = revision
                item['revision_ref'] = key
        op = dict(ordinal=ordinal, record_id=record_id, payload=payload,
                  revisions=revisions, identities=identities)
        op['sha256'] = digest(canonical(op))
        encoded = canonical(op)
        if len(encoded.encode()) + 38 > LIMIT:
            raise AuthorityError('prototype individual record exceeds encoded byte bound')
        return encoded

    def stage(self, authority: str,
              operations: Iterable[tuple[int, str, RecordEmission | None]]) -> None:
        batch = []
        size = 2 + 36  # JSON brackets and UUID parameter
        for ordinal, key, emission in operations:
            encoded = self._encode(ordinal, key, emission)
            extra = len(encoded.encode()) + bool(batch)
            if size + extra > LIMIT:
                self._stage_batch(authority, '[' + ','.join(batch) + ']')
                batch, size = [], 38
                extra = len(encoded.encode())
            batch.append(encoded)
            size += extra
        if batch:
            self._stage_batch(authority, '[' + ','.join(batch) + ']')

    def _stage_batch(self, authority: str, payload: str) -> None:
        # Every SQL send uses only UUID + bounded JSON, or a UUID alone.
        n = len(payload.encode())
        if n + len(authority.encode()) > LIMIT:
            raise AuthorityError('encoded batch exceeds limit')
        self.max_parameter_bytes = max(self.max_parameter_bytes, n)
        self.max_batch_bytes = max(self.max_batch_bytes, n + len(authority.encode()))
        self.batch_count += 1
        with self.conn.transaction():
            row = self._lock(authority)
            if row[2]:
                raise AuthorityError('sealed authority is immutable')
            self.conn.execute('CREATE TEMP TABLE IF NOT EXISTS authority_batch '
                              '(v jsonb) ON COMMIT DROP')
            self.conn.execute('TRUNCATE authority_batch')
            self.conn.execute('INSERT INTO authority_batch SELECT value FROM jsonb_array_elements(%s::jsonb)', (payload,))
            if self.conn.execute('''SELECT 1 FROM authority_batch GROUP BY v->>'ordinal'
                HAVING count(*)>1 LIMIT 1''').fetchone():
                raise AuthorityError('duplicate ordinal within batch')
            if self.conn.execute('''SELECT 1 FROM authority_batch b JOIN
                authority_prototype.operations o ON o.authority=%s
                AND o.ordinal=(b.v->>'ordinal')::bigint
                WHERE o.sha256<>b.v->>'sha256' LIMIT 1''', (authority,)).fetchone():
                raise AuthorityError('conflicting attempt retry')
            self.conn.execute('''INSERT INTO authority_prototype.revisions
                SELECT DISTINCT r.key,r.value FROM authority_batch b,
                LATERAL jsonb_each_text(b.v->'revisions') r ON CONFLICT DO NOTHING''')
            if self.conn.execute('''SELECT 1 FROM authority_batch b,
                LATERAL jsonb_each_text(b.v->'revisions') r
                JOIN authority_prototype.revisions stored ON stored.sha256=r.key
                WHERE stored.payload<>r.value LIMIT 1''').fetchone():
                raise AuthorityError('corrupt shared revision')
            self.conn.execute('''INSERT INTO authority_prototype.operations
                (authority,ordinal,record_id,sha256,payload,revisions,identities)
                SELECT %s,(v->>'ordinal')::bigint,v->>'record_id',v->>'sha256',
                NULLIF(v->'payload','null'::jsonb),
                (SELECT COALESCE(jsonb_agg(key),'[]'::jsonb) FROM jsonb_each(v->'revisions')),v->'identities'
                FROM authority_batch ON CONFLICT DO NOTHING''', (authority,))
            self.conn.execute('''INSERT INTO authority_prototype.records
                SELECT DISTINCT ON(v->>'record_id') %s,v->>'record_id',(v->>'ordinal')::bigint
                FROM authority_batch ORDER BY v->>'record_id',(v->>'ordinal')::bigint DESC
                ON CONFLICT(authority,record_id) DO UPDATE SET ordinal=excluded.ordinal
                WHERE authority_prototype.records.ordinal<excluded.ordinal''', (authority,))
            self.conn.execute('''INSERT INTO authority_prototype.identities
                SELECT DISTINCT %s::uuid,(b.v->>'ordinal')::bigint,i->>0,i->>1,i->>2
                FROM authority_batch b,LATERAL jsonb_array_elements(b.v->'identities') i
                ON CONFLICT DO NOTHING''', (authority,))

    def _iter(self, authority: str, key: str | None = None):
        # Server cursor prevents libpq from buffering the whole result. Each
        # SQL suppresses oversized encoded rows before sending their values.
        # A page carries <=READ_PAGE*MAX_FETCH_ROW_BYTES encoded field bytes
        # plus bounded protocol metadata. Expanded emissions are checked again
        # before construction. These are encoded bounds, not a Python RSS cap.
        with self.conn.cursor(name='authority_' + uuid.uuid4().hex) as cur:
            cur.itersize = READ_PAGE
            # A fresh authority can be absent from column statistics. Keep
            # the unique ordinal lookup parameterized even with a tiny row
            # estimate; flattening can otherwise materialize all operations
            # and compare every record against them. OFFSET 0 drops no rows.
            query = '''SELECT CASE WHEN octet_length(encoded)<=%s THEN encoded ELSE NULL END, octet_length(encoded) FROM (SELECT jsonb_build_array(r.record_id,o.payload,o.revisions,o.ordinal,o.sha256,o.identities,
              (SELECT COALESCE(jsonb_object_agg(v.sha256,v.payload),'{}'::jsonb)
               FROM authority_prototype.revisions v
               WHERE v.sha256 IN (SELECT jsonb_array_elements_text(o.revisions))))::text AS encoded
              FROM authority_prototype.records r CROSS JOIN LATERAL (
                SELECT * FROM authority_prototype.operations candidate
                WHERE candidate.authority=r.authority AND candidate.ordinal=r.ordinal
                OFFSET 0
              ) o
              WHERE r.authority=%s AND o.payload IS NOT NULL
              {key_filter}
              ORDER BY r.record_id COLLATE "C") encoded_rows'''
            query = query.replace('{key_filter}', 'AND r.record_id=%s' if key is not None else '')
            cur.execute(query, (MAX_FETCH_ROW_BYTES, authority, key) if key is not None else (MAX_FETCH_ROW_BYTES, authority))
            for encoded, encoded_bytes in cur:
                if encoded is None or encoded_bytes > MAX_FETCH_ROW_BYTES:
                    raise AuthorityError('prototype encoded fetch row exceeds byte bound')
                record_id, payload, revision_keys, ordinal, op_sha, stored_identities, revisions = json.loads(encoded)
                identities = []
                nodes = ([payload['primary']] if payload['primary'] else []) + payload['descendants']
                for node in nodes:
                    identities.append(['node', node['node_id'], node.get('ownership_mode', 'authoritative')])
                for edge in payload['edges']:
                    identities.append(['edge', canonical([edge['src'], edge['edge_type'], edge['dst']]), ''])
                original = dict(ordinal=ordinal, record_id=record_id, payload=payload,
                                revisions=revisions, identities=identities)
                if stored_identities != identities or digest(canonical(original)) != op_sha:
                    raise AuthorityError('corrupt authority operation')
                if set(revision_keys) != set(revisions):
                    raise AuthorityError('missing shared revision')
                for sha, value in revisions.items():
                    if digest(value) != sha:
                        raise AuthorityError('corrupt shared revision')
                nodes = ([payload['primary']] if payload['primary'] else []) + payload['descendants']
                decoded_revisions = {sha: json.loads(value) for sha, value in revisions.items()}
                for item in nodes + payload['edges']:
                    sha = item.pop('revision_ref')
                    if sha not in revisions:
                        raise AuthorityError('missing referenced revision')
                    item['source_revision'] = decoded_revisions[sha]
                logical_size(payload['primary'], payload['descendants'], payload['edges'])
                yield record_id, RecordEmission.from_cursor(payload)

    def _logical_digest(self, authority: str):
        h, count = hashlib.sha256(), 0
        h.update(b'{"records":{')
        for key, emission in self._iter(authority):
            if count:
                h.update(b',')
            h.update(canonical(key).encode())
            h.update(b':')
            h.update(canonical(emission.to_cursor()).encode())
            count += 1
        h.update(b'}}')
        return h.hexdigest(), count

    def seal(self, authority: str, *, expected_digest: str, expected_count: int,
             expected_operations: int) -> Reference:
        with self.conn.transaction():
            source, incarnation, sealed, sha, count = self._lock(authority)
            if sealed:
                operation_count = self.conn.execute('SELECT operation_count FROM authority_prototype.authorities WHERE id=%s', (authority,)).fetchone()[0]
                if operation_count != expected_operations:
                    raise AuthorityError('conflicting sealed operation count')
                if (sha, count) != (expected_digest, expected_count):
                    raise AuthorityError('conflicting sealed authority')
                self._validate(Reference(authority, source, incarnation, sha, count))
                return Reference(authority, source, incarnation, sha, count)
            stats = self.conn.execute('SELECT count(*),min(ordinal),max(ordinal) '
                                      'FROM authority_prototype.operations WHERE authority=%s', (authority,)).fetchone()
            expected = (expected_operations, 0, expected_operations - 1) if expected_operations else (0, None, None)
            if stats != expected:
                raise AuthorityError('incomplete operation sequence')
            if self.conn.execute('''SELECT 1 FROM authority_prototype.identities i
              JOIN authority_prototype.records r ON r.authority=i.authority AND r.ordinal=i.ordinal
              WHERE i.authority=%s AND i.kind='node' GROUP BY i.identity
              HAVING min(i.ownership)<>max(i.ownership) LIMIT 1''', (authority,)).fetchone():
                raise AuthorityError('conflicting node ownership')
            sha, count = self._logical_digest(authority)
            self._validate_indexes(authority)
            if (sha, count) != (expected_digest, expected_count):
                raise AuthorityError('logical digest or count mismatch')
            self.conn.execute('UPDATE authority_prototype.authorities SET sealed=true,sha256=%s,count=%s,operation_count=%s WHERE id=%s',
                              (sha, count, expected_operations, authority))
            self.conn.execute('INSERT INTO authority_prototype.refs VALUES (%s,%s) '
                              'ON CONFLICT(source) DO UPDATE SET authority=excluded.authority', (source, authority))
            return Reference(authority, source, incarnation, sha, count)

    def _validate_indexes(self, authority: str):
        mismatch = self.conn.execute('''WITH expected AS (
          SELECT DISTINCT o.ordinal,i->>0 AS kind,i->>1 AS identity,i->>2 AS ownership
          FROM authority_prototype.records r JOIN authority_prototype.operations o
          ON o.authority=r.authority AND o.ordinal=r.ordinal,
          LATERAL jsonb_array_elements(o.identities) i
          WHERE r.authority=%s AND o.payload IS NOT NULL),
        actual AS (SELECT i.ordinal,i.kind,i.identity,i.ownership
          FROM authority_prototype.identities i JOIN authority_prototype.records r
          ON r.authority=i.authority AND r.ordinal=i.ordinal WHERE i.authority=%s)
        SELECT 1 FROM ((SELECT * FROM expected EXCEPT SELECT * FROM actual)
          UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected)) mismatch LIMIT 1''',
          (authority, authority)).fetchone()
        if mismatch:
            raise AuthorityError('corrupt authority identity index')

    def _validate(self, ref: Reference):
        row = self._lock(ref.authority, for_update=False)
        if ref.format != FORMAT or row != (ref.source, ref.incarnation, True, ref.sha256, ref.count):
            raise AuthorityError('unsealed or mismatched reference')
        if self._logical_digest(ref.authority) != (ref.sha256, ref.count):
            raise AuthorityError('corrupt authority')
        self._validate_indexes(ref.authority)

    @contextmanager
    def reader(self, ref: Reference):
        idle = self.conn.info.transaction_status == TransactionStatus.IDLE
        with self.conn.transaction():
            if idle:
                self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            elif self.conn.execute('SHOW transaction_isolation').fetchone()[0] not in ('repeatable read', 'serializable'):
                raise AuthorityError('verified reader requires a stable transaction snapshot')
            self._validate(ref)
            reader = VerifiedReader(self, ref)
            try:
                yield reader
            finally:
                reader.close()

    def iterate(self, ref: Reference) -> Iterator[tuple[str, RecordEmission]]:
        with self.reader(ref) as reader:
            yield from reader.iterate()

    def get(self, ref: Reference, key: str) -> RecordEmission | None:
        with self.reader(ref) as reader:
            return reader.get(key)

    def merge(self, authority: str, ref: Reference, *, start_ordinal: int = 0) -> int:
        # Bounded cross-authority copy; receiver's later ordinals win.
        count = 0
        def operations():
            nonlocal count
            for key, value in self.iterate(ref):
                yield start_ordinal + count, key, value
                count += 1
        self.stage(authority, operations())
        return count

    def missing_identities(self, ref: Reference, expected: Iterable[tuple[str, str]]):
        with self.reader(ref) as reader:
            yield from reader.missing_identities(expected)

    def _missing_identities(self, ref: Reference, expected: Iterable[tuple[str, str]]):
        """Only called inside an authenticated reader snapshot."""
        with self.conn.transaction():
            self.conn.execute('CREATE TEMP TABLE IF NOT EXISTS authority_expected(kind text,identity text,PRIMARY KEY(kind,identity)) ON COMMIT DROP')
            self.conn.execute('TRUNCATE authority_expected')
            batch, size = [], 2
            def send():
                self.conn.execute('INSERT INTO authority_expected SELECT value->>0,value->>1 '
                                  'FROM jsonb_array_elements(%s::jsonb) ON CONFLICT DO NOTHING',
                                  ('[' + ','.join(batch) + ']',))
            for kind, identity in expected:
                if kind not in ('node', 'edge') or len(identity.encode()) > 2048:
                    raise AuthorityError('invalid bounded expected identity')
                item = canonical([kind, identity])
                added = len(item.encode()) + bool(batch)
                if size + added > LIMIT:
                    send()
                    batch, size = [], 2
                    added = len(item.encode())
                batch.append(item)
                size += added
            if batch:
                send()
            with self.conn.cursor(name='missing_' + uuid.uuid4().hex) as cur:
                cur.itersize = READ_PAGE
                cur.execute('''SELECT e.kind,e.identity FROM authority_expected e
                  WHERE NOT EXISTS (SELECT 1 FROM authority_prototype.identities i
                  JOIN authority_prototype.records r ON r.authority=i.authority AND r.ordinal=i.ordinal
                  JOIN authority_prototype.operations o ON o.authority=r.authority AND o.ordinal=r.ordinal
                  WHERE i.authority=%s AND i.kind=e.kind AND i.identity=e.identity AND o.payload IS NOT NULL)
                  ORDER BY e.kind,e.identity''', (ref.authority,))
                yield from cur


class VerifiedReader:
    """Amortize full integrity verification across indexed reads in one snapshot."""
    def __init__(self, store: AuthorityStore, ref: Reference):
        self.store, self.ref, self.active = store, ref, True
        self._open = []

    def _check(self):
        if not self.active:
            raise AuthorityError('reader snapshot has ended')

    def close(self):
        self.active = False
        try:
            for iterator in reversed(self._open[:]):
                iterator.close()
        finally:
            self._open.clear()

    def _guarded(self, iterable):
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
            iterator.close()
            if iterator in self._open:
                self._open.remove(iterator)

    def iterate(self):
        return self._guarded(self.store._iter(self.ref.authority))

    def get(self, key: str):
        self._check()
        if not isinstance(key, str) or not key or len(key.encode()) > 2048:
            raise AuthorityError('invalid bounded record identity')
        iterator = self._guarded(self.store._iter(self.ref.authority, key))
        try:
            return next((v for _, v in iterator), None)
        finally:
            iterator.close()

    def missing_identities(self, expected):
        return self._guarded(self.store._missing_identities(self.ref, expected))
