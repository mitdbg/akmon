"""Isolated PostgreSQL prototype; no normal schema, SourceRunner or graph writes."""
import hashlib
import json
import os
import resource
import time
import uuid
from dataclasses import replace

import psycopg
import pytest

from okg.substrate.ingest.record_authority import (
    AuthorityError, LIMIT, READ_PAGE, MAX_FETCH_ROW_BYTES, canonical, digest,
)
from tests.substrate.record_authority_prototype_store import (
    AuthorityStore, install_prototype,
)
from okg.substrate.library.sources.base import EdgeKey, RecordEmission, RecordNode, RecordSet


# Every row opens a real connection and installs the prototype schema, so the
# `not postgres` fast lane must not select this file.
pytestmark = pytest.mark.postgres


@pytest.fixture
def store(okg_dsn: str):
    # The prototype owns its own ``authority_prototype`` schema and never
    # touches the ``okg`` schema, so it runs against the ordinary private
    # substrate test database rather than a hand-created one. A hand-created
    # database name would make this suite skip silently in CI.
    dsn = os.environ.get('OKG_AUTHORITY_PROTOTYPE_TEST_DSN') or okg_dsn
    with psycopg.connect(dsn, autocommit=True) as conn:
        install_prototype(conn)
        counters = dict(fetch_calls=0, execute_calls=0, max_fetch_rows=0,
                        logical_page_bytes=0, encoded_page_bytes=0, validation_passes=0)
        first_plan = []
        class ObservedCursor(psycopg.ServerCursor):
            def execute(self, query, params=None, **kwargs):
                if not first_plan and 'encoded_rows' in str(query) and len(params or ()) == 2:
                    # Plan only: never execute or prewarm the first data query.
                    first_plan.extend(conn.execute('EXPLAIN (FORMAT JSON) DECLARE authority_test CURSOR FOR ' + query, params).fetchone()[0])
                return super().execute(query, params, **kwargs)
            def _fetch_gen(self, num):
                counters['fetch_calls'] += 1
                rows = yield from super()._fetch_gen(num)
                counters['max_fetch_rows'] = max(counters['max_fetch_rows'], len(rows))
                if rows and isinstance(rows[0][1], int):
                    actual_bytes = sum(len(row[0].encode()) if row[0] is not None else 0 for row in rows)
                    counters['encoded_page_bytes'] = max(counters['encoded_page_bytes'], actual_bytes)
                    assert actual_bytes <= READ_PAGE * MAX_FETCH_ROW_BYTES
                return rows
        class ObservedConnection:
            max_text_parameter_bytes = 0
            max_text_send_bytes = 0
            def execute(self, query, params=None):
                counters['execute_calls'] += 1
                sizes = [len(value.encode('utf-8')) for value in (params or ()) if isinstance(value, str)]
                self.max_text_parameter_bytes = max([self.max_text_parameter_bytes, *sizes])
                self.max_text_send_bytes = max(self.max_text_send_bytes, sum(sizes))
                return conn.execute(query, params)
            def cursor(self, *args, **kwargs):
                if 'name' in kwargs:
                    return ObservedCursor(conn, kwargs['name'])
                return conn.cursor(*args, **kwargs)
            def __getattr__(self, name):
                return getattr(conn, name)
        class ObservedStore(AuthorityStore):
            def _validate(self, ref):
                counters['validation_passes'] += 1
                return super()._validate(ref)
            def _iter(self, *args, **kwargs):
                page_size, rows = 0, 0
                for key, value in super()._iter(*args, **kwargs):
                    page_size += len(canonical(value.to_cursor()).encode())
                    rows += 1
                    counters['logical_page_bytes'] = max(counters['logical_page_bytes'], page_size)
                    if rows == READ_PAGE:
                        rows, page_size = 0, 0
                    yield key, value
        store = ObservedStore(ObservedConnection())
        store.counters = counters
        store.dsn = dsn
        store.first_plan = first_plan
        yield store


def begin(store):
    return store.begin('source-' + uuid.uuid4().hex, 'incarnation-1', 'attempt-1')


def node(key, revision=None, ownership='authoritative'):
    return RecordEmission(primary=RecordNode(key, 'dataset', {'id': key}, revision or {}, ownership))


def seal(store, authority, records, operations=None):
    return store.seal(authority, expected_digest=digest(canonical(RecordSet(records).to_cursor())),
                      expected_count=len(records), expected_operations=len(records) if operations is None else operations)


def interrupt(message: str) -> None:
    """Raise from a call so the raise is not the last statement of a with body.

    Both call sites raise inside ``with pytest.raises(...):`` to prove a
    context manager rolls back. CodeQL does not model ``pytest.raises`` as
    swallowing the exception, so a bare ``raise`` as the final statement of the
    nested ``with`` body makes every statement after the outer block read as
    unreachable (py/unreachable-statement). Raising through a call keeps the
    behaviour identical and keeps the assertions that follow analysable.
    """

    raise RuntimeError(message)


def test_roundtrip_unicode_edges_duplicates_retractions_and_merge(store):
    a = begin(store)
    edge = RecordEmission(primary=RecordNode('é', 'dataset'),
                          descendants=[RecordNode('child', 'dataset', ownership_mode='substitute')],
                          edges=[EdgeKey('é', 'child', 'contains', {'unicode': '🌍'})])
    records = {'ä': edge, 'z': node('new')}
    ops = [(0, 'z', node('old')), (1, 'delete', node('gone')),
           (2, 'ä', edge), (3, 'z', node('new')), (4, 'delete', None)]
    store.stage(a, ops)
    store.stage(a, ops)  # immutable attempt replay
    ref = seal(store, a, records, len(ops))
    assert dict(store.iterate(ref)) == records
    assert store.get(ref, 'ä') == edge
    assert store.get(ref, 'delete') is None
    assert list(store.missing_identities(ref, [('node', 'é'), ('node', 'gone'),
                ('edge', canonical(['é', 'contains', 'child']))])) == [('node', 'gone')]
    b = begin(store)
    assert store.merge(b, ref) == 2
    store.stage(b, [(2, 'z', None)])
    merged = seal(store, b, {'ä': edge}, 3)
    assert dict(store.iterate(merged)) == {'ä': edge}


def test_empty_and_legacy_logical_digest(store):
    a = begin(store)
    ref = seal(store, a, {})
    assert list(store.iterate(ref)) == []
    assert ref.sha256 == hashlib.sha256(b'{"records":{}}').hexdigest()


def test_incomplete_conflicting_retry_and_sealed_mutation(store):
    a = begin(store)
    store.stage(a, [(1, 'x', node('x'))])
    with pytest.raises(AuthorityError, match='incomplete'):
        seal(store, a, {'x': node('x')}, 2)
    store.stage(a, [(0, 'y', node('y'))])
    with pytest.raises(AuthorityError, match='conflicting attempt'):
        store.stage(a, [(1, 'x', node('different'))])
    ref = seal(store, a, {'x': node('x'), 'y': node('y')})
    assert seal(store, a, {'x': node('x'), 'y': node('y')}) == ref
    with pytest.raises(AuthorityError, match='immutable'):
        store.stage(a, [(2, 'z', node('z'))])


def test_ownership_conflict_and_digest_mismatch_do_not_seal(store):
    a = begin(store)
    records = {'x': node('same'), 'y': node('same', ownership='substitute')}
    store.stage(a, [(i, k, v) for i, (k, v) in enumerate(records.items())])
    with pytest.raises(AuthorityError, match='ownership'):
        seal(store, a, records)
    b = begin(store)
    store.stage(b, [(0, 'x', node('x'))])
    with pytest.raises(AuthorityError, match='digest or count'):
        seal(store, b, {'wrong': node('x')})
    assert store.conn.execute('SELECT sealed FROM authority_prototype.authorities WHERE id=%s', (b,)).fetchone() == (False,)
    assert store.conn.execute('SELECT 1 FROM authority_prototype.refs WHERE authority=%s', (b,)).fetchone() is None


def test_seal_reference_publication_rolls_back_atomically(store):
    a = begin(store)
    store.stage(a, [(0, 'x', node('x'))])
    with pytest.raises(RuntimeError, match='interrupt'):
        with store.conn.transaction():
            seal(store, a, {'x': node('x')})
            with psycopg.connect(store.dsn, autocommit=True) as observer:
                assert observer.execute('SELECT sealed FROM authority_prototype.authorities WHERE id=%s', (a,)).fetchone() == (False,)
                assert observer.execute('SELECT 1 FROM authority_prototype.refs WHERE authority=%s', (a,)).fetchone() is None
            interrupt('interrupt before outer commit')
    assert store.conn.execute('SELECT sealed FROM authority_prototype.authorities WHERE id=%s', (a,)).fetchone() == (False,)
    assert store.conn.execute('SELECT 1 FROM authority_prototype.refs WHERE authority=%s', (a,)).fetchone() is None
    seal(store, a, {'x': node('x')})


@pytest.mark.parametrize('damage', ['missing_revision', 'corrupt_revision', 'corrupt_operation', 'missing_record', 'stale'])
def test_invalid_authority_fails_closed(store, damage):
    a = begin(store)
    revision = {'unique': uuid.uuid4().hex}
    store.stage(a, [(0, 'x', node('x', revision))])
    ref = seal(store, a, {'x': node('x', revision)})
    with store.conn.transaction(force_rollback=True):
        store.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        if damage == 'missing_revision':
            store.conn.execute('DELETE FROM authority_prototype.revisions WHERE sha256=%s', (digest(canonical(revision)),))
        elif damage == 'corrupt_revision':
            store.conn.execute("UPDATE authority_prototype.revisions SET payload='{}' WHERE sha256=%s", (digest(canonical(revision)),))
        elif damage == 'corrupt_operation':
            store.conn.execute("UPDATE authority_prototype.operations SET payload=jsonb_set(payload,'{primary,node_id}','\"tampered\"') WHERE authority=%s", (a,))
        elif damage == 'missing_record':
            store.conn.execute('DELETE FROM authority_prototype.records WHERE authority=%s', (a,))
        else:
            store.conn.execute("UPDATE authority_prototype.sources SET incarnation='new' WHERE source=%s", (ref.source,))
        with pytest.raises(AuthorityError):
            list(store.iterate(ref))
    assert store.get(ref, 'x') == node('x', revision)
    with pytest.raises(AuthorityError):
        list(store.iterate(replace(ref, sha256='bad')))


def test_indexed_get_does_not_scan_digest(store, monkeypatch):
    a = begin(store)
    store.stage(a, [(0, 'x', node('x'))])
    ref = seal(store, a, {'x': node('x')})
    with store.reader(ref) as reader:
        monkeypatch.setattr(store, '_logical_digest', lambda *args: pytest.fail('repeated authority scan inside verified reader'))
        assert reader.get('x') == node('x')
        assert reader.get('missing') is None
    with pytest.raises(AuthorityError, match='ended'):
        reader.get('x')


def test_bound_refuses_oversized_single_record_without_partial_write(store):
    a = begin(store)
    with pytest.raises(AuthorityError, match='record exceeds'):
        store.stage(a, [(0, 'x', node('x', {'huge': 'x' * LIMIT}))])
    assert store.conn.execute('SELECT count(*) FROM authority_prototype.operations WHERE authority=%s', (a,)).fetchone()[0] == 0


def test_measured_ten_thousand_shared_revision_records(store, record_property):
    revision = {'r': 'x' * 832}
    assert len(canonical(revision).encode()) == 840
    a = begin(store)
    start = time.monotonic()
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    store.stage(a, ((i, f'record:{i:05d}', node(f'node:{i:05d}', revision)) for i in range(10000)))
    staged_s = time.monotonic() - start
    before_seal = dict(store.counters)
    # Independent small-format oracle intentionally materializes only this
    # bounded fixture, outside the measured store's memory path.
    records = {f'record:{i:05d}': node(f'node:{i:05d}', revision) for i in range(10000)}
    expected = digest(canonical(RecordSet(records).to_cursor()))
    del records
    seal_start = time.monotonic()
    ref = store.seal(a, expected_digest=expected, expected_count=10000, expected_operations=10000)
    seal_s = time.monotonic() - seal_start
    def nodes(plan):
        yield plan
        for child in plan.get('Plans', []):
            yield from nodes(child)
    lookups = [n for n in nodes(store.first_plan[0]['Plan']) if n.get('Relation Name') == 'operations']
    assert lookups and all('authority' in n.get('Index Cond', '') and 'ordinal' in n.get('Index Cond', '') for n in lookups)
    record_property('first_cursor_plan_before_execution', json.dumps(store.first_plan))
    seal_fetch_calls = store.counters['fetch_calls'] - before_seal['fetch_calls']
    seal_connection_execute_calls = store.counters['execute_calls'] - before_seal['execute_calls']
    assert seal_fetch_calls <= 10000 // READ_PAGE + 1
    with store.reader(ref) as reader:
        before_queries = store.counters['execute_calls']
        assert list(reader.missing_identities(('node', f'node:{i:05d}') for i in range(10000))) == []
        expected_stage_queries = store.counters['execute_calls'] - before_queries
        assert expected_stage_queries < 10
        for i in range(10):
            assert reader.get(f'record:{i:05d}') is not None
    assert sum(1 for _ in store.iterate(ref)) == 10000
    assert store.conn.execute('SELECT count(*) FROM authority_prototype.revisions WHERE sha256=%s', (digest(canonical(revision)),)).fetchone()[0] == 1
    assert store.max_batch_bytes <= LIMIT
    assert store.max_batch_bytes > LIMIT * .9
    metrics = dict(records=10000, shared_revision_bytes=840, max_parameter_bytes=store.max_parameter_bytes,
                   max_batch_bytes=store.max_batch_bytes, batches=store.batch_count,
                   actual_max_text_parameter_bytes=store.conn.max_text_parameter_bytes,
                   actual_max_text_send_bytes=store.conn.max_text_send_bytes,
                   seal_fetch_calls=seal_fetch_calls, seal_connection_execute_calls=seal_connection_execute_calls,
                   expected_identity_stage_queries=expected_stage_queries,
                   fetch_calls=store.counters['fetch_calls'],
                   max_fetch_rows=store.counters['max_fetch_rows'],
                   max_reconstructed_page_bytes=store.counters['logical_page_bytes'],
                   max_encoded_fetch_page_bytes=store.counters['encoded_page_bytes'],
                   encoded_fetch_page_ceiling_bytes=READ_PAGE*MAX_FETCH_ROW_BYTES,
                   validation_passes=store.counters['validation_passes'],
                   stage_s=staged_s, seal_s=seal_s, total_s=time.monotonic()-start,
                   rss_before_kib=rss_before, max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                   database_bytes=store.conn.execute('SELECT pg_database_size(current_database())').fetchone()[0],
                   rss_scope='whole pytest process including bounded legacy oracle; not isolated store allocation')
    for key, value in metrics.items():
        record_property(key, value)
    print('AUTHORITY_PROTOTYPE_METRICS=' + json.dumps(metrics, sort_keys=True))


def test_shared_revision_fanout_bound_is_checked_before_staging(store):
    emission = RecordEmission(descendants=[RecordNode(str(i), 'dataset', source_revision={'r': 'x' * 20000}) for i in range(100)])
    a = begin(store)
    with pytest.raises(AuthorityError, match='logical record exceeds'):
        store.stage(a, [(0, 'fanout', emission)])
    assert store.conn.execute('SELECT count(*) FROM authority_prototype.operations WHERE authority=%s', (a,)).fetchone()[0] == 0


@pytest.mark.parametrize('damage', ['identity_delete', 'identity_extra', 'identity_wrong_owner', 'pointer_delete'])
def test_verified_negative_lookup_rejects_damaged_indexes(store, damage):
    a = begin(store)
    store.stage(a, [(0, 'x', node('node:x'))])
    ref = seal(store, a, {'x': node('node:x')})
    with store.conn.transaction(force_rollback=True):
        store.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        if damage == 'identity_delete':
            store.conn.execute('DELETE FROM authority_prototype.identities WHERE authority=%s', (a,))
        elif damage == 'identity_extra':
            store.conn.execute("INSERT INTO authority_prototype.identities VALUES (%s,0,'node','invented','authoritative')", (a,))
        elif damage == 'identity_wrong_owner':
            store.conn.execute("UPDATE authority_prototype.identities SET ownership='substitute' WHERE authority=%s", (a,))
        else:
            store.conn.execute('DELETE FROM authority_prototype.records WHERE authority=%s', (a,))
        with pytest.raises(AuthorityError):
            store.get(ref, 'x')
        with pytest.raises(AuthorityError):
            list(store.missing_identities(ref, [('node', 'node:x')]))


def test_sealed_retry_checks_operation_count(store):
    a = begin(store)
    store.stage(a, [(0, 'x', node('x'))])
    ref = seal(store, a, {'x': node('x')})
    with pytest.raises(AuthorityError, match='operation count'):
        store.seal(a, expected_digest=ref.sha256, expected_count=1, expected_operations=2)


def test_verified_reader_uses_consistent_snapshot(store):
    a = begin(store)
    store.stage(a, [(0, 'x', node('x'))])
    ref = seal(store, a, {'x': node('x')})
    with store.reader(ref) as reader:
        with psycopg.connect(store.dsn, autocommit=True) as writer:
            writer.execute('DELETE FROM authority_prototype.records WHERE authority=%s', (a,))
        assert reader.get('x') == node('x')  # committed damage is outside this snapshot
    with pytest.raises(AuthorityError):
        store.get(ref, 'x')  # a new reader authenticates the new state and refuses


def test_near_limit_shared_fanout_roundtrip_and_read_time_refusal(store):
    from tests.substrate.record_authority_prototype_store import logical_size
    rev = {'r': 'x' * 20000}
    emission = RecordEmission(descendants=[RecordNode(str(i), 'dataset', source_revision=rev) for i in range(48)])
    size = logical_size(None, (n.to_cursor() for n in emission.descendants), [])
    assert LIMIT * .9 < size < LIMIT
    a = begin(store)
    store.stage(a, [(0, 'x', emission)])
    ref = seal(store, a, {'x': emission})
    assert store.get(ref, 'x') == emission
    # Simulate an old/corrupt format row that bypassed the new writer bound.
    # Only the small normalized representation is serialized; no expanded JSON.
    with store.conn.transaction(force_rollback=True):
        store.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        row = store.conn.execute('SELECT payload FROM authority_prototype.operations WHERE authority=%s', (a,)).fetchone()[0]
        row['descendants'] = row['descendants'] * 3
        identities = [['node', n['node_id'], 'authoritative'] for n in row['descendants']]
        op = dict(ordinal=0, record_id='x', payload=row, revisions={digest(canonical(rev)): canonical(rev)}, identities=identities)
        store.conn.execute('UPDATE authority_prototype.operations SET payload=%s::jsonb,identities=%s::jsonb,sha256=%s WHERE authority=%s',
                           (canonical(row), canonical(identities), digest(canonical(op)), a))
        with pytest.raises(AuthorityError, match='logical record exceeds'):
            store.get(ref, 'x')


def test_oversized_encoded_fields_are_suppressed_before_fetch(store):
    revision = {'unique': uuid.uuid4().hex}
    a = begin(store)
    store.stage(a, [(0, 'x', node('x', revision))])
    ref = seal(store, a, {'x': node('x', revision)})
    with store.conn.transaction(force_rollback=True):
        store.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        store.conn.execute('UPDATE authority_prototype.revisions SET payload=%s WHERE sha256=%s',
                           ('x' * (MAX_FETCH_ROW_BYTES + 1), digest(canonical(revision))))
        with pytest.raises(AuthorityError, match='encoded fetch row exceeds'):
            store.get(ref, 'x')


def test_reader_early_close_exception_and_transaction_contract(store):
    from psycopg.pq import TransactionStatus
    a = begin(store)
    store.stage(a, [(0, 'x', node('x'))])
    ref = seal(store, a, {'x': node('x')})
    iterator = store.iterate(ref)
    assert next(iterator)[0] == 'x'
    iterator.close()
    assert store.conn.info.transaction_status == TransactionStatus.IDLE
    with pytest.raises(RuntimeError):
        with store.reader(ref):
            interrupt('reader body failed')
    assert store.conn.info.transaction_status == TransactionStatus.IDLE
    with store.conn.transaction():
        with pytest.raises(AuthorityError, match='stable transaction'):
            store.get(ref, 'x')
    with store.reader(ref):
        assert store.conn.execute("SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND mode='RowShareLock'").fetchone()[0] == 0


@pytest.mark.parametrize('kind', ['iterate', 'missing'])
def test_escaped_reader_generator_cannot_yield_prefetched_rows(store, kind):
    from psycopg.pq import TransactionStatus
    a = begin(store)
    records = {str(i): node(str(i)) for i in range(20)}
    store.stage(a, ((i, key, value) for i, (key, value) in enumerate(records.items())))
    ref = seal(store, a, records)
    with store.reader(ref) as reader:
        iterator = reader.iterate() if kind == 'iterate' else reader.missing_identities(('node', 'absent'+str(i)) for i in range(20))
        next(iterator)  # a16-row page now has buffered unconsumed values
    assert store.conn.info.transaction_status == TransactionStatus.IDLE
    with pytest.raises(AuthorityError, match='snapshot has ended'):
        next(iterator)
    assert store.conn.execute('SELECT count(*) FROM pg_cursors').fetchone()[0] == 0


def test_oversized_lookup_key_is_refused_before_sql(store, monkeypatch):
    a = begin(store)
    store.stage(a, [(0, 'x', node('x'))])
    ref = seal(store, a, {'x': node('x')})
    with store.reader(ref) as reader:
        monkeypatch.setattr(store, '_iter', lambda *args, **kwargs: pytest.fail('lookup key reached SQL reader'))
        with pytest.raises(AuthorityError, match='bounded record identity'):
            reader.get('é' * 1025)
