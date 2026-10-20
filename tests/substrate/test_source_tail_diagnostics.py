"""Observable, payload-free source-tail diagnostics; no database required."""
import hashlib
import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import okg.substrate.ingest.record_set_pieces as record_set_pieces
import okg.substrate.ingest.runner as runner_module
import okg.substrate.ingest.service as service
from okg.substrate.library.sources.base import RecordSet

SECRET = 'credential-and-source-payload-sentinel'


class FlushedSink(io.StringIO):
    def flush(self):
        self.flushed = self.getvalue()


SourceRunner = runner_module.SourceRunner


def bare_runner():
    runner = object.__new__(SourceRunner)
    runner.source_name = SECRET
    return runner


@pytest.mark.parametrize('sqlstate', [None, '53200'])
def test_failure_is_flushed_while_compensation_is_blocked(monkeypatch, tmp_path, sqlstate, capsys):
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    entered = threading.Event()
    release = threading.Event()
    class Failure(RuntimeError):
        pass
    error = Failure(SECRET)
    error.sqlstate = sqlstate
    class FailingRunner:
        _tail_phase = SourceRunner._tail_phase

        def __init__(self, *args, **kwargs):
            self.source_name = SECRET

        def run(self, *args, **kwargs):
            with self._tail_phase('record_set.execute', payload_bytes=123, parameter_count=6):
                raise error

        def release_deferred_bootstrap_lock(self):
            pass
    def compensate(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        raise RuntimeError('compensation sentinel')
    context = SimpleNamespace(admission_id='unit-admission', run_id='unit-run')
    monkeypatch.setattr(service, 'SourceRunner', FailingRunner)
    monkeypatch.setattr(service, '_prepare_source_admission', lambda **kw: (context, {}, 'pending'))
    monkeypatch.setattr(service, '_run_mode_for_source', lambda *args: 'cursor')
    monkeypatch.setattr(service, 'import_source_adapter_class', lambda entry: SimpleNamespace)
    monkeypatch.setattr(service, 'source_adapter_graph_branch', lambda entry: 'default')
    monkeypatch.setattr(service, 'source_adapter_init_params', lambda *args, **kw: {})
    monkeypatch.setattr(service, 'bind_source_adapter_identity', lambda *args: None)
    monkeypatch.setattr(service, '_snapshot_source_state', lambda *args, **kw: None)
    monkeypatch.setattr(service, 'resolve_deployment_source_pii_posture', lambda **kw: None)
    monkeypatch.setattr(service, 'runtime_enforce_mode_dsn_keys', lambda *args: set())
    monkeypatch.setattr(service, '_retract_chronos_run', compensate)
    (tmp_path / 'deployment.yaml').write_text('{}')
    options = service.IngestOptions(dsn='postgres://unused', deployment='unit', deployment_dir=tmp_path)
    entry = SimpleNamespace(raw={}, ownership_id='unit.source', admission_contract=None)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(service._run_source, options, entry, 'unit')
        try:
            assert entered.wait(5)
            assert not future.done()
            events = [json.loads(line) for line in sink.flushed.splitlines()]
            assert events[-1]['status'] == 'failed'
            assert events[-1]['sqlstate'] == sqlstate
            assert SECRET not in sink.flushed
        finally:
            release.set()
        result = future.result(5)
    assert result['source_tail_failure'] == events[-1]
    assert result['source_tail_failure']['exception_class'] == 'Failure'
    assert result['status'] == 'FAIL'
    # Diagnostics leave the normal single JSON stdout result separate.
    assert capsys.readouterr().out == ''
    print(json.dumps(result))
    assert json.loads(capsys.readouterr().out)['source_tail_failure'] == events[-1]


def test_authority_serialization_order_and_no_extra_payload_serialization(monkeypatch):
    """The stored value is the one-shot canonical JSON, and serialize is observed per authority."""
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._inline_record_set_max_bytes = lambda: 0
    payload = {'records': [SECRET]}
    persisted = []
    runner._store_external_record_set = lambda value, **kw: persisted.append((value, kw))
    for verified in (False, True):
        runner._record_set_authority_cursor(payload, summary={'chronos_graph_verified': verified}, proof=None)
    serialized = json.dumps(payload, sort_keys=True, default=str, separators=(',', ':'))
    assert [value for value, _ in persisted] == [serialized] * 2
    assert [kw['payload_bytes'] for _, kw in persisted] == [len(serialized.encode())] * 2
    assert [kw['digest'] for _, kw in persisted] == [hashlib.sha256(serialized.encode()).hexdigest()] * 2
    events = [json.loads(line) for line in sink.getvalue().splitlines()]
    assert [(e['authority_ordinal'], e['verified']) for e in events if e['status'] == 'started' and e['phase'] == 'record_set.serialize'] == [(1, False), (2, True)]
    assert SECRET not in sink.getvalue()


def test_authority_payload_is_never_encoded_whole(monkeypatch):
    """Issue #3008: size and digest come from per-record chunks, one encode each.

    The whole text is never encoded; a 1.6 GB payload is not copied again.
    """
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._ensure_source_record_sets_table = lambda: None
    runner._inline_record_set_max_bytes = lambda: 0
    runner._record_set_jsonb_max_bytes = lambda: 1 << 20
    payload = {'records': {'a': [SECRET], 'b': {'k': SECRET}}}
    params = []
    cursor = SimpleNamespace(execute=lambda sql, values: params.append(values))
    runner._acquire_conn = lambda: SimpleNamespace(cursor=lambda: nullcontext(cursor))
    encoded_sizes = []
    serialized = json.dumps(payload, sort_keys=True, default=str, separators=(',', ':'))

    class Probe(str):
        def encode(self, *args, **kwargs):
            data = str.encode(self, *args, **kwargs)
            encoded_sizes.append(len(data))
            return data

    monkeypatch.setattr(
        runner_module, '_iter_record_set_json',
        lambda value: (Probe(chunk) for chunk in record_set_pieces.iter_record_set_json(value)),
    )
    out = runner._record_set_authority_cursor(payload, summary={}, proof=None)
    # Every byte is encoded exactly once, and never as one whole-payload copy.
    assert sum(encoded_sizes) == len(serialized.encode())
    assert len(encoded_sizes) > 1
    assert max(encoded_sizes) < len(serialized)
    digest = hashlib.sha256(serialized.encode('utf-8')).hexdigest()
    assert out[runner_module._RECORD_SET_REF_KEY]['sha256'] == digest
    assert params == [(SECRET, serialized, '{}', None, digest)]
    events = [json.loads(line) for line in sink.getvalue().splitlines()]
    assert events[-1]['phase'] == 'record_set.execute'
    assert events[-1]['payload_bytes'] == len(serialized.encode('utf-8'))
    assert events[-1]['payload_parameter_ordinal'] == 2


def test_external_record_set_payload_unchanged_and_shape_only(monkeypatch):
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._ensure_source_record_sets_table = lambda: None
    runner._record_set_jsonb_max_bytes = lambda: 1 << 20
    calls = []
    cursor = SimpleNamespace(execute=lambda sql, params: calls.append(params))
    runner._acquire_conn = lambda: SimpleNamespace(cursor=lambda: nullcontext(cursor))
    runner._store_external_record_set(SECRET, summary={}, proof=None, digest='digest')
    assert calls == [(SECRET, SECRET, '{}', None, 'digest')]
    events = [json.loads(line) for line in sink.getvalue().splitlines()]
    assert events[-1]['phase'] == 'record_set.execute'
    assert events[-1]['payload_bytes'] == len(SECRET.encode())
    assert events[-1]['payload_parameter_ordinal'] == 2
    assert SECRET not in sink.getvalue()


def test_oversized_payload_goes_to_pieces_not_one_parameter(monkeypatch):
    """Above the JSONB threshold no single parameter carries the payload."""
    runner = bare_runner()
    runner._record_set_jsonb_max_bytes = lambda: 8
    stored = []

    def pieced(chunks, *, digest_fn, summary, proof):
        stored.append((list(chunks), digest_fn()))
        return digest_fn()

    runner._store_pieced_record_set = pieced
    runner._ensure_source_record_sets_table = lambda: None
    runner._acquire_conn = lambda: pytest.fail('no single-row INSERT above the threshold')
    runner._store_external_record_set(SECRET, summary={}, proof=None, digest='digest')
    assert stored == [([(SECRET, len(SECRET))], 'digest')]


def test_piece_size_setting_is_refused_when_invalid(monkeypatch):
    for raw in ('abc', '0', '3', str(1 << 30)):
        monkeypatch.setenv('OKG_SOURCE_RUNNER_RECORD_SET_PIECE_MAX_BYTES', raw)
        with pytest.raises(runner_module.SubstrateError):
            SourceRunner._record_set_piece_max_bytes()
    monkeypatch.setenv('OKG_SOURCE_RUNNER_RECORD_SET_PIECE_MAX_BYTES', '4096')
    assert SourceRunner._record_set_piece_max_bytes() == 4096
    monkeypatch.delenv('OKG_SOURCE_RUNNER_RECORD_SET_PIECE_MAX_BYTES')
    assert SourceRunner._record_set_piece_max_bytes() == 64 * 1024 * 1024


def test_authority_refuses_a_non_record_set(monkeypatch):
    runner = bare_runner()
    with pytest.raises(TypeError):
        runner._record_set_authority_cursor(SimpleNamespace(), summary={}, proof=None)


def test_other_cursor_blob_failure_has_distinct_phase_and_no_payload(monkeypatch):
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._ensure_source_cursor_blobs_table = lambda: None
    def fail(sql, params):
        raise RuntimeError(SECRET)
    cursor = SimpleNamespace(execute=lambda *args: None, fetchall=lambda: [], executemany=fail)
    runner._acquire_conn = lambda: SimpleNamespace(cursor=lambda: nullcontext(cursor))
    with pytest.raises(RuntimeError):
        runner._store_external_cursor_fields([('private-key', SECRET.encode(), 'digest', len(SECRET))])
    event = runner.tail_failure_diagnostic
    assert event['phase'] == 'cursor_blob.insert'
    assert event['parameter_count_per_insert'] == 5
    assert event['row_count'] == 1
    assert SECRET not in sink.getvalue()
    assert 'private-key' not in sink.getvalue()


def _failing_record_set():
    def fail():
        raise MemoryError(SECRET)
    return RecordSet(records={'k': SimpleNamespace(to_cursor=fail)})


def test_serialization_failure_is_observed_before_any_sql(monkeypatch):
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._acquire_conn = lambda: pytest.fail('no SQL before serialization succeeds')
    with pytest.raises(MemoryError):
        runner._record_set_authority_cursor(_failing_record_set(), summary={}, proof=None)
    assert runner.tail_failure_diagnostic['phase'] == 'record_set.serialize'
    assert SECRET not in sink.getvalue()


@pytest.mark.parametrize('verified', [False, True])
def test_record_serialization_failure_has_current_invocation_and_verified_flag(monkeypatch, verified):
    runner = bare_runner()
    runner._tail_authority_ordinal = 7
    runner._tail_verified = not verified
    runner.graph_branch = 'default'
    runner._record_set_identities = lambda record_set: (set(), set())
    runner._record_set_node_ownership_modes = lambda record_set: {}
    runner._chronos_branch_incarnation = lambda: None
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    with pytest.raises(MemoryError):
        runner._cursor_with_record_set({}, _failing_record_set(), chronos_graph_verified=verified)
    event = runner.tail_failure_diagnostic
    assert event['phase'] == 'record_set.serialize'
    assert event['authority_ordinal'] == 8
    assert event['verified'] is verified
    assert SECRET not in sink.getvalue()


def test_proof_failure_identified_before_authority_serialization(monkeypatch):
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    def fail(record_set):
        raise MemoryError(SECRET)
    runner._record_set_identities = fail
    with pytest.raises(MemoryError):
        runner._cursor_with_record_set({}, SimpleNamespace(to_cursor=lambda: {}))
    assert runner.tail_failure_diagnostic['phase'] == 'record_set.proof'
    assert 'record_set.serialize' not in sink.getvalue()
    assert SECRET not in sink.getvalue()


@pytest.mark.parametrize('failure_type', [MemoryError, RuntimeError])
@pytest.mark.parametrize('status', ['started', 'completed', 'failed'])
@pytest.mark.parametrize('failure_site', ['write', 'flush', 'construction'])
def test_diagnostic_sink_failure_preserves_business_outcome(monkeypatch, failure_type, status, failure_site):
    runner = bare_runner()
    class BrokenSink(FlushedSink):
        latest_status = None
        def write(self, value):
            self.latest_status = json.loads(value)['status']
            if self.latest_status == status and failure_site == 'write':
                raise failure_type('observer unavailable')
            return super().write(value)
        def flush(self):
            if self.latest_status == status and failure_site == 'flush':
                raise failure_type('observer unavailable')
            super().flush()
    real_dumps = json.dumps
    def dumps(value, **kwargs):
        if failure_site == 'construction' and value.get('status') == status:
            raise failure_type('observer unavailable')
        return real_dumps(value, **kwargs)
    monkeypatch.setattr(runner_module.json, 'dumps', dumps)
    monkeypatch.setattr(runner_module.sys, 'stderr', BrokenSink())
    original = ValueError('original')
    entered = []
    if status == 'failed':
        caught = None
        try:
            with runner._tail_phase('record_set.execute'):
                entered.append(True)
                raise original
        except ValueError as exc:
            caught = exc
        assert caught is original
    else:
        with runner._tail_phase('record_set.execute'):
            entered.append(True)
    assert entered == [True]


@pytest.mark.postgres
@pytest.mark.parametrize('fault', [None, 'footprint', 'coverage'])
def test_actual_source_run_terminal_phases_and_reuse(clean_runtime, monkeypatch, fault):
    import psycopg
    from tests.substrate.test_source_runner_record_set import _StreamFootprintAdapter, _node
    from tests.substrate._db_helpers import seed_catalog_version
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    runner = SourceRunner(clean_runtime, source_name='test.tail-diagnostics',
                          write_fact_log=True, deletion_semantics='missing_from_completed_scope')
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    original = MemoryError('injected terminal failure')
    if fault == 'footprint':
        original_method = runner_module._StreamFootprintAccumulator.record_set
        monkeypatch.setattr(runner_module._StreamFootprintAccumulator, 'record_set',
                            lambda self: (_ for _ in ()).throw(original))
    elif fault == 'coverage':
        original_method = runner._verify_record_set_chronos_coverage
        monkeypatch.setattr(runner, '_verify_record_set_chronos_coverage',
                            lambda *args: (_ for _ in ()).throw(original))
    adapter = _StreamFootprintAdapter([{'facts': [_node('tail:a')]}, {'facts': [_node('tail:a')]}])
    if fault:
        with pytest.raises(MemoryError) as caught:
            runner.run(adapter, mode='scope_complete')
        assert caught.value is original
        expected = 'stream_footprint.assemble' if fault == 'footprint' else 'record_set.coverage'
        assert runner.tail_failure_diagnostic['phase'] == expected
        if fault == 'footprint':
            monkeypatch.setattr(runner_module._StreamFootprintAccumulator, 'record_set', original_method)
        else:
            monkeypatch.setattr(runner, '_verify_record_set_chronos_coverage', original_method)
    else:
        runner.run(adapter, mode='scope_complete')
        events = [json.loads(line) for line in sink.getvalue().splitlines() if line.startswith('{')]
        phases = [e['phase'] for e in events if e['status'] == 'started']
        assert phases.index('stream.final_flush') < phases.index('stream_footprint.assemble')
        assert phases.index('stream_footprint.assemble') < phases.index('record_set.serialize') < phases.index('record_set.coverage')
        authorities = [e for e in events if e['phase'] == 'record_set.serialize' and e['status'] == 'started']
        assert [(e['authority_ordinal'], e['verified']) for e in authorities] == [(1, False), (2, True)]
    prior_correlation = runner._tail_diagnostic_id
    sink.seek(0)
    sink.truncate(0)
    runner.run(adapter, mode='scope_complete')
    assert runner.tail_failure_diagnostic is None
    assert runner._tail_diagnostic_id != prior_correlation
    events = [json.loads(line) for line in sink.getvalue().splitlines() if line.startswith('{')]
    authorities = [e for e in events if e['phase'] == 'record_set.serialize' and e['status'] == 'started']
    assert authorities[0]['authority_ordinal'] == 1


@pytest.mark.parametrize('failure_site', ['write', 'construction'])
def test_lost_diagnostic_records_are_counted_not_hidden(monkeypatch, failure_site):
    """An observer that cannot emit leaves a count behind, never nothing."""
    runner = bare_runner()
    runner._tail_observer_failures = 0

    class BrokenSink(FlushedSink):
        def write(self, value):
            if failure_site == 'write':
                raise MemoryError('observer unavailable')
            return super().write(value)

    real_dumps = json.dumps

    def dumps(value, **kwargs):
        if failure_site == 'construction' and isinstance(value, dict) and 'phase' in value:
            raise MemoryError('observer unavailable')
        return real_dumps(value, **kwargs)

    monkeypatch.setattr(runner_module.json, 'dumps', dumps)
    monkeypatch.setattr(runner_module.sys, 'stderr', BrokenSink())
    original = ValueError('original')
    caught = None
    try:
        with runner._tail_phase('record_set.execute'):
            raise original
    except ValueError as exc:
        caught = exc
    assert caught is original
    # Both the started and the failed record were lost, and both are counted.
    assert runner._tail_observer_failures == 2
    # The primary record survives a dead sink: it is assigned before any I/O.
    assert runner.tail_failure_diagnostic['phase'] == 'record_set.execute'
    assert runner.tail_failure_diagnostic['exception_class'] == 'ValueError'


def test_run_result_discloses_lost_diagnostic_records(monkeypatch, tmp_path):
    """A silently unwritable observer is disclosed on the failing run result."""

    class DeadSink(FlushedSink):
        def write(self, value):
            raise MemoryError('observer unavailable')

    monkeypatch.setattr(runner_module.sys, 'stderr', DeadSink())
    error = RuntimeError(SECRET)

    class FailingRunner:
        _tail_phase = SourceRunner._tail_phase

        def __init__(self, *args, **kwargs):
            self.source_name = SECRET

        def run(self, *args, **kwargs):
            self._tail_observer_failures = 0
            with self._tail_phase('record_set.execute', payload_bytes=123):
                raise error

        def release_deferred_bootstrap_lock(self):
            pass

    context = SimpleNamespace(admission_id='unit-admission', run_id='unit-run')
    monkeypatch.setattr(service, 'SourceRunner', FailingRunner)
    monkeypatch.setattr(service, '_prepare_source_admission', lambda **kw: (context, {}, 'pending'))
    monkeypatch.setattr(service, '_run_mode_for_source', lambda *args: 'cursor')
    monkeypatch.setattr(service, 'import_source_adapter_class', lambda entry: SimpleNamespace)
    monkeypatch.setattr(service, 'source_adapter_graph_branch', lambda entry: 'default')
    monkeypatch.setattr(service, 'source_adapter_init_params', lambda *args, **kw: {})
    monkeypatch.setattr(service, 'bind_source_adapter_identity', lambda *args: None)
    monkeypatch.setattr(service, '_snapshot_source_state', lambda *args, **kw: None)
    monkeypatch.setattr(service, 'resolve_deployment_source_pii_posture', lambda **kw: None)
    monkeypatch.setattr(service, 'runtime_enforce_mode_dsn_keys', lambda *args: set())
    monkeypatch.setattr(service, '_retract_chronos_run', lambda *args, **kw: None)
    (tmp_path / 'deployment.yaml').write_text('{}')
    options = service.IngestOptions(dsn='postgres://unused', deployment='unit', deployment_dir=tmp_path)
    entry = SimpleNamespace(raw={}, ownership_id='unit.source', admission_contract=None)
    result = service._run_source(options, entry, 'unit')
    assert result['status'] == 'FAIL'
    assert result['source_tail_observer_failures'] == 2
    assert result['source_tail_failure']['phase'] == 'record_set.execute'
    assert SECRET not in json.dumps(result['source_tail_failure'])


def test_metadata_cannot_forge_fixed_fields_or_carry_non_scalars(monkeypatch):
    """Caller metadata is bounded: no overwrite, no oversized or non-scalar value."""
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    with runner._tail_phase(
        'record_set.execute',
        schema='forged', correlation='forged', verified='forged',
        authority_ordinal=999, status='forged', elapsed_ms='forged',
        blob=object(), oversized='x' * 129, kept_str='x' * 128, kept_int=7,
    ):
        pass
    event = json.loads(sink.getvalue().splitlines()[-1])
    assert event['schema'] == 'okg.source_tail.v1'
    assert event['phase'] == 'record_set.execute'
    assert event['status'] == 'completed'
    assert event['verified'] is False
    assert event['authority_ordinal'] == 0
    assert 'blob' not in event
    assert 'oversized' not in event
    assert event['kept_str'] == 'x' * 128
    assert event['kept_int'] == 7
    # A record that survives the filter is always serializable, so it can be
    # stored on the runner and returned without failing a second time.
    assert json.loads(json.dumps(event)) == event


def test_unserializable_metadata_never_reaches_the_failure_record(monkeypatch):
    """The stored diagnostic cannot become a second failure on the failure path."""
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    original = ValueError('original')
    caught = None
    try:
        with runner._tail_phase('record_set.execute', blob=object()):
            raise original
    except ValueError as exc:
        caught = exc
    assert caught is original
    stored = runner.tail_failure_diagnostic
    assert 'blob' not in stored
    assert json.loads(json.dumps(stored)) == stored


def test_phase_is_positional_only_so_metadata_cannot_collide(monkeypatch):
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    with runner._tail_phase('cursor_blob.insert', phase='forged'):
        pass
    assert json.loads(sink.getvalue().splitlines()[-1])['phase'] == 'cursor_blob.insert'


def test_already_stored_cursor_blobs_return_early_inside_the_phase(monkeypatch):
    """The early return inside a phase completes it and skips the insert."""
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._ensure_source_cursor_blobs_table = lambda: None
    executed = []
    cursor = SimpleNamespace(
        execute=lambda sql, params: None,
        fetchall=lambda: [('private-key', 'digest')],
        executemany=lambda *args: executed.append(args),
    )
    runner._acquire_conn = lambda: SimpleNamespace(cursor=lambda: nullcontext(cursor))
    runner._store_external_cursor_fields([('private-key', SECRET.encode(), 'digest', len(SECRET))])
    assert executed == []
    events = [json.loads(line) for line in sink.getvalue().splitlines()]
    assert [(e['phase'], e['status']) for e in events] == [
        ('cursor_blob.probe', 'started'), ('cursor_blob.probe', 'completed'),
    ]
    assert events[-1]['payload_bytes'] == len(SECRET)
    assert getattr(runner, 'tail_failure_diagnostic', None) is None
    assert SECRET not in sink.getvalue()
    assert 'private-key' not in sink.getvalue()


def test_bad_row_shape_fails_where_it_always_did_and_is_still_diagnosed(monkeypatch):
    """Phase metadata is computed defensively, so it cannot pre-empt the real error."""
    runner = bare_runner()
    sink = FlushedSink()
    monkeypatch.setattr(runner_module.sys, 'stderr', sink)
    runner._ensure_source_cursor_blobs_table = lambda: None
    cursor = SimpleNamespace(execute=lambda sql, params: None, fetchall=lambda: [],
                             executemany=lambda *args: None)
    runner._acquire_conn = lambda: SimpleNamespace(cursor=lambda: nullcontext(cursor))
    # A short row would make an unguarded `sum(row[3] ...)` raise IndexError at
    # the call site, before the phase opened, replacing the real failure with an
    # observer failure and leaving no diagnostic at all.
    with pytest.raises(ValueError, match='not enough values to unpack'):
        runner._store_external_cursor_fields([('private-key', SECRET.encode(), 'digest')])
    event = runner.tail_failure_diagnostic
    assert event['phase'] == 'cursor_blob.probe'
    assert event['exception_class'] == 'ValueError'
    # The unavailable size is disclosed as null rather than dropped.
    assert event['payload_bytes'] is None
    assert SECRET not in sink.getvalue()


def test_run_resets_every_diagnostic_field_without_a_database(monkeypatch):
    """The reset at the top of run() is proven without the postgres marker."""
    runner = bare_runner()
    runner.tail_failure_diagnostic = {'phase': 'stale'}
    runner._tail_observer_failures = 9
    runner._tail_authority_ordinal = 7
    runner._tail_verified = True
    runner._tail_diagnostic_id = 'stale-correlation'
    sentinel = RuntimeError('stop after the reset')

    def stop(adapter):
        raise sentinel

    runner._policy_profile = stop
    with pytest.raises(RuntimeError) as caught:
        SourceRunner.run(runner, SimpleNamespace())
    assert caught.value is sentinel
    assert runner.tail_failure_diagnostic is None
    assert runner._tail_observer_failures == 0
    assert runner._tail_authority_ordinal == 0
    assert runner._tail_verified is False
    assert runner._tail_diagnostic_id != 'stale-correlation'
