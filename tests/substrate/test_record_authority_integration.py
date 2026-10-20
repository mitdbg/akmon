"""Production indexed-v2 record authority against the real ``okg`` schema.

These tests exercise ``ProductionAuthorityStore`` on the migrated substrate
schema (migration 0031).  They prove three things the isolated prototype could
not: the v2 canonical digest is byte-identical to the legacy v1 digest over the
same records, the reader never holds a whole component in memory, and corrupt
or mismatched v2 authority fails closed with no legacy fallback.
"""
from __future__ import annotations

import hashlib
import json
import tracemalloc
import uuid

import psycopg
import pytest

from okg.substrate.ingest.record_authority import (
    IDENTITY_FIELD_MAX_BYTES,
    LIMIT,
    AuthorityError,
    PrivateAuthorityCandidate,
    ProductionAuthorityStore,
    ProductionReference,
    canonical,
)
from okg.substrate.library.sources.base import (
    EdgeKey,
    RecordAuthorityOperation,
    RecordAuthorityStream,
    RecordEmission,
    RecordNode,
    RecordSet,
)
from okg.substrate.record_authority_bench import (
    BENCH_SCHEMA,
    BenchShape,
    CountingConnection,
    main as bench_main,
    run_bench,
)

pytestmark = pytest.mark.postgres


# --------------------------------------------------------------------------
# fixtures and helpers
# --------------------------------------------------------------------------


@pytest.fixture
def conn(clean_runtime: str):
    with psycopg.connect(clean_runtime, autocommit=True) as connection:
        yield connection


@pytest.fixture
def store(conn):
    return ProductionAuthorityStore(conn)


def _identity(prefix: str = "") -> tuple[str, str, str]:
    return (prefix + "source-" + uuid.uuid4().hex, "incarnation-1", "attempt-1")


def _emission(key: str, *, body: str = "x") -> RecordEmission:
    """A record with a primary node, a descendant and an edge."""

    return RecordEmission(
        primary=RecordNode(key, "dataset", {"id": key, "body": body}, {"rev": 1}),
        descendants=[RecordNode(key + "-child", "dataset", {}, {"rev": 1})],
        edges=[EdgeKey(key, key + "-child", "contains", {"kind": "child"})],
    )


def _operations(records: dict[str, RecordEmission]):
    """Turn legacy emissions into ordered v2 operations, id-sorted."""

    for ordinal, key in enumerate(sorted(records)):
        emission = records[key]
        cursor = emission.to_cursor()
        yield RecordAuthorityOperation(
            ordinal=ordinal,
            record_id=key,
            primary=cursor["primary"],
            descendants=tuple(cursor["descendants"]),
            edges=tuple(cursor["edges"]),
        )


def _stream(records: dict[str, RecordEmission]) -> RecordAuthorityStream:
    operations = list(_operations(records))
    components = sum(
        (1 if op.primary is not None else 0)
        + len(tuple(op.descendants))
        + len(tuple(op.edges))
        for op in operations
    )
    return RecordAuthorityStream(
        operations=operations,
        logical_sha256=_legacy_digest(records),
        record_count=len(records),
        operation_count=len(operations),
        component_count=components,
    )


def _legacy_digest(records: dict[str, RecordEmission]) -> str:
    """Exactly what the legacy v1 path hashes: canonical RecordSet.to_cursor."""

    return hashlib.sha256(
        canonical(RecordSet(records).to_cursor()).encode("utf-8")
    ).hexdigest()


def _seal(store: ProductionAuthorityStore, records) -> ProductionReference:
    source, incarnation, attempt = _identity()
    candidate = store.stage_stream(source, incarnation, attempt, _stream(records))
    return store.seal_and_bind(candidate)


def _corrupt(conn, sql: str, params=None) -> None:
    """Damage stored authority despite the immutability triggers.

    The 0031 triggers refuse every ordinary mutation, which is the point of the
    design.  A corruption test therefore has to disable replication-role
    triggers deliberately; that is the only way to simulate storage damage.
    """

    conn.execute("SET session_replication_role = 'replica'")
    try:
        conn.execute(sql, params)
    finally:
        conn.execute("SET session_replication_role = 'origin'")


# --------------------------------------------------------------------------
# canonical-digest equivalence with the legacy v1 path
# --------------------------------------------------------------------------


def test_v2_seal_reproduces_the_exact_legacy_v1_digest(store):
    records = {
        "a": _emission("a"),
        "z": _emission("z"),
        "ä": _emission("ä", body="🌍 unicode"),
        "empty": RecordEmission(),
    }
    reference = _seal(store, records)

    assert reference.sha256 == _legacy_digest(records)
    assert reference.count == len(records)

    with store.reader(reference) as verified:
        read_back = b"".join(verified.iterate_canonical_bytes())
    assert read_back == canonical(RecordSet(records).to_cursor()).encode("utf-8")
    assert json.loads(read_back) == RecordSet(records).to_cursor()


def test_empty_v2_authority_matches_the_legacy_empty_digest(store):
    reference = _seal(store, {})
    assert reference.sha256 == hashlib.sha256(b'{"records":{}}').hexdigest()
    with store.reader(reference) as verified:
        assert b"".join(verified.iterate_canonical_bytes()) == b'{"records":{}}'


def test_retraction_removes_a_record_from_the_sealed_digest(store):
    live = {"keep": _emission("keep")}
    source, incarnation, attempt = _identity()
    operations = [
        RecordAuthorityOperation(
            ordinal=0,
            record_id="drop",
            primary=_emission("drop").to_cursor()["primary"],
            descendants=(),
            edges=(),
        ),
        RecordAuthorityOperation(ordinal=1, record_id="drop", retract=True),
        *[
            RecordAuthorityOperation(
                ordinal=2 + index,
                record_id=operation.record_id,
                primary=operation.primary,
                descendants=operation.descendants,
                edges=operation.edges,
            )
            for index, operation in enumerate(_operations(live))
        ],
    ]
    stream = RecordAuthorityStream(
        operations=operations,
        logical_sha256=_legacy_digest(live),
        record_count=1,
        operation_count=len(operations),
        component_count=1 + 3,
    )
    reference = store.seal_and_bind(
        store.stage_stream(source, incarnation, attempt, stream)
    )
    assert reference.sha256 == _legacy_digest(live)
    with store.reader(reference) as verified:
        assert list(verified.iterate_record_ids()) == ["keep"]


# --------------------------------------------------------------------------
# bounded memory: the NO-GO regression
# --------------------------------------------------------------------------


class _ByteAccountingStore(ProductionAuthorityStore):
    """Count the payload bytes the reader holds at once.

    Every stored payload byte reaches Python through ``_iter_payload_bytes``.
    Recording the largest single chunk and the largest chunk the reader has
    alive at any moment turns "the reader streams" into a measurable claim.
    """

    def __init__(self, conn):
        super().__init__(conn)
        self.max_chunk_bytes = 0
        self.total_payload_bytes = 0

    def _iter_payload_bytes(self, sha: str, *args, **kwargs):
        for chunk in super()._iter_payload_bytes(sha, *args, **kwargs):
            self.max_chunk_bytes = max(self.max_chunk_bytes, len(chunk))
            self.total_payload_bytes += len(chunk)
            yield chunk


def test_reader_never_holds_more_than_one_chunk_of_a_huge_component(clean_runtime):
    """A single component far larger than the chunk size streams in O(chunk).

    The component below is ~24 MiB of JSON in one field, i.e. 24x the 1 MiB
    chunk bound.  A reader that materialized the field (the reviewed NO-GO)
    would hold all of it; this proves peak held payload bytes stay at one chunk.
    """

    with psycopg.connect(clean_runtime, autocommit=True) as connection:
        store = _ByteAccountingStore(connection)
        huge = "q" * (24 * LIMIT)
        records = {"huge": RecordEmission(
            primary=RecordNode("huge", "dataset", {"blob": huge}, {"rev": 1})
        )}
        reference = _seal(store, records)
        assert reference.sha256 == _legacy_digest(records)

        store.max_chunk_bytes = 0
        store.total_payload_bytes = 0
        observed = 0
        # A chunk-size assertion alone would pass on a reader that buffered
        # every chunk it was handed, so measure allocated bytes as well.
        tracemalloc.start()
        try:
            tracemalloc.reset_peak()
            with store.reader(reference) as verified:
                for chunk in verified.iterate_canonical_bytes():
                    observed += len(chunk)
                    assert len(chunk) <= LIMIT
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        # The whole 24 MiB really did pass through the reader ...
        assert store.total_payload_bytes > 20 * LIMIT
        assert observed > 20 * LIMIT
        # ... never more than one chunk arrived at a time ...
        assert store.max_chunk_bytes <= LIMIT
        # ... and nothing accumulated: peak allocation stays a small multiple
        # of one chunk, far below the 24 MiB the reader streamed. A reader
        # that materialized the field would peak at or above 24 * LIMIT.
        assert peak < 4 * LIMIT, peak


def test_per_record_streams_are_also_bounded(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as connection:
        store = _ByteAccountingStore(connection)
        huge = "w" * (8 * LIMIT)
        records = {
            "a": _emission("a"),
            "huge": RecordEmission(
                primary=RecordNode("huge", "dataset", {"blob": huge}, {"rev": 1})
            ),
        }
        reference = _seal(store, records)
        store.max_chunk_bytes = 0
        seen: dict[str, int] = {}
        tracemalloc.start()
        try:
            tracemalloc.reset_peak()
            with store.reader(reference) as verified:
                for record_id, chunks in verified.iterate_record_bytes():
                    seen[record_id] = sum(len(chunk) for chunk in chunks)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert sorted(seen) == ["a", "huge"]
        assert seen["huge"] > 7 * LIMIT
        assert store.max_chunk_bytes <= LIMIT
        assert peak < 4 * LIMIT, peak


def test_identity_fields_are_refused_above_the_bounded_ceiling(store):
    source, incarnation, attempt = _identity()
    oversized = "n" * (IDENTITY_FIELD_MAX_BYTES + 1)
    stream = RecordAuthorityStream(
        operations=[RecordAuthorityOperation(
            ordinal=0,
            record_id="r",
            primary=RecordNode(oversized, "dataset", {}, {"rev": 1}).to_cursor(),
        )],
        logical_sha256="0" * 64,
        record_count=1,
        operation_count=1,
        component_count=1,
    )
    with pytest.raises(AuthorityError, match="identity field exceeds byte bound"):
        store.stage_stream(source, incarnation, attempt, stream)


# --------------------------------------------------------------------------
# fail-closed behavior: no fallback to legacy or inline
# --------------------------------------------------------------------------


def test_corrupt_payload_chunk_refuses_the_reader(store, conn):
    records = {"a": _emission("a")}
    reference = _seal(store, records)
    _corrupt(
        conn,
        "UPDATE okg.source_record_authority_payloads SET byte_count=byte_count+1 "
        "WHERE sha256=(SELECT payload_sha256 "
        "FROM okg.source_record_authority_component_fields "
        "WHERE authority_id=%s LIMIT 1)",
        (reference.authority,),
    )
    with pytest.raises(AuthorityError):
        with store.reader(reference) as verified:
            b"".join(verified.iterate_canonical_bytes())


def test_deleted_payload_chunk_refuses_the_reader(store, conn):
    records = {"a": _emission("a")}
    reference = _seal(store, records)
    _corrupt(
        conn,
        "DELETE FROM okg.source_record_authority_payload_chunks "
        "WHERE sha256=(SELECT payload_sha256 "
        "FROM okg.source_record_authority_component_fields "
        "WHERE authority_id=%s LIMIT 1)",
        (reference.authority,),
    )
    with pytest.raises(AuthorityError):
        with store.reader(reference) as verified:
            b"".join(verified.iterate_canonical_bytes())


def test_wrong_expected_digest_never_seals(store):
    source, incarnation, attempt = _identity()
    records = {"a": _emission("a")}
    stream = _stream(records)
    lied = RecordAuthorityStream(
        operations=stream.operations,
        logical_sha256="f" * 64,
        record_count=stream.record_count,
        operation_count=stream.operation_count,
        component_count=stream.component_count,
    )
    candidate = store.stage_stream(source, incarnation, attempt, lied)
    with pytest.raises(AuthorityError, match="digest or count mismatch"):
        store.seal_and_bind(candidate)
    assert store.conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_refs WHERE source=%s",
        (source,),
    ).fetchone()[0] == 0
    assert store.conn.execute(
        "SELECT state FROM okg.source_record_authorities WHERE authority_id=%s",
        (candidate.authority,),
    ).fetchone()[0] == "staging"


def test_wrong_expected_record_count_never_seals(store):
    source, incarnation, attempt = _identity()
    records = {"a": _emission("a")}
    stream = _stream(records)
    lied = RecordAuthorityStream(
        operations=stream.operations,
        logical_sha256=stream.logical_sha256,
        record_count=stream.record_count + 1,
        operation_count=stream.operation_count,
        component_count=stream.component_count,
    )
    candidate = store.stage_stream(source, incarnation, attempt, lied)
    with pytest.raises(AuthorityError, match="digest or count mismatch"):
        store.seal_and_bind(candidate)


def test_wrong_component_count_is_refused_while_staging(store):
    source, incarnation, attempt = _identity()
    records = {"a": _emission("a")}
    stream = _stream(records)
    lied = RecordAuthorityStream(
        operations=stream.operations,
        logical_sha256=stream.logical_sha256,
        record_count=stream.record_count,
        operation_count=stream.operation_count,
        component_count=stream.component_count + 1,
    )
    with pytest.raises(AuthorityError, match="count mismatch"):
        store.stage_stream(source, incarnation, attempt, lied)


def test_staged_authority_is_not_readable_before_sealing(store):
    source, incarnation, attempt = _identity()
    records = {"a": _emission("a")}
    candidate = store.stage_stream(source, incarnation, attempt, _stream(records))
    unsealed = ProductionReference(
        candidate.authority, source, incarnation,
        candidate.expected_sha256, candidate.expected_records,
    )
    with pytest.raises(AuthorityError, match="exact current sealed authority"):
        with store.reader(unsealed):
            pass


def test_a_superseded_authority_is_not_readable(store):
    source, incarnation, attempt = _identity()
    first_records = {"a": _emission("a")}
    first = store.seal_and_bind(
        store.stage_stream(source, incarnation, attempt, _stream(first_records))
    )
    second_records = {"a": _emission("a"), "b": _emission("b")}
    store.seal_and_bind(
        store.stage_stream(source, incarnation, "attempt-2", _stream(second_records))
    )
    with pytest.raises(AuthorityError, match="exact current sealed authority"):
        with store.reader(first):
            pass


def test_a_deleted_identity_row_refuses_the_reader(store, conn):
    records = {"a": _emission("a")}
    reference = _seal(store, records)
    _corrupt(
        conn,
        "DELETE FROM okg.source_record_authority_identities "
        "WHERE authority_id=%s AND identity_kind='edge'",
        (reference.authority,),
    )
    with pytest.raises(AuthorityError, match="identity index"):
        with store.reader(reference):
            pass


def test_reader_rejects_an_escaped_iterator(store):
    reference = _seal(store, {"a": _emission("a"), "b": _emission("b")})
    with store.reader(reference) as verified:
        escaped = verified.iterate_record_ids()
        assert next(escaped) == "a"
    with pytest.raises(AuthorityError, match="closed"):
        next(escaped)


# --------------------------------------------------------------------------
# bounded consumer surface (coverage / retraction)
# --------------------------------------------------------------------------


def test_missing_identities_anti_join_is_authenticated_and_bounded(store):
    records = {"a": _emission("a")}
    reference = _seal(store, records)
    expected = [
        ("node", "a"),
        ("node", "a-child"),
        ("node", "absent"),
        ("edge", canonical(["a", "contains", "a-child"])),
        ("edge", canonical(["a", "missing", "a-child"])),
    ]
    with store.reader(reference) as verified:
        missing = sorted(verified.missing_identities(expected))
    assert missing == [
        ("edge", canonical(["a", "missing", "a-child"])),
        ("node", "absent"),
    ]


def test_identity_iteration_reports_ownership(store):
    records = {"a": _emission("a")}
    reference = _seal(store, records)
    with store.reader(reference) as verified:
        identities = sorted(verified.iterate_identities())
    assert ("node", "a", "authoritative") in identities
    assert ("edge", canonical(["a", "contains", "a-child"]), "") in identities


# --------------------------------------------------------------------------
# legacy v1 storage is untouched
# --------------------------------------------------------------------------


def test_v2_writes_never_touch_the_legacy_record_set_tables(store, conn):
    before = conn.execute(
        "SELECT count(*) FROM okg.source_record_sets"
    ).fetchone()[0]
    blobs_before = conn.execute(
        "SELECT count(*) FROM okg.source_record_set_blobs"
    ).fetchone()[0]
    _seal(store, {"a": _emission("a"), "b": _emission("b")})
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_sets"
    ).fetchone()[0] == before
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_set_blobs"
    ).fetchone()[0] == blobs_before


def test_pending_admission_handoff_is_not_a_public_reference(store):
    """The staged handoff carries no cursor shape and no public reference."""

    source, incarnation, attempt = _identity()
    candidate = store.stage_stream(
        source, incarnation, attempt, _stream({"a": _emission("a")})
    )
    assert isinstance(candidate, PrivateAuthorityCandidate)
    assert not hasattr(candidate, "to_cursor")
    assert store.conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_refs WHERE source=%s",
        (source,),
    ).fetchone()[0] == 0


def test_no_whole_field_decoder_survives_on_the_production_store(store):
    """Regression for the reviewed NO-GO.

    The pre-fix module carried ``_load_payload``/``_component_values``/
    ``_iter_records``: a whole-record reader that decoded every component field
    into Python objects, so one large record needed whole-field memory. Those
    are gone, and the only decoder left refuses anything above the identity
    ceiling instead of buffering it.
    """

    for removed in ("_load_payload", "_component_values", "_iter_records"):
        assert not hasattr(ProductionAuthorityStore, removed), removed

    oversized = "z" * (IDENTITY_FIELD_MAX_BYTES * 8)
    sha = store._store_payload(oversized)
    with store.conn.transaction():
        with pytest.raises(AuthorityError, match="exceeds byte bound"):
            store._load_bounded_payload(sha, max_bytes=IDENTITY_FIELD_MAX_BYTES)
    with store.conn.transaction():
        # The same bytes still stream without a ceiling.
        assert sum(
            len(chunk) for chunk in store._iter_payload_bytes(sha)
        ) == len(canonical(oversized).encode("utf-8"))


def test_run_result_declares_the_private_authority_field() -> None:
    """A rename of the handoff field must fail here, not go quiet.

    ``_finalize_source_admission`` reads ``private_authority_candidate`` off the
    run result with a ``None`` default, because every in-memory RunResult double
    in the suite would otherwise raise. That default is safe only while the real
    dataclass still declares the field: if it were renamed, the service would
    silently stop sealing instead of erroring. This is the loud check, and it
    mirrors ``test_run_result_exposes_rejected_fact_count_field``.
    """

    import dataclasses

    from okg.substrate.ingest.runner import RunResult

    names = {field.name for field in dataclasses.fields(RunResult)}
    assert "private_authority_candidate" in names


# --------------------------------------------------------------------------
# batched staging: boundaries, dedupe, and resumability
# --------------------------------------------------------------------------


class _FlushCountingStore(ProductionAuthorityStore):
    """Record what each write batch carried."""

    def __init__(self, conn):
        super().__init__(conn)
        self.flushes: list[tuple[int, int, int]] = []

    def _flush_batch(self, batch):
        carried = (len(batch.operations), len(batch.payloads), len(batch.chunks))
        super()._flush_batch(batch)
        if any(carried):
            self.flushes.append(carried)


class _ChunkTrackingStore(_FlushCountingStore):
    """Record which payload digests were ever carried by a write batch."""

    def __init__(self, conn):
        super().__init__(conn)
        self.batched_shas: set[str] = set()

    def _flush_batch(self, batch):
        self.batched_shas.update(row[0] for row in batch.chunks)
        super()._flush_batch(batch)


class _FailingFlushStore(_FlushCountingStore):
    """Fail the nth write batch after its rows are already in the transaction.

    The failure is raised from the last step of the flush, so the operation,
    component, field and identity rows of that batch have been written and
    have to be rolled back. A failure raised before the first statement would
    prove only that a batch which never started leaves nothing behind.
    """

    def __init__(self, conn, fail_on: int):
        super().__init__(conn)
        self._fail_on = fail_on
        self.committed_operations = 0

    def _write_batch_records(self, authority):
        if len(self.flushes) + 1 == self._fail_on:
            raise RuntimeError("staging batch failed")
        super()._write_batch_records(authority)

    def _flush_batch(self, batch):
        pending = len(batch.operations)
        super()._flush_batch(batch)
        self.committed_operations += pending


def _wide_operation(ordinal: int, record_id: str, *, descendants: int, body: int):
    """One record whose components are large enough to cross a batch bound."""

    return RecordAuthorityOperation(
        ordinal=ordinal,
        record_id=record_id,
        primary=RecordNode(record_id, "dataset", {}, {"rev": 1}).to_cursor(),
        descendants=tuple(
            RecordNode(
                "%s-child-%d" % (record_id, index), "dataset",
                {"body": "b" * body}, {"rev": 1},
            ).to_cursor()
            for index in range(descendants)
        ),
    )


def _stream_of(operations, records: dict[str, RecordEmission]) -> RecordAuthorityStream:
    operations = list(operations)
    components = sum(
        (1 if op.primary is not None else 0)
        + len(tuple(op.descendants)) + len(tuple(op.edges))
        for op in operations
    )
    return RecordAuthorityStream(
        operations=operations,
        logical_sha256=_legacy_digest(records),
        record_count=len(records),
        operation_count=len(operations),
        component_count=components,
    )


def test_staging_costs_far_fewer_round_trips_than_records(conn):
    """The write path is batched, not one transaction per record.

    Before batching, each record cost a transaction plus roughly ten
    statements; 200 records could not be staged in fewer than 200 round trips.
    """

    counting = CountingConnection(conn)
    store = ProductionAuthorityStore(counting)
    records = {"rec-%04d" % index: _emission("rec-%04d" % index)
               for index in range(200)}
    source, incarnation, attempt = _identity()
    object.__setattr__(counting, "round_trips", 0)
    candidate = store.stage_stream(source, incarnation, attempt, _stream(records))
    staged_round_trips = counting.round_trips
    assert staged_round_trips < 200, staged_round_trips
    reference = store.seal_and_bind(candidate)
    assert reference.sha256 == _legacy_digest(records)


def test_one_record_spanning_several_batches_round_trips(conn, monkeypatch):
    """A single record wider than the batch bound is written across batches.

    Its payload rows go out as soon as the buffer is full — no operation row
    references them yet — and its index rows follow in the batch that closes
    the record. The sealed digest is still the legacy digest.
    """

    monkeypatch.setattr(
        "okg.substrate.ingest.record_authority.STAGE_BATCH_BYTES", 32 * 1024
    )
    store = _FlushCountingStore(conn)
    record_id = "wide"
    emission = RecordEmission(
        primary=RecordNode(record_id, "dataset", {}, {"rev": 1}),
        descendants=[
            RecordNode(
                "%s-child-%d" % (record_id, index), "dataset",
                {"body": "b" * (64 * 1024)}, {"rev": 1},
            )
            for index in range(48)
        ],
    )
    records = {record_id: emission}
    operations = [_wide_operation(0, record_id, descendants=48, body=64 * 1024)]
    source, incarnation, attempt = _identity()
    candidate = store.stage_stream(
        source, incarnation, attempt, _stream_of(operations, records)
    )
    # More than one batch, and every batch but the last carried payload rows
    # for a record whose operation row had not been written yet.
    assert len(store.flushes) > 1, store.flushes
    assert [carried[0] for carried in store.flushes[:-1]] == [
        0 for _ in store.flushes[:-1]
    ], store.flushes
    assert store.flushes[-1][0] == 1
    reference = store.seal_and_bind(candidate)
    assert reference.sha256 == _legacy_digest(records)
    with store.reader(reference) as verified:
        assert list(verified.iterate_record_ids()) == [record_id]


def test_a_multi_chunk_field_inside_a_batched_stage_stays_whole(conn, monkeypatch):
    """A field larger than one chunk keeps its chunk sequence in one write.

    Batches are bounded by encoded bytes, so a multi-megabyte field would
    otherwise be split across several of them and a crash could leave a
    payload a later writer reads as already stored. Such a field is written by
    its own transaction instead, and the batches split around it: none of them
    ever carries one of its chunk rows.
    """

    monkeypatch.setattr(
        "okg.substrate.ingest.record_authority.STAGE_BATCH_BYTES", 32 * 1024
    )
    store = _ChunkTrackingStore(conn)
    big = "m" * (3 * LIMIT)
    # Enough small records on either side that the stage really is several
    # batches, so "the batches split around it" is a claim with a witness.
    records = {
        "rec-%04d" % index: RecordEmission(
            primary=RecordNode(
                "rec-%04d" % index, "dataset",
                {"body": "s" * (16 * 1024)}, {"rev": index},
            )
        )
        for index in range(160)
    }
    records["big"] = RecordEmission(
        primary=RecordNode("big", "dataset", {"blob": big}, {"rev": 1})
    )
    reference = _seal(store, records)
    assert reference.sha256 == _legacy_digest(records)
    sha = conn.execute(
        "SELECT sha256 FROM okg.source_record_authority_payloads "
        "WHERE chunk_count > 1"
    ).fetchall()
    assert len(sha) == 1, sha
    stored = conn.execute(
        "SELECT p.chunk_count,(SELECT count(*) "
        "  FROM okg.source_record_authority_payload_chunks c "
        " WHERE c.sha256=p.sha256) "
        "FROM okg.source_record_authority_payloads p WHERE p.sha256=%s",
        (sha[0][0],),
    ).fetchone()
    assert stored[0] == stored[1] > 1, stored
    # The batches split around it: no batch buffered any of its rows, and the
    # stage really was batched rather than one flush at the end.
    assert sha[0][0] not in store.batched_shas, store.batched_shas
    assert len(store.flushes) > 1, store.flushes


def test_a_revision_shared_across_batches_is_stored_exactly_once(conn, monkeypatch):
    """Content-addressed dedupe survives the batch boundary.

    Two records far enough apart to land in different write batches carry the
    same component field. One payload row and one chunk row exist for it, not
    one per batch.
    """

    monkeypatch.setattr(
        "okg.substrate.ingest.record_authority.STAGE_BATCH_BYTES", 32 * 1024
    )
    store = _FlushCountingStore(conn)
    shared = {"body": "s" * (32 * 1024)}
    records = {}
    for index in range(64):
        key = "rec-%04d" % index
        records[key] = RecordEmission(
            primary=RecordNode(key, "dataset", dict(shared), {"rev": 1})
        )
    source, incarnation, attempt = _identity()
    candidate = store.stage_stream(source, incarnation, attempt, _stream(records))
    assert len(store.flushes) > 1, store.flushes
    reference = store.seal_and_bind(candidate)
    assert reference.sha256 == _legacy_digest(records)
    shared_sha = hashlib.sha256(
        canonical(shared).encode("utf-8")
    ).hexdigest()
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_payloads "
        "WHERE sha256=%s", (shared_sha,),
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_payload_chunks "
        "WHERE sha256=%s", (shared_sha,),
    ).fetchone()[0] == 1


def test_a_failed_batch_leaves_the_authority_unreadable_and_resumable(conn, monkeypatch):
    """A batch that fails part way through publishes nothing and can be redone.

    The failing batch has already copied its rows when it raises, so this also
    proves those rows roll back. The attempt keeps whatever earlier batches
    committed, so a retry restages from ordinal zero over rows that are
    already there, and the sealed digest is the one the complete stream
    declares.
    """

    monkeypatch.setattr(
        "okg.substrate.ingest.record_authority.STAGE_BATCH_BYTES", 32 * 1024
    )
    records = {"rec-%04d" % index: _emission("rec-%04d" % index)
               for index in range(400)}
    source, incarnation, attempt = _identity()

    failing = _FailingFlushStore(conn, fail_on=2)
    with pytest.raises(RuntimeError, match="staging batch failed"):
        failing.stage_stream(source, incarnation, attempt, _stream(records))
    assert len(failing.flushes) == 1, failing.flushes

    authority = conn.execute(
        "SELECT authority_id,state FROM okg.source_record_authorities "
        "WHERE source=%s AND incarnation=%s AND attempt=%s",
        (source, incarnation, attempt),
    ).fetchone()
    assert authority[1] == "staging"
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_refs WHERE source=%s",
        (source,),
    ).fetchone()[0] == 0
    partial = conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_operations "
        "WHERE authority_id=%s", (authority[0],),
    ).fetchone()[0]
    # Exactly the batches that committed, and none of the failed batch: its
    # operation, component, field and identity rows were written inside the
    # transaction and rolled back with it.
    assert 0 < partial < len(records), partial
    assert partial == failing.committed_operations, (
        partial, failing.committed_operations,
    )
    with pytest.raises(AuthorityError, match="exact current sealed authority"):
        with ProductionAuthorityStore(conn).reader(ProductionReference(
            str(authority[0]), source, incarnation, _legacy_digest(records),
            len(records),
        )):
            pass

    resumed = ProductionAuthorityStore(conn)
    candidate = resumed.stage_stream(
        source, incarnation, attempt, _stream(records)
    )
    assert candidate.authority == str(authority[0])
    reference = resumed.seal_and_bind(candidate)
    assert reference.sha256 == _legacy_digest(records)
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_operations "
        "WHERE authority_id=%s", (authority[0],),
    ).fetchone()[0] == len(records)


def test_every_record_shape_reproduces_the_legacy_digest(store):
    """The one-cursor canonical sweep re-emits every body shape byte for byte.

    The sweep replaced a nested per-record walk, so the shapes that differ only
    by which component arrays are empty are exactly where it could drift: an
    array separator emitted for an empty array, a primary rendered as an empty
    object instead of null, a comma missing between two descendants.
    """

    records = {
        "empty": RecordEmission(),
        "edges-only": RecordEmission(
            edges=[EdgeKey("edges-only", "other", "contains", {})]
        ),
        "descendants-only": RecordEmission(
            descendants=[
                RecordNode("descendants-only-a", "dataset", {}, {"rev": 1}),
                RecordNode("descendants-only-b", "dataset", {}, {"rev": 1}),
            ]
        ),
        "primary-only": RecordEmission(
            primary=RecordNode("primary-only", "dataset", {}, {"rev": 1})
        ),
        "primary-and-edges": RecordEmission(
            primary=RecordNode("primary-and-edges", "dataset", {}, {"rev": 1}),
            edges=[EdgeKey("primary-and-edges", "other", "contains", {})],
        ),
        "primary-and-descendants": RecordEmission(
            primary=RecordNode("primary-and-descendants", "dataset", {}, {"rev": 1}),
            descendants=[
                RecordNode("primary-and-descendants-a", "dataset", {}, {"rev": 1})
            ],
        ),
        "unicode-ü-\U0001f30d": _emission(
            "unicode-ü-\U0001f30d", body="\U0001f30d body"
        ),
        "all-three": _emission("all-three"),
    }
    reference = _seal(store, records)
    assert reference.sha256 == _legacy_digest(records)
    with store.reader(reference) as verified:
        assert list(verified.iterate_record_ids()) == sorted(records)
        assert b"".join(verified.iterate_canonical_bytes()) == canonical(
            RecordSet(records).to_cursor()
        ).encode("utf-8")


# --------------------------------------------------------------------------
# the committed benchmark harness
# --------------------------------------------------------------------------


#: Logical digest of 500 records at the lean guard shape, pinned so a change
#: to the generator or to the canonical encoding fails here instead of quietly
#: making two benchmark runs incomparable.
LEAN_DIGEST = "880375c07cc770186c380fd61a1e74af6a3c8017327309179c4fcde9b3cb353b"

#: Same, for the 25-record determinism guard.
SEED_7_DIGEST = "443ed69d893c09b2f641062edba01f1e030d6a25ea3120bb1f1bdd6189e1dd77"


def test_bench_harness_stages_and_seals_in_far_fewer_round_trips(clean_runtime):
    """The committed harness runs, reports, and shows the write path batching.

    This is the fast guard for the same property
    ``test_staging_costs_far_fewer_round_trips_than_records`` asserts, but
    through the artifact operators actually re-run.  An unbatched write path
    costs at least one flush -- tens of statements -- per record, so 500
    records would cost thousands of round trips.  The bound is a constant, not
    a fraction of the record count: staging cost here is set by the number of
    batches and the fixed per-authority statements, so it barely moves with N,
    and a bound that scaled with N would loosen as the benchmark grew.  The
    measured cost at this shape is 67 round trips, so the bound leaves real
    headroom without leaving room for a per-record flush.  The shape is
    deliberately lean so the guard stays under a second; the harness default
    shape is the one used for measurement.
    """

    report = run_bench(
        clean_runtime,
        records=500,
        shape=BenchShape(descendants=0, edges=0, body_bytes=32),
        seed=1664,
    )

    assert report["schema"] == BENCH_SCHEMA
    assert report["records"] == 500
    assert report["logical_sha256"] == LEAN_DIGEST
    assert report["total_seconds"] > 0
    assert report["records_per_second"] > 0
    assert report["row_counts"]["okg.source_record_authority_operations"] == 500
    assert report["row_counts"]["okg.source_record_authority_records"] == 500
    assert report["round_trips"]["total"] < 120, report["round_trips"]


def test_bench_harness_batches_a_realistic_record_shape(clean_runtime):
    """Batching also holds for a record with descendants, edges and prose.

    The lean guard above strips a record down to its primary node, which is
    the cheapest shape there is.  This one runs the shape the committed 20k
    and 200k measurements used, where each record carries two descendants,
    three edges and a 384-byte body. The previous 1 MiB batch cost 426
    round trips for 500 records. The larger bounded, deduplicated batch must
    reduce that cost while preserving every component.
    """

    report = run_bench(
        clean_runtime,
        records=500,
        shape=BenchShape(descendants=2, edges=3, body_bytes=384),
        seed=1664,
    )

    assert report["records"] == 500
    assert report["row_counts"]["okg.source_record_authority_components"] == 3000
    assert report["round_trips"]["stage"] < 65, report["round_trips"]
    assert report["round_trips"]["total"] < 300, report["round_trips"]


def test_bench_harness_is_deterministic_for_one_seed(clean_runtime):
    """The same seed and shape produce the same logical authority digest.

    Both halves matter: two runs in this process must agree, and they must
    agree with the digest pinned when the harness was committed, so a later
    change to the generator cannot silently make new numbers incomparable with
    the attached 20k and 200k reports.
    """

    shape = BenchShape(descendants=1, edges=1, body_bytes=32)
    first = run_bench(clean_runtime, records=25, shape=shape, seed=7)
    second = run_bench(clean_runtime, records=25, shape=shape, seed=7)
    assert first["logical_sha256"] == second["logical_sha256"]
    assert first["logical_sha256"] == SEED_7_DIGEST
    assert first["authority_id"] != second["authority_id"]


def test_bench_harness_records_the_starting_row_counts(clean_runtime):
    """The report says what was already in the database before it staged.

    The first run on a clean database starts from zero; a second run starts
    from what the first left behind, because an authority write is immutable
    and is never cleaned up.
    """

    first = run_bench(
        clean_runtime, records=5,
        shape=BenchShape(descendants=0, edges=0, body_bytes=16), seed=3,
    )
    second = run_bench(
        clean_runtime, records=5,
        shape=BenchShape(descendants=0, edges=0, body_bytes=16), seed=3,
    )

    table = "okg.source_record_authority_records"
    assert first["baseline_rows"][table] == 0
    assert second["baseline_rows"][table] == 5
    assert second["row_counts"][table] == 5


def test_bench_harness_drops_edges_that_have_no_descendant_to_point_at(
    clean_runtime,
):
    """Edges without descendants are dropped, and the report says so."""

    report = run_bench(
        clean_runtime, records=4,
        shape=BenchShape(descendants=0, edges=3, body_bytes=16), seed=5,
    )

    assert report["shape_edges_clamped"] is True
    assert report["shape"]["edges"] == 0
    assert report["row_counts"]["okg.source_record_authority_components"] == 4


def test_bench_cli_refuses_a_zero_record_count():
    """``--records 0`` is a usage error, not a traceback from run_bench."""

    with pytest.raises(SystemExit):
        bench_main(["--dsn", "postgres:///nowhere", "--records", "0"])


def test_realistic_stage_buffer_bounds_memory_and_resets_dedupe(conn, record_property):
    """A source larger than the batch keeps payload and dedupe memory bounded."""

    from okg.substrate.ingest.record_authority import STAGE_BATCH_BYTES
    from okg.substrate.record_authority_bench import build_stream

    store = _FlushCountingStore(conn)
    stream = build_stream(1500, BenchShape(), 1664)
    tracemalloc.start()
    try:
        candidate = store.stage_stream(*_identity(), stream)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    record_property("peak_traced_stage_bytes", peak)
    record_property("stage_batches", len(store.flushes))
    assert len(store.flushes) > 1
    # Includes Python row tuples, digest strings, dedupe set, COPY adaptation,
    # and generation of the next record, not just encoded payload lengths.
    assert peak < 4 * STAGE_BATCH_BYTES, peak
    reference = store.seal_and_bind(candidate)
    assert reference.count == 1500
    assert reference.sha256 == stream.logical_sha256


def test_seal_identity_spill_keeps_set_semantics_across_copy_batches(
    conn, monkeypatch,
):
    """Repeated node/edge identities may span COPYs without masking damage."""

    monkeypatch.setattr(
        "okg.substrate.ingest.record_authority.IDENTITY_BATCH_ROWS", 2,
    )
    store = ProductionAuthorityStore(conn)
    primary = RecordNode("shared", "dataset", {}, {"rev": 1})
    edge = EdgeKey("shared", "child", "contains", {})
    records = {"a": RecordEmission(
        primary=primary,
        descendants=[primary, primary],
        edges=[edge, edge, edge],
    )}
    reference = _seal(store, records)
    assert reference.sha256 == _legacy_digest(records)
    with store.reader(reference) as reader:
        assert sorted(reader.iterate_identities()) == [
            ("edge", canonical(["shared", "contains", "child"]), ""),
            ("node", "shared", "authoritative"),
        ]
    _corrupt(
        conn,
        "DELETE FROM okg.source_record_authority_identities "
        "WHERE authority_id=%s AND identity_kind='edge'",
        (reference.authority,),
    )
    with pytest.raises(AuthorityError, match="corrupt authority identity index"):
        with store.reader(reference):
            pass


# --------------------------------------------------------------------------
# verified private staged identities (no production activation)
# --------------------------------------------------------------------------


def _candidate(store, records=None):
    records = {"a": _emission("a")} if records is None else records
    return store.stage_stream(*_identity(), _stream(records))


def _private_reader(store, candidate, **bindings):
    return store.private_identity_reader(candidate, **{
        "expected_source": candidate.source,
        "expected_incarnation": candidate.incarnation,
        "expected_attempt": candidate.attempt,
        **bindings,
    })


def test_private_reader_is_eager_declared_and_never_public(store, conn):
    from okg.substrate.ingest.record_authority import VerifiedPrivateIdentityReader

    candidate = _candidate(store)
    public = ProductionReference(
        candidate.authority, candidate.source, candidate.incarnation,
        candidate.expected_sha256, candidate.expected_records,
    )
    with _private_reader(store, candidate) as reader:
        assert isinstance(reader, VerifiedPrivateIdentityReader)
        assert not any(hasattr(reader, name) for name in (
            "reference", "to_cursor", "verified", "iterate_canonical_bytes",
        ))
        # Diagnostic SQL witnesses declaration before the first identity read.
        assert conn.execute(
            "SELECT count(*) FROM pg_cursors WHERE name LIKE 'authority_identity_rows_%'"
        ).fetchone()[0] == 1
        assert list(reader.iterate_identities()) == [
            ("edge", canonical(["a", "contains", "a-child"]), ""),
            ("node", "a", "authoritative"),
            ("node", "a-child", "authoritative"),
        ]
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_refs WHERE source=%s",
        (candidate.source,),
    ).fetchone()[0] == 0
    with pytest.raises(AuthorityError, match="exact current sealed authority"):
        with store.reader(public):
            pytest.fail("a private read must not make a public reference")
    assert conn.execute(
        "SELECT count(*) FROM pg_cursors WHERE name LIKE 'authority_identity_rows_%'"
    ).fetchone()[0] == 0
    assert store.seal_and_bind(candidate).sha256 == candidate.expected_sha256


@pytest.mark.parametrize("binding", ["source", "incarnation", "attempt"])
def test_private_reader_requires_independent_run_bindings(store, binding):
    candidate = _candidate(store)
    with pytest.raises(AuthorityError, match="run binding mismatch"):
        with _private_reader(store, candidate, **{"expected_" + binding: "wrong"}):
            pytest.fail("mismatched caller context escaped")
    with _private_reader(store, candidate) as reader:
        assert list(reader.iterate_identities())


@pytest.mark.parametrize("field,value", [
    ("authority", "not-a-uuid"), ("authority", None),
    ("source", ""), ("incarnation", 1), ("attempt", "x" * 2049),
    ("expected_sha256", "0" * 63), ("expected_sha256", "G" * 64),
    ("expected_sha256", None),
    ("expected_records", True), ("expected_operations", False),
    ("expected_components", True), ("expected_records", -1),
    ("expected_operations", 0.0), ("expected_components", "0"),
])
def test_private_reader_rejects_malformed_candidates_before_output(store, field, value):
    from dataclasses import replace

    candidate = replace(_candidate(store, {}), **{field: value})
    with pytest.raises(AuthorityError):
        with _private_reader(store, candidate):
            pytest.fail("malformed empty candidate escaped")


def test_private_reader_rejects_wrong_type_missing_sealed_and_forged_header(store):
    from dataclasses import replace

    with pytest.raises(AuthorityError, match="invalid private authority candidate"):
        with store.private_identity_reader(
            object(), expected_source="a", expected_incarnation="b", expected_attempt="c",
        ):
            pytest.fail("wrong candidate type escaped")
    candidate = _candidate(store)
    for forged in (
        replace(candidate, authority=str(uuid.uuid4())),
        replace(candidate, source="forged-source"),
    ):
        with pytest.raises(AuthorityError):
            with _private_reader(store, forged):
                pytest.fail("missing or forged header escaped")
    store.seal_and_bind(candidate)
    with pytest.raises(AuthorityError, match="sealed or mismatched"):
        with _private_reader(store, candidate):
            pytest.fail("sealed candidate escaped the private boundary")


@pytest.mark.parametrize("field", [
    "expected_sha256", "expected_records", "expected_operations", "expected_components",
])
def test_private_reader_rejects_wrong_expectations_even_when_abandoned(store, field):
    from dataclasses import replace

    candidate = _candidate(store, {})
    value = "f" * 64 if field == "expected_sha256" else 1
    with pytest.raises(AuthorityError):
        with _private_reader(store, replace(candidate, **{field: value})):
            pytest.fail("verification was deferred until iteration")
    with _private_reader(store, candidate) as reader:
        assert list(reader.iterate_identities()) == []


def _historical_candidate(store):
    old = _emission("old").to_cursor()
    live = {"same": _emission("live")}
    new = live["same"].to_cursor()
    operations = [
        RecordAuthorityOperation(
            ordinal=0, record_id="same", primary=old["primary"],
            descendants=tuple(old["descendants"]), edges=tuple(old["edges"]),
        ),
        RecordAuthorityOperation(ordinal=1, record_id="same", retract=True),
        RecordAuthorityOperation(
            ordinal=2, record_id="same", primary=new["primary"],
            descendants=tuple(new["descendants"]), edges=tuple(new["edges"]),
        ),
    ]
    return store.stage_stream(*_identity(), _stream_of(operations, live))


@pytest.mark.parametrize("corruption", ["missing_history", "swapped_counts", "ordinal_gap"])
def test_private_reader_authenticates_operation_and_historical_component_structure(
    store, conn, corruption,
):
    candidate = _historical_candidate(store)
    if corruption == "missing_history":
        _corrupt(conn,
            "DELETE FROM okg.source_record_authority_components "
            "WHERE authority_id=%s AND operation_ordinal=0 AND component_kind='edge'",
            (candidate.authority,),
        )
    elif corruption == "swapped_counts":
        # The declared total stays six, but neither operation's count is true.
        _corrupt(conn,
            "UPDATE okg.source_record_authority_operations "
            "SET component_count=CASE ordinal WHEN 0 THEN 2 ELSE 4 END "
            "WHERE authority_id=%s AND ordinal IN (0,2)", (candidate.authority,),
        )
    else:
        _corrupt(conn,
            "UPDATE okg.source_record_authority_operations SET ordinal=4 "
            "WHERE authority_id=%s AND ordinal=1", (candidate.authority,),
        )
    with pytest.raises(AuthorityError, match="sequence|physical component count"):
        with _private_reader(store, candidate):
            pytest.fail("corrupt historical structure escaped")
    with pytest.raises(AuthorityError, match="sequence|physical component count"):
        store.seal_and_bind(candidate)


@pytest.mark.parametrize("damage", ["identity", "non_identity", "missing_chunk", "index"])
def test_private_reader_authenticates_live_bytes_and_index_before_yield(store, conn, damage):
    candidate = _candidate(store)
    if damage == "index":
        _corrupt(conn,
            "DELETE FROM okg.source_record_authority_identities "
            "WHERE authority_id=%s AND identity_kind='edge'", (candidate.authority,),
        )
    else:
        field = "node_id" if damage == "identity" else "source_revision"
        sha = conn.execute(
            "SELECT payload_sha256 FROM okg.source_record_authority_component_fields "
            "WHERE authority_id=%s AND component_kind='primary' AND field_name=%s",
            (candidate.authority, field),
        ).fetchone()[0]
        if damage == "missing_chunk":
            _corrupt(conn, "DELETE FROM okg.source_record_authority_payload_chunks WHERE sha256=%s", (sha,))
        else:
            _corrupt(conn,
                "UPDATE okg.source_record_authority_payload_chunks SET payload=%s WHERE sha256=%s",
                (b'"damaged"', sha),
            )
    with pytest.raises(AuthorityError):
        with _private_reader(store, candidate):
            pytest.fail("caller could abandon the reader before corruption was checked")


def test_private_reader_preserves_live_multiplicity_unicode_and_ownership(store):
    shared = RecordNode("shared-🌍", "dataset", {}, {"rev": 1}, ownership_mode="substitute")
    records = {key: RecordEmission(primary=shared) for key in ("a", "ü")}
    candidate = _candidate(store, records)
    with _private_reader(store, candidate) as reader:
        assert list(reader.iterate_identities()) == [
            ("node", "shared-🌍", "substitute"),
            ("node", "shared-🌍", "substitute"),
        ]
    historical = _historical_candidate(store)
    with _private_reader(store, historical) as reader:
        rows = list(reader.iterate_identities())
        assert len(rows) == 3
        assert ("node", "live", "authoritative") in rows
        assert not any("old" in identity for _, identity, _ in rows)


def test_private_reader_verifies_multichunk_payload_in_bounded_memory(conn):
    store = _ByteAccountingStore(conn)
    candidate = _candidate(store, {"huge": _emission("huge", body="q" * (8 * LIMIT))})
    tracemalloc.start()
    try:
        with _private_reader(store, candidate) as reader:
            rows = list(reader.iterate_identities())
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert len(rows) == 3
    assert store.total_payload_bytes > 8 * LIMIT
    assert store.max_chunk_bytes <= LIMIT
    assert peak < 4 * LIMIT, peak


@pytest.mark.parametrize("consumed", [False, True])
def test_private_reader_rejects_escaped_prefetched_iterator(store, consumed):
    candidate = _candidate(store)
    with _private_reader(store, candidate) as reader:
        escaped = reader.iterate_identities()
        if consumed:
            next(escaped)
    with pytest.raises(AuthorityError, match="closed"):
        next(escaped)
    with pytest.raises(AuthorityError, match="closed"):
        reader.iterate_identities()


def test_private_reader_releases_exclusion_after_validation_and_body_failures(store):
    from dataclasses import replace

    candidate = _candidate(store)
    with pytest.raises(AuthorityError, match="digest or count mismatch"):
        with _private_reader(store, replace(candidate, expected_sha256="0" * 64)):
            pytest.fail("bad digest escaped")
    consumer_failure_propagated = False
    try:
        with _private_reader(store, candidate) as reader:
            next(reader.iterate_identities())
            raise RuntimeError("consumer failed")
    except RuntimeError as exc:
        assert str(exc) == "consumer failed"
        consumer_failure_propagated = True
    assert consumer_failure_propagated
    with _private_reader(store, candidate) as reader:
        assert len(list(reader.iterate_identities())) == 3
    assert store.seal_and_bind(candidate).sha256 == candidate.expected_sha256


def test_private_reader_does_not_authorize_a_later_corrupt_seal(store, conn):
    candidate = _candidate(store)
    with _private_reader(store, candidate):
        pass
    _corrupt(conn,
        "DELETE FROM okg.source_record_authority_identities "
        "WHERE authority_id=%s AND identity_kind='edge'", (candidate.authority,),
    )
    with pytest.raises(AuthorityError, match="identity index"):
        store.seal_and_bind(candidate)


def test_private_reader_excludes_second_store_during_verification_and_iteration(store, conn, monkeypatch):
    second = ProductionAuthorityStore(conn)
    candidate = _candidate(store)
    public = _seal(store, {"public": _emission("public")})
    original = store._verify_candidate
    witnessed = []

    def refuse_other_calls():
        actions = [
            lambda: second.begin(*_identity()),
            lambda: second.stage_stream(*_identity(), _stream({})),
            lambda: second.stage_record(candidate.authority, 1, "b", None, (), ()),
            lambda: second.stage_retraction(candidate.authority, 1, "a"),
            lambda: second.seal_and_bind(candidate),
        ]
        for action in actions:
            with pytest.raises(AuthorityError, match="active verified reader"):
                action()
        for context in (_private_reader(second, candidate), second.reader(public)):
            with pytest.raises(AuthorityError, match="active verified reader"):
                with context:
                    pytest.fail("overlapping reader escaped")

    def verify(value):
        refuse_other_calls()
        witnessed.append("verification")
        return original(value)

    monkeypatch.setattr(store, "_verify_candidate", verify)
    with _private_reader(store, candidate) as reader:
        refuse_other_calls()
        assert next(reader.iterate_identities())
        witnessed.append("iteration")
    assert witnessed == ["verification", "iteration"]
    monkeypatch.setattr(store, "_verify_candidate", original)
    # Public-reader-first overlap is also excluded on this same handle.
    with second.reader(public):
        with pytest.raises(AuthorityError, match="active verified reader"):
            with _private_reader(store, candidate):
                pytest.fail("private reader borrowed an active public connection")
    assert second.seal_and_bind(candidate).sha256 == candidate.expected_sha256


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
def test_private_reader_existing_transaction_isolation(store, conn, isolation):
    candidate = _candidate(store)
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL " + isolation)
        if isolation == "READ COMMITTED":
            with pytest.raises(AuthorityError, match="unstable transaction"):
                with _private_reader(store, candidate):
                    pytest.fail("unstable snapshot accepted")
        else:
            with _private_reader(store, candidate) as reader:
                assert len(list(reader.iterate_identities())) == 3
    with _private_reader(store, candidate) as reader:
        assert list(reader.iterate_identities())


@pytest.mark.parametrize("outer_transaction", [False, True])
def test_private_reader_holds_header_lock_until_enclosing_transaction_ends(
    store, conn, clean_runtime, outer_transaction,
):
    from contextlib import nullcontext

    candidate = _candidate(store)
    with psycopg.connect(clean_runtime, autocommit=True) as other:
        other.execute("SET lock_timeout='100ms'")

        second = ProductionAuthorityStore(other)

        def blocked_update():
            with pytest.raises(psycopg.errors.LockNotAvailable):
                second.seal_and_bind(candidate)

        with conn.transaction() if outer_transaction else nullcontext():
            if outer_transaction:
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            with _private_reader(store, candidate):
                blocked_update()
            if outer_transaction:
                blocked_update()
        reference = second.seal_and_bind(candidate)
        assert reference.authority == candidate.authority
        assert reference.sha256 == candidate.expected_sha256


def test_private_reader_returns_empty_after_complete_retraction(store):
    old = _emission("gone").to_cursor()
    operations = [
        RecordAuthorityOperation(
            ordinal=0, record_id="gone", primary=old["primary"],
            descendants=tuple(old["descendants"]), edges=tuple(old["edges"]),
        ),
        RecordAuthorityOperation(ordinal=1, record_id="gone", retract=True),
    ]
    candidate = store.stage_stream(*_identity(), _stream_of(operations, {}))
    with _private_reader(store, candidate) as reader:
        assert list(reader.iterate_identities()) == []


def test_private_reader_checks_lifetime_after_cursor_advancement(store):
    candidate = _candidate(store)
    with _private_reader(store, candidate) as reader:
        original = reader._cursor

        class ClosingCursor:
            @property
            def closed(self):
                return original.closed

            def __next__(self):
                row = next(original)
                reader._active = False
                return row

            def close(self):
                original.close()

        reader._cursor = ClosingCursor()
        reader._rows = reader._cursor
        with pytest.raises(AuthorityError, match="closed"):
            next(reader.iterate_identities())



def test_private_reader_supports_iterable_cursor_wrapper_and_closes_escapes(conn):
    store = ProductionAuthorityStore(CountingConnection(conn))
    candidate = _candidate(store)
    with _private_reader(store, candidate) as reader:
        escaped = reader.iterate_identities()
        assert next(escaped)
        assert len(list(escaped)) == 2
    with pytest.raises(AuthorityError, match="closed"):
        next(escaped)
    # Leave buffered rows unconsumed to witness close of the wrapper generator.
    with _private_reader(store, candidate) as reader:
        escaped = reader.iterate_identities()
        next(escaped)
        rows = reader._rows
    assert rows.gi_frame is None
    with pytest.raises(AuthorityError, match="closed"):
        next(escaped)
    assert store.seal_and_bind(candidate).sha256 == candidate.expected_sha256


@pytest.mark.parametrize("sample", [None, 1, 0])
def test_real_bench_matches_legacy_digest_and_physical_rows(clean_runtime, tmp_path, sample):
    from okg.substrate.record_authority_bench import run_real_bench
    from tests.substrate.test_record_authority_bench_input import record, write_snapshot

    records = [record("a", rich=True), record("雪", rich=True)]
    records[0]["edges"][0]["provenance"] = ""
    path = write_snapshot(tmp_path, records)
    report = run_real_bench(clean_runtime, manifest_path=str(path), sample_records=sample)
    selected = records if sample is None else records[:sample]
    legacy = RecordSet(records={r["record_id"]: RecordEmission(
        primary=RecordNode(**r["primary"]),
        descendants=[RecordNode(**v) for v in r["descendants"]],
        edges=[EdgeKey(**v) for v in r["edges"]],
    ) for r in selected})
    assert report["logical_sha256"] == hashlib.sha256(canonical(legacy.to_cursor()).encode()).hexdigest()
    assert report["logical_sha256"] == report["selection"]["logical_sha256"]
    assert report["schema"] == "okg.record_authority.real_bench.v1"
    assert report["selection"]["records"] == len(selected)
    assert report["selection"]["components"] == len(selected) * 3
    assert report["selection"]["policy"] == ("full" if sample is None else "canonical-prefix")
    assert report["bundle"]["source_revision"]["n_records"] == 200000
    assert report["source"] != report["bundle"]["source"]
    assert report["timings"]["external_preparation_seconds"] is None
    for phase in ("capture_seconds", "validation_seconds", "selection_digest_seconds", "stage_seconds", "seal_seconds"):
        assert report["timings"][phase] >= 0
    counts = report["row_counts"]
    assert counts["okg.source_record_authority_records"] == len(selected)
    assert counts["okg.source_record_authority_operations"] == len(selected)
    assert counts["okg.source_record_authority_components"] == len(selected) * 3
    assert counts["okg.source_record_authority_identities"] == len(selected) * 3
    assert counts["okg.source_record_authority_component_fields"] == len(selected) * 17
    assert report["storage"]["database_bytes_after"] > 0
    assert report["storage"]["cluster_wal_bytes_generated"] >= 0
    if not selected:
        assert report["records_per_second"] == 0
        assert report["round_trips_per_record"] is None
    with psycopg.connect(clean_runtime, autocommit=True) as connection:
        store = ProductionAuthorityStore(connection)
        ref = ProductionReference(authority=report["authority_id"], source=report["source"],
                                  incarnation=report["incarnation"],
                                  sha256=report["logical_sha256"], count=len(selected))
        assert b"".join(store.iterate_canonical_bytes(ref)) == canonical(legacy.to_cursor()).encode()


def test_real_bench_empty_complete_snapshot(clean_runtime, tmp_path):
    from okg.substrate.record_authority_bench import run_real_bench
    from tests.substrate.test_record_authority_bench_input import write_snapshot

    report = run_real_bench(clean_runtime, manifest_path=str(write_snapshot(tmp_path, [])))
    assert report["selection"]["records"] == 0
    assert report["selection"]["components"] == 0
    assert report["selection"]["policy"] == "full"
    assert report["memory"]["spool_bytes"] == 0
    assert report["memory"]["max_record_bytes"] == 0
    assert report["round_trips_per_record"] is None
    assert report["logical_sha256"] == hashlib.sha256(canonical(RecordSet().to_cursor()).encode()).hexdigest()


def test_real_bench_invalid_tail_leaves_database_unchanged(clean_runtime, tmp_path):
    from okg.substrate.record_authority_bench import _baseline_rows, run_real_bench
    from tests.substrate.test_record_authority_bench_input import record, write_snapshot, replace_records

    path = write_snapshot(tmp_path, [record("a"), record("b")])
    replace_records(path, (json.dumps(record("a")) + '\n{"invalid":true}\n').encode())
    with psycopg.connect(clean_runtime, autocommit=True) as connection:
        before = _baseline_rows(connection)
        headers = connection.execute("SELECT count(*) FROM okg.source_record_authorities").fetchone()
        with pytest.raises(AuthorityError):
            run_real_bench(clean_runtime, manifest_path=str(path), sample_records=1)
        assert _baseline_rows(connection) == before
        assert connection.execute("SELECT count(*) FROM okg.source_record_authorities").fetchone() == headers
