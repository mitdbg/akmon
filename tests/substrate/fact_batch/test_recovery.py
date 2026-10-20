from __future__ import annotations

import uuid
from dataclasses import replace

import psycopg
import pytest

from okg.substrate.admission import ensure_envelope
from okg.substrate.fact_application import (
    FactBatchApplication,
    FactBatchRecoveryBlocked,
    FactBatchRecoveryCoordinator,
)
from okg.substrate.fact_batch import (
    ApplicationReceipt,
    ApplicationReceiptConflict,
    FactBatchHeader,
    FactChunk,
    MemoryApplicationReceiptStore,
    PostgresApplicationReceiptStore,
)
from okg.substrate.library.sources.base import NodeFact, ProgressMarker
from okg.substrate.producer_authority import ProducerProfile


def test_marker_fault_leaves_prepared_receipt_then_recovery_finishes() -> None:
    header = _header()
    profile = _profile()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    graph_calls: list[str] = []
    markers: dict[str, str] = {}

    def apply_graph(_facts, _header):
        graph_calls.append(chunk.digest)
        return {"nodes_upserted": 1, "branch_token": "candidate:1"}

    def fail_evidence(_chunk, _receipt):
        raise RuntimeError("marker transaction crashed")

    application = FactBatchApplication(
        header=header,
        profile=profile,
        receipt_store=store,
        apply_graph=apply_graph,
        write_chunk_evidence=fail_evidence,
        validate_signature=False,
    )
    with pytest.raises(RuntimeError, match="marker transaction crashed"):
        application.apply_chunk(chunk)

    prepared = store.get(header.admission_id, header.run_id, 0)
    assert prepared is not None and prepared.state == "prepared"
    assert markers == {}

    recovered = FactBatchRecoveryCoordinator(
        header=header,
        profile=profile,
        receipt_store=store,
        validate_signature=False,
    ).recover_prepared_chunk(
        chunk,
        apply_graph=apply_graph,
        write_chunk_evidence=lambda value, _receipt: (
            markers.update(
                {
                    marker.record_id: marker.fingerprint
                    for marker in value.progress_markers
                }
            )
            or ()
        ),
    )

    assert recovered.state == "applied"
    assert recovered.graph_result["nodes_upserted"] == 1
    assert markers == {"record-1": "fingerprint-1"}
    assert len(graph_calls) == 2


@pytest.mark.postgres
def test_postgres_marker_and_applied_receipt_commit_atomically(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        run_id = uuid.uuid4()
        envelope = ensure_envelope(
            conn,
            deployment_name="test",
            producer_id="source:test",
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key=f"atomic:{run_id}",
            run_id=run_id,
        )
        header = replace(
            _header(),
            run_id=run_id,
            admission_id=uuid.UUID(envelope.admission_id),
            idempotency_key=f"atomic:{run_id}",
        )
        chunk = _chunk(header)
        store = PostgresApplicationReceiptStore(conn)

        def insert_then_fail(_chunk, _receipt):
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO okg.source_progress "
                    "(deployment, source_name, key, value) "
                    "VALUES ('test', 'source:test', 'record:1', 'advanced')"
                )
            raise RuntimeError("crash before receipt transition")

        application = FactBatchApplication(
            header=header,
            profile=_profile(),
            receipt_store=store,
            apply_graph=lambda _facts, _header: {
                "nodes_upserted": 1,
                "branch_token": "candidate:1",
            },
            write_chunk_evidence=insert_then_fail,
            validate_signature=False,
        )
        with pytest.raises(RuntimeError, match="crash before receipt"):
            application.apply_chunk(chunk)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.source_progress "
                "WHERE deployment = 'test' AND source_name = 'source:test'"
            )
            assert cur.fetchone()[0] == 0
        assert store.get(header.admission_id, header.run_id, 0).state == "prepared"

        recovered = FactBatchRecoveryCoordinator(
            header=header,
            profile=_profile(),
            receipt_store=store,
            validate_signature=False,
        ).recover_prepared_chunk(
            chunk,
            apply_graph=lambda _facts, _header: {
                "nodes_upserted": 1,
                "branch_token": "candidate:1",
            },
            write_chunk_evidence=lambda _chunk, _receipt: _insert_marker(conn),
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM okg.source_progress "
                "WHERE deployment = 'test' AND source_name = 'source:test'"
            )
            marker = cur.fetchone()

    assert recovered.state == "applied"
    assert marker == ("advanced",)


def test_recovery_rejects_digest_mismatch_and_missing_authority() -> None:
    header = _header()
    profile = _profile()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    store.prepare(ApplicationReceipt.prepared(header, chunk))
    coordinator = FactBatchRecoveryCoordinator(
        header=header,
        profile=profile,
        receipt_store=store,
        validate_signature=False,
    )
    changed = FactChunk.create(
        run_id=header.run_id,
        ordinal=0,
        facts=[_node(title="changed")],
        progress_markers=chunk.progress_markers,
    )

    with pytest.raises(ApplicationReceiptConflict, match="digest"):
        coordinator.recover_prepared_chunk(
            changed,
            apply_graph=lambda _facts, _header: {"nodes_upserted": 1},
        )

    with pytest.raises(Exception, match="fingerprint mismatch"):
        FactBatchRecoveryCoordinator(
            header=header,
            profile=replace(
                profile,
                profile_fingerprint="sha256:" + "b" * 64,
            ),
            receipt_store=store,
            validate_signature=False,
        )


def test_recovery_blocks_unverifiable_graph_result() -> None:
    header = _header()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    store.prepare(ApplicationReceipt.prepared(header, chunk))

    with pytest.raises(FactBatchRecoveryBlocked, match="no verifiable"):
        FactBatchRecoveryCoordinator(
            header=header,
            profile=_profile(),
            receipt_store=store,
            validate_signature=False,
        ).recover_prepared_chunk(
            chunk,
            apply_graph=lambda _facts, _header: {},
        )

    assert store.get(header.admission_id, header.run_id, 0).state == "prepared"


def test_compensation_must_complete_before_rejected_terminal_state() -> None:
    header = _header()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    applied = store.transition(
        store.prepare(ApplicationReceipt.prepared(header, chunk)).key,
        "applied",
        graph_result={"nodes_upserted": 1},
        branch_token="candidate:1",
    )
    coordinator = FactBatchRecoveryCoordinator(
        header=header,
        profile=_profile(),
        receipt_store=store,
        validate_signature=False,
    )

    with pytest.raises(FactBatchRecoveryBlocked, match="proven compensation"):
        coordinator.assert_terminal_disposition_allowed("rejected", [applied])

    calls: list[int] = []
    compensated = coordinator.compensate(
        [applied],
        compensate_graph=lambda receipt: calls.append(receipt.chunk_ordinal),
    )
    coordinator.assert_terminal_disposition_allowed("rejected", compensated)

    assert calls == [0]
    assert compensated[0].state == "compensated"


def test_compensation_failure_is_durable_and_blocks_recovery() -> None:
    header = _header()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    applied = store.transition(
        store.prepare(ApplicationReceipt.prepared(header, chunk)).key,
        "applied",
        graph_result={"nodes_upserted": 1},
    )
    coordinator = FactBatchRecoveryCoordinator(
        header=header,
        profile=_profile(),
        receipt_store=store,
        validate_signature=False,
    )

    with pytest.raises(FactBatchRecoveryBlocked, match="remains compensating"):
        coordinator.compensate(
            [applied],
            compensate_graph=lambda _receipt: (_ for _ in ()).throw(
                RuntimeError("backend unavailable")
            ),
        )

    unresolved = store.get(header.admission_id, header.run_id, 0)
    assert unresolved is not None and unresolved.state == "compensating"
    with pytest.raises(FactBatchRecoveryBlocked):
        coordinator.assert_terminal_disposition_allowed("failed", [unresolved])


@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError("timeout"),
        RuntimeError("cancelled"),
        ValueError("narrowing violation"),
        OSError("crash"),
    ],
)
def test_apply_failures_never_advance_marker_or_receipt(
    failure: Exception,
) -> None:
    header = _header()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    marker_advanced = False

    def fail(_facts, _header):
        raise failure

    def evidence(_chunk, _receipt):
        nonlocal marker_advanced
        marker_advanced = True
        return ()

    application = FactBatchApplication(
        header=header,
        profile=_profile(),
        receipt_store=store,
        apply_graph=fail,
        write_chunk_evidence=evidence,
        validate_signature=False,
    )

    with pytest.raises(type(failure)):
        application.apply_chunk(chunk)

    receipt = store.get(header.admission_id, header.run_id, 0)
    assert receipt is not None and receipt.state == "prepared"
    assert marker_advanced is False


def test_replay_of_applied_recovery_is_idempotent() -> None:
    header = _header()
    chunk = _chunk(header)
    store = MemoryApplicationReceiptStore()
    applied = store.transition(
        store.prepare(ApplicationReceipt.prepared(header, chunk)).key,
        "applied",
        graph_result={"nodes_upserted": 1},
        branch_token="candidate:1",
    )
    graph_calls = 0

    def apply(_facts, _header):
        nonlocal graph_calls
        graph_calls += 1
        return {"nodes_upserted": 1}

    recovered = FactBatchRecoveryCoordinator(
        header=header,
        profile=_profile(),
        receipt_store=store,
        validate_signature=False,
    ).recover_prepared_chunk(chunk, apply_graph=apply)

    assert recovered == applied
    assert graph_calls == 0


def _header() -> FactBatchHeader:
    return FactBatchHeader(
        schema_version=1,
        run_id=uuid.UUID("10000000-0000-0000-0000-000000000001"),
        admission_id=uuid.UUID("20000000-0000-0000-0000-000000000002"),
        producer_profile_fingerprint="sha256:" + "a" * 64,
        deployment="test",
        target_branch="candidate",
        catalog_version_id=7,
        intent="source_sync",
        idempotency_key="recovery-replay-key",
        requested_scope={"repo": "okg"},
    )


def _profile() -> ProducerProfile:
    return ProducerProfile(
        deployment_name="test",
        producer_id="source:test",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={"repo": "okg"},
        allowed_record_kinds=("node",),
        output_signature={},
        policy_clauses=(),
        output_scope_summary={},
        source_ref={"source": "test"},
        catalog_version_id=7,
        profile_fingerprint="sha256:" + "a" * 64,
        status="active",
    )


def _chunk(header: FactBatchHeader) -> FactChunk:
    return FactChunk.create(
        run_id=header.run_id,
        ordinal=0,
        facts=[_node()],
        progress_markers=[ProgressMarker("record-1", "fingerprint-1")],
    )


def _node(*, title: str = "one") -> NodeFact:
    return NodeFact(
        node_id="document:1",
        subtype="document",
        attrs={"title": title},
        source_record_id={"id": "1"},
        source_revision={"revision": "1"},
    )


def _insert_marker(conn: psycopg.Connection) -> tuple[()]:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.source_progress "
            "(deployment, source_name, key, value) "
            "VALUES ('test', 'source:test', 'record:1', 'advanced')"
        )
    return ()
