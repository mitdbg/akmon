from __future__ import annotations

import uuid
import time
from dataclasses import replace
from datetime import datetime, timezone

import psycopg
import pytest

from okg.substrate.fact_application import (
    FactBatchApplication,
    FactBatchApplicationError,
    ZeroOutputEvidence,
    assert_zero_output_acceptance_allowed,
    chronos_apply_callback,
)
from okg.substrate.fact_batch import (
    ApplicationReceipt,
    FactBatchHeader,
    FactChunk,
    MemoryApplicationReceiptStore,
    PostgresApplicationReceiptStore,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.base import SourceRun
from okg.substrate.producer_authority import ProducerProfile


class FakeGraphStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def apply_facts(self, facts, **kwargs):
        self.calls.append({"facts": list(facts), **kwargs})
        return {"nodes_applied": len(self.calls[-1]["facts"])}


def test_shared_lifecycle_pins_authority_before_apply_and_finalizes_receipt() -> None:
    header = _header()
    profile = _profile()
    graph = FakeGraphStore()
    application = FactBatchApplication(
        header=header,
        profile=profile,
        receipt_store=MemoryApplicationReceiptStore(),
        apply_graph=chronos_apply_callback(
            graph,
            source=profile.producer_id,
            strict_endpoints=False,
        ),
        validate_signature=False,
    )

    assert application.state == "prepared"
    applied = application.apply_chunk(_chunk(header))
    assert application.state == "applying"
    assert applied.receipt.state == "applied"
    assert graph.calls[0]["branch"] == "candidate"
    assert graph.calls[0]["source"] == profile.producer_id
    assert graph.calls[0]["run_id"] == str(header.run_id)

    summary = application.finish_observation(
        completion="delta",
        observed_scope={"subtypes": ["document"]},
    )

    assert application.state == "finalized"
    assert summary.application_receipts == (applied.receipt,)


def test_replay_is_idempotent_and_never_reapplies_an_applied_chunk() -> None:
    header = _header()
    store = MemoryApplicationReceiptStore()
    graph = FakeGraphStore()

    first = _application(header, store, graph)
    first.apply_chunk(_chunk(header))
    first.finish_observation(completion="delta")

    replay = _application(header, store, graph)
    replay.apply_chunk(_chunk(header))
    replay.finish_observation(completion="delta")

    assert len(graph.calls) == 1


def test_graph_failure_leaves_prepared_recovery_evidence() -> None:
    header = _header()
    store = MemoryApplicationReceiptStore()

    def fail(_facts, _header):
        raise RuntimeError("graph unavailable")

    application = FactBatchApplication(
        header=header,
        profile=_profile(),
        receipt_store=store,
        apply_graph=fail,
        validate_signature=False,
    )
    chunk = _chunk(header)

    with pytest.raises(RuntimeError, match="graph unavailable"):
        application.apply_chunk(chunk)

    receipt = store.get(header.admission_id, header.run_id, 0)
    assert receipt is not None
    assert receipt.state == "prepared"


def test_zero_output_observation_is_separate_from_acceptance() -> None:
    application = _application(
        _header(), MemoryApplicationReceiptStore(), FakeGraphStore()
    )

    summary = application.finish_observation(completion="delta")
    assert summary.terminal_chunk_count == 0

    with pytest.raises(
        FactBatchApplicationError,
        match="server-resolved evidence",
    ):
        assert_zero_output_acceptance_allowed(None)


@pytest.mark.parametrize(
    ("health", "complete", "authorized"),
    [
        ("auth_failed", True, True),
        ("acquire_failed", True, True),
        ("acquire_stale", True, True),
        ("ok", False, True),
        ("ok", True, False),
    ],
)
def test_failed_degraded_or_incomplete_zero_output_cannot_advance_state(
    health: str,
    complete: bool,
    authorized: bool,
) -> None:
    with pytest.raises(
        FactBatchApplicationError,
        match="does not authorize terminal state",
    ):
        assert_zero_output_acceptance_allowed(
            ZeroOutputEvidence(
                kind="unchanged",
                health_status=health,
                completed_scope=complete,
                terminal_state_authorized=authorized,
                reason="source terminal evidence",
            ),
        )


@pytest.mark.parametrize(
    "kind",
    ["unchanged", "authoritative_empty_snapshot"],
)
def test_authorized_healthy_complete_zero_output_may_finalize(
    kind: str,
) -> None:
    application = _application(
        _header(), MemoryApplicationReceiptStore(), FakeGraphStore()
    )

    assert_zero_output_acceptance_allowed(
        ZeroOutputEvidence(
            kind=kind,
            health_status="ok",
            completed_scope=True,
            terminal_state_authorized=True,
            reason="server source policy accepted terminal evidence",
        )
    )
    summary = application.finish_observation(
        completion="snapshot" if "snapshot" in kind else "delta",
    )

    assert summary.terminal_chunk_count == 0


@pytest.mark.postgres
def test_terminal_rejection_is_blocked_until_receipts_are_compensated(
    clean_runtime: str,
) -> None:
    from okg.substrate.admission import ensure_envelope, transition_envelope
    from okg.substrate.errors import SubstrateError

    run_id = uuid.uuid4()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="test",
            producer_id="source:test",
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key=f"rejection-fence:{run_id}",
            source="source:test",
            run_id=run_id,
            authority={"profile_fingerprint": _profile().profile_fingerprint},
        )
        header = replace(
            _header(),
            run_id=run_id,
            admission_id=uuid.UUID(envelope.admission_id),
        )
        chunk = _chunk(header)
        store = PostgresApplicationReceiptStore(conn)
        applied = store.transition(
            store.prepare(ApplicationReceipt.prepared(header, chunk)).key,
            "applied",
            graph_result={"nodes_upserted": 1},
        )

        with pytest.raises(SubstrateError, match="proven FactBatch compensation"):
            transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status="rejected",
                actor="source:test",
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM okg.admission_envelopes "
                "WHERE admission_id = %s::uuid",
                (envelope.admission_id,),
            )
            assert cur.fetchone() == ("pending",)

        store.transition(applied.key, "compensating")
        store.transition(applied.key, "compensated")
        result = transition_envelope(
            conn,
            admission_id=envelope.admission_id,
            status="rejected",
            actor="source:test",
        )

    assert result["status"] == "rejected"


def test_caller_cannot_self_grant_profile_scope_or_identity() -> None:
    header = _header(requested_scope={"provider": "other"})
    with pytest.raises(Exception, match="scope exceeds"):
        _application(header, MemoryApplicationReceiptStore(), FakeGraphStore())

    mismatch = _header(fingerprint="sha256:" + "b" * 64)
    with pytest.raises(Exception, match="fingerprint mismatch"):
        _application(mismatch, MemoryApplicationReceiptStore(), FakeGraphStore())


@pytest.mark.postgres
def test_legacy_connector_uses_admission_batch_receipt_and_owned_exit(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp.connector_ingest import connector_ingest_batch

    class _SkippedPublisher:
        def cycle(self) -> dict[str, str]:
            return {"status": "skipped_by_contract_test"}

    built: list[dict[str, object]] = []

    def _build_skipped_publisher(**kwargs: object) -> _SkippedPublisher:
        built.append(kwargs)
        return _SkippedPublisher()

    # This contract covers the admission receipt, not the publish. The
    # connector builds its publisher through the deployment factory, which
    # needs catalog writer authority the "default" test deployment does not
    # have, so the whole build is stubbed rather than only Publisher.cycle.
    monkeypatch.setattr(
        "okg.substrate.publisher.factory.build_deployment_publisher",
        _build_skipped_publisher,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = connector_ingest_batch(
            conn,
            "default",
            "gmail_api",
            "gmail_api:host_connector",
            [
                {
                    "external_id": "message-1",
                    "thread_id": "thread-1",
                    "labels": ["INBOX"],
                    "subject": "bounded connector payload",
                }
            ],
            provenance={"allowlist": {"labels": ["INBOX"]}},
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.status, r.state,
                       e.authority->'compatibility'->>'owner',
                       e.authority->'compatibility'->>'exit_condition'
                  FROM okg.admission_envelopes e
                  JOIN okg.fact_batch_receipts r USING (admission_id)
                 WHERE e.admission_id = %s::uuid
                """,
                (result["admission_id"],),
            )
            row = cur.fetchone()

    assert result["status"] == "ingest_complete", result["publish"]
    assert result["publish"] == {"status": "skipped_by_contract_test"}
    assert [call["deployment_name"] for call in built] == ["default"]
    assert row[0:3] == (
        "accepted",
        "applied",
        "build-akmon-connector-suite",
    )
    assert "ProviderConnection" in row[3]


def test_legacy_connector_zero_output_does_not_create_acceptance() -> None:
    from okg.substrate.mcp.connector_ingest import connector_ingest_batch

    result = connector_ingest_batch(
        None,
        "missing-deployment",
        "gmail_api",
        "gmail_api:host_connector",
        [],
        provenance={"allowlist": {"labels": ["INBOX"]}},
    )

    assert result["status"] == "no_records"


@pytest.mark.postgres
def test_orchestrated_source_runner_streams_through_shared_receipts(
    clean_runtime: str,
) -> None:
    from okg.substrate.admission import ensure_envelope
    from okg.substrate.ingest.runner import SourceRunContext, SourceRunner

    profile = replace(
        _profile(),
        deployment_name="default",
        producer_id="source:runner-contract",
        authority_scope={},
        output_signature={"nodes": [{"subtype": "document"}]},
        catalog_version_id=None,
    )
    run_id = uuid.uuid4()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="default",
            producer_id=profile.producer_id,
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key="runner-contract-replay",
            source=profile.producer_id,
            run_id=run_id,
            authority={"profile_fingerprint": profile.profile_fingerprint},
        )

    class Adapter:
        name = "runner-contract"
        profile = "append_only"

        def run(self, _run_id, **_kwargs):
            return SourceRun(
                facts=[
                    NodeFact(
                        node_id="doc:runner-contract",
                        subtype="document",
                        attrs={"title": "streamed"},
                        source_record_id={"id": "runner-contract"},
                        source_revision={"revision": "1"},
                    )
                ],
                completed_scope=True,
                next_cursor={"cursor": "1"},
            )

    result = SourceRunner(
        clean_runtime,
        source_name=profile.producer_id,
        deployment_name="default",
    ).run(
        Adapter(),
        run_context=SourceRunContext(
            run_id=str(run_id),
            observed_at=datetime.now(timezone.utc),
            admission_id=envelope.admission_id,
            idempotency_key="runner-contract-replay",
            producer_profile=profile,
        ),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT state, canonical_digest
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
            receipt = cur.fetchone()

    assert result.fact_batch_summary is not None
    assert result.fact_batch_summary["terminal_chunk_count"] == 1
    assert receipt[0] == "applied"
    assert receipt[1].startswith("sha256:")


@pytest.mark.postgres
def test_orchestrated_source_runner_admits_command_in_attrs(
    clean_runtime: str,
) -> None:
    # #822: a source that records a `command` in a node's attrs used to fail
    # admission every cycle (FactBatchFieldError: node.attrs contains forbidden
    # control/secret field 'command'), which failed the whole source-sync
    # publish so no generation ever projected. Streaming that same fact through
    # shared admission must now reach a durable receipt in state 'applied'.
    from okg.substrate.admission import ensure_envelope
    from okg.substrate.ingest.runner import SourceRunContext, SourceRunner

    profile = replace(
        _profile(),
        deployment_name="default",
        producer_id="source:runner-command",
        authority_scope={},
        output_signature={"nodes": [{"subtype": "document"}]},
        catalog_version_id=None,
    )
    run_id = uuid.uuid4()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="default",
            producer_id=profile.producer_id,
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key="runner-command-replay",
            source=profile.producer_id,
            run_id=run_id,
            authority={"profile_fingerprint": profile.profile_fingerprint},
        )

    class Adapter:
        name = "runner-command"
        profile = "append_only"

        def run(self, _run_id, **_kwargs):
            return SourceRun(
                facts=[
                    NodeFact(
                        node_id="doc:runner-command",
                        subtype="document",
                        attrs={
                            "title": "analysis notes",
                            "metadata": {"command": "uv run pytest -q tests/foo.py"},
                        },
                        source_record_id={"id": "runner-command"},
                        source_revision={"revision": "1"},
                    )
                ],
                completed_scope=True,
                next_cursor={"cursor": "1"},
            )

    result = SourceRunner(
        clean_runtime,
        source_name=profile.producer_id,
        deployment_name="default",
    ).run(
        Adapter(),
        run_context=SourceRunContext(
            run_id=str(run_id),
            observed_at=datetime.now(timezone.utc),
            admission_id=envelope.admission_id,
            idempotency_key="runner-command-replay",
            producer_profile=profile,
        ),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT state, canonical_digest
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
            receipt = cur.fetchone()

    assert result.fact_batch_summary is not None
    assert result.fact_batch_summary["terminal_chunk_count"] == 1
    assert receipt[0] == "applied"
    assert receipt[1].startswith("sha256:")


@pytest.mark.postgres
def test_orchestrated_empty_source_runner_emits_zero_chunk_terminal_summary(
    clean_runtime: str,
) -> None:
    from okg.substrate.admission import ensure_envelope
    from okg.substrate.ingest.runner import SourceRunContext, SourceRunner

    profile = replace(
        _profile(),
        deployment_name="default",
        producer_id="source:runner-empty-contract",
        authority_scope={},
        output_signature={"nodes": [{"subtype": "document"}]},
        catalog_version_id=None,
    )
    run_id = uuid.uuid4()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="default",
            producer_id=profile.producer_id,
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key="runner-empty-contract-replay",
            source=profile.producer_id,
            run_id=run_id,
            authority={"profile_fingerprint": profile.profile_fingerprint},
        )

    class EmptyAdapter:
        name = "runner-empty-contract"
        profile = "append_only"

        def run(self, _run_id, **_kwargs):
            return SourceRun(
                facts=iter(()),
                completed_scope=True,
                next_cursor={"cursor": "empty"},
            )

    result = SourceRunner(
        clean_runtime,
        source_name=profile.producer_id,
        deployment_name="default",
    ).run(
        EmptyAdapter(),
        run_context=SourceRunContext(
            run_id=str(run_id),
            observed_at=datetime.now(timezone.utc),
            admission_id=envelope.admission_id,
            idempotency_key="runner-empty-contract-replay",
            producer_profile=profile,
        ),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
            receipt_count = int(cur.fetchone()[0])

    assert result.fact_batch_summary is not None
    assert result.fact_batch_summary["run_id"] == str(run_id)
    assert result.fact_batch_summary["terminal_chunk_count"] == 0
    assert result.fact_batch_summary["application_receipts"] == []
    assert result.fact_batch_summary["completion"] == "delta"
    assert receipt_count == 0


@pytest.mark.postgres
def test_admitted_probe_short_circuit_emits_zero_chunk_terminal_summary(
    clean_runtime: str,
) -> None:
    from okg.substrate.admission import ensure_envelope
    from okg.substrate.ingest.runner import SourceRunContext, SourceRunner

    profile = replace(
        _profile(),
        deployment_name="default",
        producer_id="source:runner-probe-contract",
        authority_scope={},
        output_signature={"nodes": [{"subtype": "document"}]},
        catalog_version_id=None,
    )
    run_id = uuid.uuid4()
    observed_at = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="default",
            producer_id=profile.producer_id,
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key="runner-probe-contract-replay",
            source=profile.producer_id,
            run_id=run_id,
            authority={"profile_fingerprint": profile.profile_fingerprint},
        )

    runner = SourceRunner(
        clean_runtime,
        source_name=profile.producer_id,
        deployment_name="default",
    )
    runner._active_run_context = SourceRunContext(
        run_id=str(run_id),
        observed_at=observed_at,
        admission_id=envelope.admission_id,
        idempotency_key="runner-probe-contract-replay",
        producer_profile=profile,
    )
    runner._active_sync_scope = {"scope_keys": ["unchanged"]}
    try:
        result = runner._probe_short_circuit_result(
            run_id=str(run_id),
            mode="cursor",
            observed_at=observed_at,
            runner_started_at=observed_at,
            prior_cursor={"cursor": "old"},
            probe_token={"schema": "probe", "digest": "unchanged"},
            reason="unchanged",
            completed_scope=True,
            timings_ms={},
            timings_started=time.perf_counter(),
        )
    finally:
        runner._active_run_context = None
        runner._active_sync_scope = None
        runner._fact_batch_application = None
        runner._release_conn()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
            receipt_count = int(cur.fetchone()[0])

    assert result.admission_id == envelope.admission_id
    assert result.fact_batch_summary is not None
    assert result.fact_batch_summary["run_id"] == str(run_id)
    assert result.fact_batch_summary["terminal_chunk_count"] == 0
    assert result.fact_batch_summary["application_receipts"] == []
    assert result.fact_batch_summary["completion"] == "delta"
    assert receipt_count == 0


def _application(header, store, graph) -> FactBatchApplication:
    return FactBatchApplication(
        header=header,
        profile=_profile(),
        receipt_store=store,
        apply_graph=chronos_apply_callback(
            graph,
            source="source:test",
            strict_endpoints=False,
        ),
        validate_signature=False,
    )


def _header(
    *,
    requested_scope=None,
    fingerprint: str = "sha256:" + "a" * 64,
) -> FactBatchHeader:
    return FactBatchHeader(
        schema_version=1,
        run_id=uuid.UUID("00000000-0000-0000-0000-000000000101"),
        admission_id=uuid.UUID("00000000-0000-0000-0000-000000000201"),
        producer_profile_fingerprint=fingerprint,
        deployment="test",
        target_branch="candidate",
        catalog_version_id=7,
        intent="source_sync",
        idempotency_key="server-pinned-replay-key",
        requested_scope=requested_scope,
    )


def _profile() -> ProducerProfile:
    return ProducerProfile(
        deployment_name="test",
        producer_id="source:test",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={"provider": "approved"},
        allowed_record_kinds=("node",),
        output_signature={},
        policy_clauses=(),
        output_scope_summary={"nodes": ["document"]},
        source_ref={"source": "fixture"},
        catalog_version_id=7,
        profile_fingerprint="sha256:" + "a" * 64,
        status="active",
    )


def _chunk(header: FactBatchHeader) -> FactChunk:
    return FactChunk.create(
        run_id=header.run_id,
        ordinal=0,
        facts=[
            NodeFact(
                node_id="doc:1",
                subtype="document",
                attrs={"title": "One"},
                source_record_id={"id": "1"},
                source_revision={"revision": "1"},
            )
        ],
    )
